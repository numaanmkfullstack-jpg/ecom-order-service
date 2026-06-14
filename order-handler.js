const axios = require('axios');
const amqp = require('amqplib');
const { saveOrder, getOrder: getOrderFromDb } = require('./db');

// These URLs should come from environment variables
const PAYMENT_SERVICE_URL = process.env.PAYMENT_SERVICE_URL || 'http://localhost:3003';
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || 'http://localhost:3004';
const RABBITMQ_HOST = process.env.RABBITMQ_HOST || 'amqp://localhost:5672';

// =========================
// INVENTORY CHECK (WITH TIMEOUT)
// =========================

async function checkInventory(items) {
    const failures = [];
    
    for (const item of items) {
        try {
            // Call inventory service to get stock for this product
            const response = await axios.get(`${INVENTORY_SERVICE_URL}/inventory/${item.productId}`, {
                timeout: 3000  // 3 second timeout
            });
            
            if (response.status === 200) {
                const stock = response.data;
                const availableQuantity = stock.quantity;
                
                if (availableQuantity < item.quantity) {
                    failures.push({
                        productId: item.productId,
                        requested: item.quantity,
                        available: availableQuantity,
                        reason: `Insufficient stock. Only ${availableQuantity} available`
                    });
                }
            }
        } catch (error) {
            // If inventory service is down, still allow order (fallback)
            console.warn(`Inventory check skipped for ${item.productId}: ${error.message}`);
            // Don't fail the order - just log and continue
        }
    }
    
    if (failures.length > 0) {
        return { available: false, failures };
    }
    
    return { available: true, failures: [] };
}

// =========================
// CREATE ORDER (WITH TIMEOUTS)
// =========================

async function createOrder(orderData) {
    console.log(`[ORDER] Processing order ${orderData.id}`);
    
    // 1. CHECK INVENTORY (optional - don't block if inventory service is down)
    try {
        console.log(`[ORDER] Checking inventory for order ${orderData.id}`);
        const inventoryCheck = await checkInventory(orderData.items);
        
        if (!inventoryCheck.available) {
            const errorMessage = inventoryCheck.failures.map(f => f.reason).join('; ');
            console.error(`[ORDER] Inventory check failed: ${errorMessage}`);
            throw new Error(`Inventory check failed: ${errorMessage}`);
        }
        
        console.log(`[ORDER] Inventory check passed for order ${orderData.id}`);
    } catch (error) {
        // If inventory check fails with error (not stock issue), still continue
        if (error.message.includes('Inventory check failed')) {
            throw error;
        }
        console.warn(`[ORDER] Inventory service issue, continuing anyway: ${error.message}`);
    }
    
    // 2. Validate payment (with timeout)
    console.log(`[ORDER] Validating payment for order ${orderData.id}`);
    try {
        const paymentResponse = await axios.post(`${PAYMENT_SERVICE_URL}/validate`, {
            orderId: orderData.id,
            amount: orderData.total,
            paymentMethod: orderData.paymentMethod
        }, { timeout: 5000 });
        
        if (!paymentResponse.data.approved) {
            throw new Error('Payment rejected: ' + paymentResponse.data.reason);
        }
        
        console.log(`[ORDER] Payment approved for order ${orderData.id}`);
    } catch (paymentError) {
        console.error(`[ORDER] Payment failed: ${paymentError.message}`);
        throw new Error(`Payment failed: ${paymentError.message}`);
    }
    
    // 3. Save order to database
    const order = await saveOrder({
        ...orderData,
        status: 'PAYMENT_CONFIRMED',
        createdAt: new Date()
    });
    
    console.log(`[ORDER] Order saved: ${order.id}`);
    
    // 4. Publish to message queue (do not await - fire and forget)
    setImmediate(async () => {
        try {
            const connection = await amqp.connect(RABBITMQ_HOST);
            const channel = await connection.createChannel();
            const queue = 'inventory_orders';
            
            await channel.assertQueue(queue, { durable: true });
            channel.sendToQueue(queue, Buffer.from(JSON.stringify({
                orderId: order.id,
                items: orderData.items,
                timestamp: new Date()
            })), { persistent: true });
            
            console.log(`[ORDER] Published to RabbitMQ: ${order.id}`);
            
            setTimeout(() => {
                channel.close();
                connection.close();
            }, 500);
        } catch (mqError) {
            console.error(`[ORDER] Failed to publish to queue: ${mqError.message}`);
        }
    });
    
    return order;
}

// =========================
// GET ORDER
// =========================

async function getOrder(orderId) {
    return await getOrderFromDb(orderId);
}

module.exports = { createOrder, getOrder, checkInventory };