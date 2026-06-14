require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const axios = require('axios');

const app = express();
app.use(express.json());

// =========================
// DATABASE SETUP
// =========================

const pool = new Pool({
    host: process.env.POSTGRES_HOST || 'localhost',
    port: process.env.POSTGRES_PORT || 5432,
    database: process.env.POSTGRES_DB || 'orders',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD || 'postgres',
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// Create table on startup
async function initDb() {
    try {
        await pool.query(`
            CREATE TABLE IF NOT EXISTS orders (
                id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36) NOT NULL,
                items JSONB NOT NULL,
                total DECIMAL(10,2) NOT NULL,
                status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        `);
        console.log('✅ Orders table ready');
    } catch (err) {
        console.error('❌ DB init error:', err.message);
    }
}
initDb();

// =========================
// HEALTH CHECK
// =========================

app.get('/health', (req, res) => {
    res.status(200).json({
        status: 'UP',
        service: 'order-service',
        timestamp: new Date().toISOString()
    });
});

// =========================
// READINESS CHECK
// =========================

app.get('/ready', async (req, res) => {
    try {
        await pool.query('SELECT 1');
        res.status(200).json({ ready: true });
    } catch (error) {
        res.status(503).json({ ready: false, reason: error.message });
    }
});

// =========================
// CREATE ORDER WITH INVENTORY UPDATE
// =========================

app.post('/orders', async (req, res) => {
    console.log('📦 Raw request body:', req.body);
    
    try {
        const { id, userId, items, total } = req.body;
        
        // Validate required fields
        if (!id) return res.status(400).json({ error: 'Missing id' });
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        if (!items) return res.status(400).json({ error: 'Missing items' });
        if (total === undefined) return res.status(400).json({ error: 'Missing total' });
        
        console.log(`📦 Creating order: ${id} for user: ${userId}`);
        
        // Save to database
        const result = await pool.query(
            `INSERT INTO orders (id, user_id, items, total, status, created_at)
             VALUES ($1, $2, $3, $4, 'PAYMENT_CONFIRMED', NOW())
             RETURNING id, status`,
            [id, userId, JSON.stringify(items), total]
        );
        
        console.log(`✅ Order saved: ${id}`);
        
        // =========================
        // PUBLISH TO RABBITMQ FOR INVENTORY SERVICE
        // =========================
        try {
            const connection = await amqp.connect(process.env.RABBITMQ_HOST || 'amqp://localhost:5672');
            const channel = await connection.createChannel();
            const queue = 'inventory_orders';
            
            await channel.assertQueue(queue, { durable: true });
            channel.sendToQueue(queue, Buffer.from(JSON.stringify({
                orderId: id,
                items: items,
                timestamp: new Date()
            })), { persistent: true });
            
            console.log(`✅ Published to RabbitMQ: ${id}`);
            
            setTimeout(() => {
                channel.close();
                connection.close();
            }, 500);
        } catch (mqError) {
            console.error(`❌ RabbitMQ error: ${mqError.message}`);
            // Don't fail the order, just log the error
        }
        
        res.status(201).json({
            id: result.rows[0].id,
            status: result.rows[0].status,
            message: 'Order created successfully'
        });
        
    } catch (error) {
        console.error('❌ Order creation failed:', error.message);
        
        if (error.code === '23505') {
            return res.status(409).json({ error: 'Order ID already exists' });
        }
        
        res.status(500).json({ error: error.message });
    }
});

// =========================
// GET ORDER
// =========================

app.get('/orders/:id', async (req, res) => {
    try {
        const result = await pool.query('SELECT * FROM orders WHERE id = $1', [req.params.id]);
        
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Order not found' });
        }
        
        res.json(result.rows[0]);
    } catch (error) {
        console.error('Get order failed:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// =========================
// START SERVER
// =========================

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
    console.log(`🚀 Order service running on port ${PORT}`);
    console.log(`   POST /orders - Create order`);
    console.log(`   GET /orders/:id - Get order`);
});