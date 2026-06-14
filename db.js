require('dotenv').config();
const { Pool } = require('pg');

const pool = new Pool({
    host: process.env.POSTGRES_HOST || 'localhost',
    port: process.env.POSTGRES_PORT || 5432,
    database: process.env.POSTGRES_DB || 'orders',
    user: process.env.POSTGRES_USER || 'postgres',
    password: process.env.POSTGRES_PASSWORD,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

pool.on('error', (err) => {
    console.error('Unexpected PostgreSQL error:', err);
});

async function initializeDatabase() {
    const createOrdersTableQuery = `
        CREATE TABLE IF NOT EXISTS orders (
            id VARCHAR(36) PRIMARY KEY,
            user_id VARCHAR(36) NOT NULL,
            items JSONB NOT NULL,
            total DECIMAL(10,2) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    `;

    const createIndexesQuery = `
        CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
        CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
        CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
    `;

    try {
        await pool.query(createOrdersTableQuery);
        console.log('✅ Orders table verified/created');
        
        await pool.query(createIndexesQuery);
        console.log('✅ Indexes verified/created');
        
        return true;
    } catch (err) {
        console.error('❌ Failed to initialize database:', err.message);
        throw new Error('Database initialization failed');
    }
}

async function saveOrder(orderData) {
    if (!orderData?.id || !orderData?.userId) {
        throw new Error("Invalid order data: missing id or userId");
    }

    const query = `
        INSERT INTO orders (
            id, user_id, items, total, status, created_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (id) DO UPDATE SET
            status = EXCLUDED.status,
            updated_at = EXCLUDED.updated_at
        RETURNING *
    `;

    const values = [
        orderData.id,
        orderData.userId,
        JSON.stringify(orderData.items || []),
        orderData.total || 0,
        orderData.status || 'PENDING',
        orderData.createdAt || new Date(),
        new Date()
    ];

    try {
        const result = await pool.query(query, values);
        return result.rows[0];
    } catch (err) {
        console.error("Error saving order:", err.message);
        throw new Error("Failed to save order");
    }
}

async function getOrder(id) {
    if (!id) {
        throw new Error("Order ID is required");
    }

    try {
        const result = await pool.query(
            'SELECT * FROM orders WHERE id = $1',
            [id]
        );

        if (!result.rows[0]) {
            console.log(`Order ${id} not found`);
            return null;
        }

        return result.rows[0];
    } catch (err) {
        console.error("Error fetching order:", err.message);
        throw new Error("Failed to fetch order");
    }
}

async function getAllOrders(limit = 50, offset = 0) {
    try {
        const result = await pool.query(
            'SELECT * FROM orders ORDER BY created_at DESC LIMIT $1 OFFSET $2',
            [limit, offset]
        );
        return result.rows;
    } catch (err) {
        console.error("Error fetching all orders:", err.message);
        throw new Error("Failed to fetch orders");
    }
}

async function updateOrderStatus(id, status) {
    if (!id || !status) {
        throw new Error("Order ID and status are required");
    }

    try {
        const result = await pool.query(
            'UPDATE orders SET status = $1, updated_at = NOW() WHERE id = $2 RETURNING *',
            [status, id]
        );
        return result.rows[0];
    } catch (err) {
        console.error("Error updating order status:", err.message);
        throw new Error("Failed to update order");
    }
}

async function healthCheck() {
    try {
        await pool.query('SELECT 1');
        return true;
    } catch (err) {
        console.error("Database health check failed:", err.message);
        return false;
    }
}

async function closePool() {
    console.log('Closing PostgreSQL connection pool...');
    await pool.end();
}

initializeDatabase().catch(err => {
    console.error('FATAL: Could not initialize database:', err.message);
    process.exit(1);
});

module.exports = {
    saveOrder,
    getOrder,
    getAllOrders,
    updateOrderStatus,
    healthCheck,
    closePool,
    initializeDatabase,
    pool
};