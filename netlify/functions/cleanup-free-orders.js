const db = require('./database');

exports.handler = async () => {
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    await client.query('ALTER TABLE orders ADD COLUMN IF NOT EXISTS is_free_order BOOLEAN NOT NULL DEFAULT FALSE');
    await client.query('CREATE INDEX IF NOT EXISTS idx_orders_free_cleanup ON orders (timestamp) WHERE is_free_order = TRUE');
    const result = await client.query(`
      DELETE FROM orders
      WHERE is_free_order = TRUE
        AND timestamp < NOW() - INTERVAL '2 days'
      RETURNING order_id
    `);
    await client.query('COMMIT');
    console.log(`Deleted ${result.rowCount} expired free orders.`);
    return {
      statusCode: 200,
      body: JSON.stringify({ deleted: result.rowCount, retentionDays: 2 })
    };
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to clean up free orders:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to clean up free orders.' })
    };
  } finally {
    client.release();
  }
};
