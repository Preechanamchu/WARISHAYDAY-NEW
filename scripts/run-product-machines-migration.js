'use strict';

const fs = require('fs');
const path = require('path');
const db = require('../netlify/functions/database');

const run = async () => {
  const migrationPath = path.join(__dirname, '..', 'sql', 'create-product-machines.sql');
  const migrationSql = fs.readFileSync(migrationPath, 'utf8');

  try {
    await db.query(migrationSql);
    const result = await db.query('SELECT COUNT(*)::integer AS count FROM product_machines');
    console.log(`Product machines migration completed. Rows available: ${result.rows[0].count}`);
  } finally {
    await db.pool.end();
  }
};

run().catch(error => {
  console.error('Product machines migration failed:', error.message);
  process.exitCode = 1;
});

