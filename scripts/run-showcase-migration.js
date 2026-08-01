'use strict';

const fs = require('fs');
const path = require('path');
const db = require('../netlify/functions/database');

const run = async () => {
  const migrationFiles = [
    'create-showcase-settings.sql',
    'create-free-order-retention.sql'
  ];

  try {
    for (const fileName of migrationFiles) {
      const migrationPath = path.join(__dirname, '..', 'sql', fileName);
      await db.query(fs.readFileSync(migrationPath, 'utf8'));
      console.log(`Applied ${fileName}`);
    }
    const [settingsResult, productsResult] = await Promise.all([
      db.query('SELECT COUNT(*)::integer AS count FROM showcase_settings'),
      db.query('SELECT COUNT(*)::integer AS count FROM showcase_products')
    ]);
    console.log(`Showcase migration completed. Settings: ${settingsResult.rows[0].count}, selected products: ${productsResult.rows[0].count}`);
  } finally {
    await db.pool.end();
  }
};

run().catch(error => {
  console.error('Showcase migration failed:', error.message);
  process.exitCode = 1;
});
