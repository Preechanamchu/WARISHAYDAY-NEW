'use strict';

const fs = require('fs');
const path = require('path');
const db = require('../netlify/functions/database');

const run = async () => {
  if (!process.env.DATABASE_URL) {
    throw new Error('DATABASE_URL is not configured. Create a .env file or set $env:DATABASE_URL in PowerShell before running this migration.');
  }

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
    const [settingsResult, categoriesResult, productsResult, freeOrderColumnResult, cleanupFunctionResult] = await Promise.all([
      db.query('SELECT COUNT(*)::integer AS count FROM showcase_settings'),
      db.query('SELECT COUNT(*)::integer AS count FROM showcase_category_settings'),
      db.query('SELECT COUNT(*)::integer AS count FROM showcase_products'),
      db.query("SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'is_free_order') AS present"),
      db.query("SELECT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'cleanup_expired_free_orders') AS present")
    ]);
    console.log(`Showcase migration completed. Settings: ${settingsResult.rows[0].count}, category settings: ${categoriesResult.rows[0].count}, selected products: ${productsResult.rows[0].count}`);
    console.log(`Free-order retention ready. Column: ${freeOrderColumnResult.rows[0].present}, cleanup function: ${cleanupFunctionResult.rows[0].present}`);
  } finally {
    await db.pool.end();
  }
};

run().catch(error => {
  console.error('Showcase migration failed:', error?.message || error?.code || String(error));
  if (error?.code) console.error(`Database error code: ${error.code}`);
  process.exitCode = 1;
});
