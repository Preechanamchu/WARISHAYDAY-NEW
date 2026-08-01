const db = require('./database');
const requireAuth = require('./auth-middleware');

const publicHeaders = {
  'Content-Type': 'application/json',
  'Cache-Control': 'no-cache, no-store, must-revalidate',
  Pragma: 'no-cache',
  Expires: '0'
};

const EFFECT_TYPES = new Set(['confetti', 'sparkles', 'balloons', 'petals', 'fireworks']);

const normalizeSettings = (raw = {}) => {
  const effect = raw.effect && typeof raw.effect === 'object' ? raw.effect : {};
  const categories = raw.categories && typeof raw.categories === 'object' && !Array.isArray(raw.categories)
    ? raw.categories
    : {};
  return {
    selectedProductIds: Array.isArray(raw.selectedProductIds)
      ? [...new Set(raw.selectedProductIds.map(Number).filter(Number.isFinite))]
      : [],
    categories,
    maxItems: Math.min(100000, Math.max(1, Math.floor(Number(raw.maxItems) || 10))),
    effect: {
      enabled: effect.enabled === true,
      type: EFFECT_TYPES.has(effect.type) ? effect.type : 'confetti',
      intensity: Math.min(80, Math.max(10, Math.floor(Number(effect.intensity) || 30)))
    }
  };
};

const ensureTables = async (client) => {
  await client.query(`
    CREATE TABLE IF NOT EXISTS showcase_settings (
      id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
      max_items INTEGER NOT NULL DEFAULT 10 CHECK (max_items BETWEEN 1 AND 100000),
      effect_enabled BOOLEAN NOT NULL DEFAULT FALSE,
      effect_type TEXT NOT NULL DEFAULT 'confetti',
      effect_intensity INTEGER NOT NULL DEFAULT 30 CHECK (effect_intensity BETWEEN 10 AND 80),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS showcase_category_settings (
      category_id INTEGER PRIMARY KEY REFERENCES categories(id) ON DELETE CASCADE,
      title TEXT NOT NULL DEFAULT '',
      font_size INTEGER NOT NULL DEFAULT 26 CHECK (font_size BETWEEN 12 AND 64),
      font_family TEXT NOT NULL DEFAULT '''Kanit'', sans-serif',
      text_color VARCHAR(20) NOT NULL DEFAULT '#172554',
      stroke_color VARCHAR(20) NOT NULL DEFAULT '#ffffff',
      shadow_enabled BOOLEAN NOT NULL DEFAULT TRUE,
      shadow_strength INTEGER NOT NULL DEFAULT 6 CHECK (shadow_strength BETWEEN 0 AND 20),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS showcase_products (
      product_id INTEGER PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
};

const readDedicatedSettings = async (queryable) => {
  const [settingsResult, categoriesResult, productsResult] = await Promise.all([
    queryable.query('SELECT * FROM showcase_settings WHERE id = 1'),
    queryable.query('SELECT * FROM showcase_category_settings ORDER BY category_id'),
    queryable.query('SELECT product_id FROM showcase_products ORDER BY product_id')
  ]);
  if (!settingsResult.rows[0]) return null;
  const row = settingsResult.rows[0];
  const categories = {};
  categoriesResult.rows.forEach(category => {
    categories[String(category.category_id)] = {
      title: category.title || '',
      fontSize: Number(category.font_size) || 26,
      fontFamily: category.font_family || "'Kanit', sans-serif",
      color: category.text_color || '#172554',
      strokeColor: category.stroke_color || '#ffffff',
      shadowEnabled: category.shadow_enabled === true,
      shadowStrength: Number(category.shadow_strength) || 0
    };
  });
  return normalizeSettings({
    selectedProductIds: productsResult.rows.map(product => Number(product.product_id)),
    categories,
    maxItems: row.max_items,
    effect: {
      enabled: row.effect_enabled,
      type: row.effect_type,
      intensity: row.effect_intensity
    }
  });
};

const readLegacySettings = async () => {
  const result = await db.query('SELECT settings_json FROM shop_settings WHERE id = 1');
  return normalizeSettings(result.rows[0]?.settings_json?.showcaseSettings || {});
};

const getPublicShowcaseSettings = async () => {
  let showcaseSettings;
  try {
    showcaseSettings = await readDedicatedSettings(db);
  } catch (error) {
    if (error.code !== '42P01') throw error;
  }
  return showcaseSettings || readLegacySettings();
};

const getSettings = async () => {
  try {
    const showcaseSettings = await getPublicShowcaseSettings();
    return { statusCode: 200, headers: publicHeaders, body: JSON.stringify({ showcaseSettings }) };
  } catch (error) {
    console.error('Failed to fetch showcase settings:', error);
    return { statusCode: 500, headers: publicHeaders, body: JSON.stringify({ error: 'Failed to fetch showcase settings.' }) };
  }
};

const saveSettings = requireAuth(async (event) => {
  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch (_) {
    return { statusCode: 400, headers: publicHeaders, body: JSON.stringify({ error: 'Invalid JSON.' }) };
  }
  if (!payload.showcaseSettings || typeof payload.showcaseSettings !== 'object') {
    return { statusCode: 400, headers: publicHeaders, body: JSON.stringify({ error: 'showcaseSettings is required.' }) };
  }
  const settings = normalizeSettings(payload.showcaseSettings);
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    await ensureTables(client);
    await client.query(`
      INSERT INTO showcase_settings (id, max_items, effect_enabled, effect_type, effect_intensity, updated_at)
      VALUES (1, $1, $2, $3, $4, NOW())
      ON CONFLICT (id) DO UPDATE SET
        max_items = EXCLUDED.max_items,
        effect_enabled = EXCLUDED.effect_enabled,
        effect_type = EXCLUDED.effect_type,
        effect_intensity = EXCLUDED.effect_intensity,
        updated_at = NOW()
    `, [settings.maxItems, settings.effect.enabled, settings.effect.type, settings.effect.intensity]);

    await client.query('DELETE FROM showcase_category_settings');
    for (const [categoryId, rawConfig] of Object.entries(settings.categories)) {
      const id = Number(categoryId);
      if (!Number.isInteger(id)) continue;
      const config = rawConfig && typeof rawConfig === 'object' ? rawConfig : {};
      await client.query(`
        INSERT INTO showcase_category_settings (
          category_id, title, font_size, font_family, text_color,
          stroke_color, shadow_enabled, shadow_strength, updated_at
        )
        SELECT $1, $2, $3, $4, $5, $6, $7, $8, NOW()
        WHERE EXISTS (SELECT 1 FROM categories WHERE id = $1)
      `, [
        id,
        String(config.title || '').slice(0, 500),
        Math.min(64, Math.max(12, Number(config.fontSize) || 26)),
        String(config.fontFamily || "'Kanit', sans-serif").slice(0, 150),
        String(config.color || '#172554').slice(0, 20),
        String(config.strokeColor || '#ffffff').slice(0, 20),
        config.shadowEnabled === true,
        Math.min(20, Math.max(0, Number(config.shadowStrength) || 0))
      ]);
    }

    await client.query('DELETE FROM showcase_products');
    if (settings.selectedProductIds.length) {
      await client.query(`
        INSERT INTO showcase_products (product_id)
        SELECT id FROM products WHERE id = ANY($1::int[])
        ON CONFLICT (product_id) DO NOTHING
      `, [settings.selectedProductIds]);
    }
    await client.query('COMMIT');
    const savedSettings = await readDedicatedSettings(db);
    return { statusCode: 200, headers: publicHeaders, body: JSON.stringify({ showcaseSettings: savedSettings }) };
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Failed to save showcase settings:', error);
    return { statusCode: 500, headers: publicHeaders, body: JSON.stringify({ error: 'Failed to save showcase settings.' }) };
  } finally {
    client.release();
  }
});

exports.handler = async (event, context) => {
  if (event.httpMethod === 'GET') return getSettings();
  if (event.httpMethod === 'POST' || event.httpMethod === 'PUT') return saveSettings(event, context);
  return { statusCode: 405, headers: publicHeaders, body: 'Method Not Allowed' };
};

exports.getPublicShowcaseSettings = getPublicShowcaseSettings;
