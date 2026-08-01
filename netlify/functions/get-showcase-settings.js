const db = require('./database');
const requireAuth = require('./auth-middleware');

const publicHeaders = {
  'Content-Type': 'application/json',
  'Cache-Control': 'no-cache, no-store, must-revalidate',
  Pragma: 'no-cache',
  Expires: '0'
};

const getSettings = async () => {
  try {
    const result = await db.query('SELECT settings_json FROM shop_settings WHERE id = 1');
    const shopSettings = result.rows[0]?.settings_json || {};
    const showcaseSettings = shopSettings.showcaseSettings || {
      selectedProductIds: [],
      categories: {},
      maxItems: 10,
      effect: { enabled: false, type: 'confetti', intensity: 30 }
    };

    return {
      statusCode: 200,
      headers: publicHeaders,
      body: JSON.stringify({ showcaseSettings })
    };
  } catch (error) {
    console.error('Failed to fetch showcase settings:', error);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Failed to fetch showcase settings.' })
    };
  }
};

const saveSettings = requireAuth(async (event) => {
  let payload;
  try {
    payload = JSON.parse(event.body || '{}');
  } catch (_) {
    return { statusCode: 400, headers: publicHeaders, body: JSON.stringify({ error: 'Invalid JSON.' }) };
  }

  const showcaseSettings = payload.showcaseSettings;
  if (!showcaseSettings || typeof showcaseSettings !== 'object' || Array.isArray(showcaseSettings)) {
    return { statusCode: 400, headers: publicHeaders, body: JSON.stringify({ error: 'showcaseSettings is required.' }) };
  }

  showcaseSettings.maxItems = Math.min(100000, Math.max(1, Math.floor(Number(showcaseSettings.maxItems) || 10)));
  showcaseSettings.selectedProductIds = Array.isArray(showcaseSettings.selectedProductIds)
    ? [...new Set(showcaseSettings.selectedProductIds.map(Number).filter(Number.isFinite))]
    : [];
  if (!showcaseSettings.categories || typeof showcaseSettings.categories !== 'object' || Array.isArray(showcaseSettings.categories)) {
    showcaseSettings.categories = {};
  }

  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    const result = await client.query('SELECT settings_json FROM shop_settings WHERE id = 1 FOR UPDATE');
    const currentSettings = result.rows[0]?.settings_json || {};
    currentSettings.showcaseSettings = showcaseSettings;
    await client.query('UPDATE shop_settings SET settings_json = $1 WHERE id = 1', [JSON.stringify(currentSettings)]);
    await client.query('COMMIT');
    return { statusCode: 200, headers: publicHeaders, body: JSON.stringify({ showcaseSettings }) };
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
