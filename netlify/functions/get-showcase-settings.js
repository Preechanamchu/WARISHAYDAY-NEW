const db = require('./database');

exports.handler = async (event) => {
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

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
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        Pragma: 'no-cache',
        Expires: '0'
      },
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
