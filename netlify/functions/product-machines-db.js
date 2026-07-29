'use strict';

/**
 * Shared persistence helpers for the public/admin product-machine catalog.
 * The JSON setting remains as a backward-compatible copy, while this table is
 * the database source of truth once the Neon migration has been applied.
 */

const normalizeProductIds = (value) => {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch (_) {
      return [];
    }
  }
  return [];
};

const normalizeMachine = (machine, index = 0) => ({
  id: String(machine?.id || `machine_${Date.now()}_${index}`),
  name: String(machine?.name ?? machine?.name_th ?? ''),
  name_en: String(machine?.name_en ?? ''),
  imageUrl: String(machine?.imageUrl ?? machine?.image_url ?? ''),
  productIds: normalizeProductIds(machine?.productIds ?? machine?.product_ids),
  sortOrder: Number.isFinite(Number(machine?.sortOrder ?? machine?.sort_order))
    ? Number(machine?.sortOrder ?? machine?.sort_order)
    : index
});

const readProductMachines = async (queryable, legacyMachines = []) => {
  try {
    const result = await queryable.query(`
      SELECT id, name_th, name_en, image_url, product_ids, sort_order
      FROM product_machines
      ORDER BY sort_order ASC, created_at ASC, id ASC
    `);

    if (!result.rows.length && Array.isArray(legacyMachines) && legacyMachines.length) {
      return legacyMachines.map(normalizeMachine);
    }

    return result.rows.map((row, index) => normalizeMachine(row, index));
  } catch (error) {
    if (error?.code === '42P01') {
      console.warn('[product-machines] Table is missing; using legacy settings_json data.');
      return Array.isArray(legacyMachines) ? legacyMachines.map(normalizeMachine) : [];
    }
    throw error;
  }
};

const syncProductMachines = async (client, machines) => {
  if (!Array.isArray(machines)) return;
  if (machines.length > 500) {
    throw new Error('Product machine limit exceeded (maximum 500).');
  }

  // Allow the frontend/API deployment to go live before the SQL migration.
  // Until the table exists, settings_json remains the database fallback.
  const tableCheck = await client.query(
    "SELECT to_regclass('public.product_machines') AS table_name"
  );
  if (!tableCheck.rows[0]?.table_name) {
    console.warn('[product-machines] Migration pending; catalog saved in settings_json only.');
    return false;
  }

  const uniqueMachines = [];
  const seenIds = new Set();
  machines.forEach((machine, index) => {
    const normalized = normalizeMachine(machine, index);
    if (seenIds.has(normalized.id)) return;
    seenIds.add(normalized.id);
    uniqueMachines.push(normalized);
  });

  const ids = uniqueMachines.map(machine => machine.id);
  await client.query(
    'DELETE FROM product_machines WHERE NOT (id = ANY($1::text[]))',
    [ids]
  );

  for (const machine of uniqueMachines) {
    await client.query(`
      INSERT INTO product_machines
        (id, name_th, name_en, image_url, product_ids, sort_order, updated_at)
      VALUES ($1, $2, $3, $4, $5::jsonb, $6, NOW())
      ON CONFLICT (id) DO UPDATE SET
        name_th = EXCLUDED.name_th,
        name_en = EXCLUDED.name_en,
        image_url = EXCLUDED.image_url,
        product_ids = EXCLUDED.product_ids,
        sort_order = EXCLUDED.sort_order,
        updated_at = NOW()
    `, [
      machine.id,
      machine.name,
      machine.name_en,
      machine.imageUrl,
      JSON.stringify(machine.productIds),
      machine.sortOrder
    ]);
  }

  return true;
};

module.exports = {
  readProductMachines,
  syncProductMachines
};
