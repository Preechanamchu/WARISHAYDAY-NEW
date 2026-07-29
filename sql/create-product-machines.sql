BEGIN;

-- Shared catalog for the "สั่งซื้อสินค้าตามเครื่อง" page.
CREATE TABLE IF NOT EXISTS product_machines (
    id TEXT PRIMARY KEY,
    name_th TEXT NOT NULL DEFAULT '',
    name_en TEXT NOT NULL DEFAULT '',
    image_url TEXT NOT NULL DEFAULT '',
    product_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT product_machines_product_ids_array
        CHECK (jsonb_typeof(product_ids) = 'array')
);

CREATE INDEX IF NOT EXISTS idx_product_machines_sort_order
    ON product_machines (sort_order, created_at, id);

-- Migrate machines already saved in shop_settings.settings_json. Re-running
-- this migration is safe and refreshes rows with the same machine ID.
INSERT INTO product_machines
    (id, name_th, name_en, image_url, product_ids, sort_order)
SELECT
    COALESCE(NULLIF(machine.value ->> 'id', ''), 'machine_migrated_' || machine.ordinality),
    COALESCE(machine.value ->> 'name', ''),
    COALESCE(machine.value ->> 'name_en', ''),
    COALESCE(machine.value ->> 'imageUrl', ''),
    CASE
        WHEN jsonb_typeof(machine.value -> 'productIds') = 'array'
            THEN machine.value -> 'productIds'
        ELSE '[]'::jsonb
    END,
    (machine.ordinality - 1)::integer
FROM shop_settings AS settings
CROSS JOIN LATERAL jsonb_array_elements(
    CASE
        WHEN jsonb_typeof(settings.settings_json::jsonb -> 'productMachines') = 'array'
            THEN settings.settings_json::jsonb -> 'productMachines'
        ELSE '[]'::jsonb
    END
) WITH ORDINALITY AS machine(value, ordinality)
WHERE settings.id = 1
ON CONFLICT (id) DO UPDATE SET
    name_th = EXCLUDED.name_th,
    name_en = EXCLUDED.name_en,
    image_url = EXCLUDED.image_url,
    product_ids = EXCLUDED.product_ids,
    sort_order = EXCLUDED.sort_order,
    updated_at = NOW();

CREATE OR REPLACE FUNCTION set_product_machines_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_product_machines_updated_at ON product_machines;
CREATE TRIGGER trg_product_machines_updated_at
BEFORE UPDATE ON product_machines
FOR EACH ROW
EXECUTE FUNCTION set_product_machines_updated_at();

COMMIT;

-- Verification:
-- SELECT id, name_th, name_en, image_url, product_ids, sort_order
-- FROM product_machines
-- ORDER BY sort_order, created_at, id;
