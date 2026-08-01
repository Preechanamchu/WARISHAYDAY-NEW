BEGIN;

CREATE TABLE IF NOT EXISTS showcase_settings (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    max_items INTEGER NOT NULL DEFAULT 10 CHECK (max_items BETWEEN 1 AND 100000),
    effect_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    effect_type TEXT NOT NULL DEFAULT 'confetti'
        CHECK (effect_type IN ('confetti', 'sparkles', 'balloons', 'petals', 'fireworks')),
    effect_intensity INTEGER NOT NULL DEFAULT 30 CHECK (effect_intensity BETWEEN 10 AND 80),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
);

CREATE TABLE IF NOT EXISTS showcase_products (
    product_id INTEGER PRIMARY KEY REFERENCES products(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO showcase_settings (id, max_items, effect_enabled, effect_type, effect_intensity)
VALUES (1, 10, FALSE, 'confetti', 30)
ON CONFLICT (id) DO NOTHING;

-- Migrate the existing showcase configuration from shop_settings.settings_json.
WITH legacy AS (
    SELECT COALESCE(settings_json::jsonb -> 'showcaseSettings', '{}'::jsonb) AS value
    FROM shop_settings
    WHERE id = 1
)
UPDATE showcase_settings
SET max_items = LEAST(100000, GREATEST(1, COALESCE((legacy.value ->> 'maxItems')::INTEGER, max_items))),
    effect_enabled = COALESCE((legacy.value -> 'effect' ->> 'enabled')::BOOLEAN, effect_enabled),
    effect_type = CASE
        WHEN legacy.value -> 'effect' ->> 'type' IN ('confetti', 'sparkles', 'balloons', 'petals', 'fireworks')
            THEN legacy.value -> 'effect' ->> 'type'
        ELSE effect_type
    END,
    effect_intensity = LEAST(80, GREATEST(10, COALESCE((legacy.value -> 'effect' ->> 'intensity')::INTEGER, effect_intensity))),
    updated_at = NOW()
FROM legacy
WHERE showcase_settings.id = 1;

WITH legacy_categories AS (
    SELECT entry.key AS category_id, entry.value
    FROM shop_settings settings
    CROSS JOIN LATERAL jsonb_each(COALESCE(settings.settings_json::jsonb -> 'showcaseSettings' -> 'categories', '{}'::jsonb)) entry
    WHERE settings.id = 1 AND entry.key ~ '^\d+$'
)
INSERT INTO showcase_category_settings (
    category_id, title, font_size, font_family, text_color,
    stroke_color, shadow_enabled, shadow_strength
)
SELECT
    categories.id,
    COALESCE(legacy_categories.value ->> 'title', ''),
    LEAST(64, GREATEST(12, COALESCE((legacy_categories.value ->> 'fontSize')::INTEGER, 26))),
    COALESCE(NULLIF(legacy_categories.value ->> 'fontFamily', ''), '''Kanit'', sans-serif'),
    COALESCE(NULLIF(legacy_categories.value ->> 'color', ''), '#172554'),
    COALESCE(NULLIF(legacy_categories.value ->> 'strokeColor', ''), '#ffffff'),
    COALESCE((legacy_categories.value ->> 'shadowEnabled')::BOOLEAN, TRUE),
    LEAST(20, GREATEST(0, COALESCE((legacy_categories.value ->> 'shadowStrength')::INTEGER, 6)))
FROM legacy_categories
JOIN categories ON categories.id = legacy_categories.category_id::INTEGER
ON CONFLICT (category_id) DO UPDATE SET
    title = EXCLUDED.title,
    font_size = EXCLUDED.font_size,
    font_family = EXCLUDED.font_family,
    text_color = EXCLUDED.text_color,
    stroke_color = EXCLUDED.stroke_color,
    shadow_enabled = EXCLUDED.shadow_enabled,
    shadow_strength = EXCLUDED.shadow_strength,
    updated_at = NOW();

WITH selected_products AS (
    SELECT DISTINCT value::INTEGER AS product_id
    FROM shop_settings settings
    CROSS JOIN LATERAL jsonb_array_elements_text(
        COALESCE(settings.settings_json::jsonb -> 'showcaseSettings' -> 'selectedProductIds', '[]'::jsonb)
    )
    WHERE settings.id = 1 AND value ~ '^\d+$'
)
INSERT INTO showcase_products (product_id)
SELECT products.id
FROM selected_products
JOIN products ON products.id = selected_products.product_id
ON CONFLICT (product_id) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_showcase_category_updated_at
    ON showcase_category_settings(updated_at DESC);

COMMIT;
