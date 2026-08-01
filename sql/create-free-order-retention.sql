BEGIN;

-- Only orders created from the free storefront (?s=1) are marked TRUE.
-- Existing and normal paid orders remain FALSE and are never deleted by cleanup.
ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS is_free_order BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_orders_free_cleanup
    ON orders (timestamp)
    WHERE is_free_order = TRUE;

CREATE OR REPLACE FUNCTION cleanup_expired_free_orders()
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count BIGINT;
BEGIN
    DELETE FROM orders
    WHERE is_free_order = TRUE
      AND timestamp < NOW() - INTERVAL '2 days';

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$;

COMMIT;
