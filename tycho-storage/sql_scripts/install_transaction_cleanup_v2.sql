\set ON_ERROR_STOP on

-- Installer for transaction cleanup v2.
-- Runtime logic lives in clean_transaction_table_incremental.sql.

SET search_path = public, pg_catalog;

DROP FUNCTION IF EXISTS public.clean_transaction_table_incremental(integer, interval);
DROP TABLE IF EXISTS public.transaction_cleanup_run_log;

CREATE TABLE IF NOT EXISTS public.transaction_cleanup_state (
    singleton_key boolean PRIMARY KEY DEFAULT true,
    last_seen_tx_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT transaction_cleanup_state_singleton CHECK (singleton_key)
);

INSERT INTO public.transaction_cleanup_state (singleton_key, last_seen_tx_id)
VALUES (true, 0)
ON CONFLICT (singleton_key) DO NOTHING;

\ir clean_transaction_table_incremental.sql

CREATE OR REPLACE FUNCTION public.clean_transaction_table()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM public.clean_transaction_table_incremental();
END;
$$;
