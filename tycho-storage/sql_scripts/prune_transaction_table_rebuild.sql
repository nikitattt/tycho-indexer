\set ON_ERROR_STOP on
\timing on

-- Rebuild public.transaction with only referenced transaction rows.
--
-- This is intended for very large orphan cleanup where DELETE would be too slow
-- and would not return disk space without a later table rewrite anyway.
--
-- This script is destructive:
-- - It creates public.transaction_new.
-- - It drops and recreates all foreign keys that reference public.transaction.
-- - It renames the old transaction table to transaction_old.
-- - It renames transaction_new to transaction.
-- - It drops transaction_old.
--
-- Safety properties:
-- - It keeps every transaction id currently referenced by a foreign key to
--   public.transaction(id), including partitioned *_default tables.
-- - It does not try to decide by protocol semantics, tx hash, or age.
-- - If historical partitions still exist, their referenced tx rows are kept.
-- - It refuses to run if public.transaction is partitioned or has custom
--   constraints/indexes/triggers that this script does not explicitly preserve.
--
-- Operational requirements:
-- - Stop tycho-indexer and any other writers before running this.
-- - Take a database backup/snapshot first.
-- - Run from psql with: -v CONFIRM_TRANSACTION_REBUILD=1
-- - Expect an ACCESS EXCLUSIVE lock on public.transaction and referencing tables.

\if :{?CONFIRM_TRANSACTION_REBUILD}
\else
    \echo 'Refusing to run. Re-run with: -v CONFIRM_TRANSACTION_REBUILD=1'
    \quit 1
\endif

SET statement_timeout = 0;
SET lock_timeout = '30s';
SET idle_in_transaction_session_timeout = 0;
SET search_path = public, pg_catalog;

BEGIN;

DO $$
DECLARE
    unexpected text;
BEGIN
    IF (
        SELECT c.relkind
        FROM pg_class c
        WHERE c.oid = 'public.transaction'::regclass
    ) <> 'r' THEN
        RAISE EXCEPTION
            'public.transaction is not a plain heap table. This script does not support partitioned or special relkind layouts.';
    END IF;

    SELECT string_agg(c.conname, ', ' ORDER BY c.conname)
    INTO unexpected
    FROM pg_constraint c
    WHERE c.conrelid = 'public.transaction'::regclass
      AND c.conparentid = 0
      AND c.conname NOT IN (
          'transaction_pkey',
          'transaction_hash_key',
          'transaction_index_block_id_key',
          'transaction_block_id_fkey'
      );

    IF unexpected IS NOT NULL THEN
        RAISE EXCEPTION
            'public.transaction has unsupported constraints: %. Review and extend this script before running.',
            unexpected;
    END IF;

    SELECT string_agg(i.indexrelid::regclass::text, ', ' ORDER BY i.indexrelid::regclass::text)
    INTO unexpected
    FROM pg_index i
    WHERE i.indrelid = 'public.transaction'::regclass
      AND i.indexrelid::regclass::text NOT IN (
          'transaction_pkey',
          'transaction_hash_key',
          'transaction_index_block_id_key',
          'idx_transaction_block_id'
      );

    IF unexpected IS NOT NULL THEN
        RAISE EXCEPTION
            'public.transaction has unsupported indexes: %. Review and extend this script before running.',
            unexpected;
    END IF;

    SELECT string_agg(t.tgname, ', ' ORDER BY t.tgname)
    INTO unexpected
    FROM pg_trigger t
    WHERE t.tgrelid = 'public.transaction'::regclass
      AND NOT t.tgisinternal
      AND t.tgname <> 'update_modtime_transaction';

    IF unexpected IS NOT NULL THEN
        RAISE EXCEPTION
            'public.transaction has unsupported triggers: %. Review and extend this script before running.',
            unexpected;
    END IF;

    IF to_regprocedure('public.update_modified_column()') IS NULL THEN
        RAISE EXCEPTION 'Required function public.update_modified_column() does not exist.';
    END IF;
END $$;

SELECT
    'preflight_transaction_size' AS section,
    c.reltuples::bigint AS estimated_tx_rows,
    pg_size_pretty(pg_total_relation_size('public.transaction')) AS transaction_total_size
FROM pg_class c
WHERE c.oid = 'public.transaction'::regclass;

-- Lock transaction first, then every root referencing table and direct child
-- partition. This keeps the FK/rebuild view stable while the table is swapped.
LOCK TABLE public.transaction IN ACCESS EXCLUSIVE MODE;

DO $$
DECLARE
    rel record;
BEGIN
    FOR rel IN
        WITH referencing_roots AS (
            SELECT DISTINCT c.conrelid
            FROM pg_constraint c
            WHERE c.contype = 'f'
              AND c.confrelid = 'public.transaction'::regclass
              AND c.conparentid = 0
        ),
        referencing_rels AS (
            SELECT conrelid AS relid
            FROM referencing_roots
            UNION
            SELECT i.inhrelid AS relid
            FROM pg_inherits i
            JOIN referencing_roots r
                ON r.conrelid = i.inhparent
        )
        SELECT relid::regclass AS relname
        FROM referencing_rels
        ORDER BY relid::regclass::text
    LOOP
        RAISE NOTICE 'Locking %', rel.relname;
        EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', rel.relname);
    END LOOP;
END $$;

CREATE TEMP TABLE temp_transaction_fks ON COMMIT DROP AS
SELECT
    c.oid AS constraint_oid,
    c.conrelid,
    c.conrelid::regclass AS referencing_table,
    c.conname,
    pg_get_constraintdef(c.oid) AS constraint_def
FROM pg_constraint c
WHERE c.contype = 'f'
  AND c.confrelid = 'public.transaction'::regclass
  AND c.conparentid = 0
ORDER BY c.conrelid::regclass::text, c.conname;

SELECT
    'foreign_keys_to_recreate' AS section,
    referencing_table,
    conname,
    constraint_def
FROM temp_transaction_fks
ORDER BY referencing_table::text, conname;

CREATE TEMP TABLE temp_transaction_ids (
    id bigint PRIMARY KEY
) ON COMMIT DROP;

-- Dynamically preserve every transaction row referenced by any single-column FK
-- to transaction(id). Current Tycho schema uses single-column tx references.
DO $$
DECLARE
    fk record;
BEGIN
    FOR fk IN
        SELECT
            c.conrelid::regclass AS referencing_table,
            a.attname AS referencing_column
        FROM pg_constraint c
        JOIN pg_attribute a
            ON a.attrelid = c.conrelid
           AND a.attnum = c.conkey[1]
        WHERE c.contype = 'f'
          AND c.confrelid = 'public.transaction'::regclass
          AND c.conparentid = 0
          AND array_length(c.conkey, 1) = 1
          AND array_length(c.confkey, 1) = 1
        ORDER BY c.conrelid::regclass::text, c.conname
    LOOP
        RAISE NOTICE 'Collecting references from %.%', fk.referencing_table, fk.referencing_column;
        EXECUTE format(
            'INSERT INTO temp_transaction_ids(id)
             SELECT DISTINCT %1$I
             FROM %2$s
             WHERE %1$I IS NOT NULL
             ON CONFLICT (id) DO NOTHING',
            fk.referencing_column,
            fk.referencing_table
        );
    END LOOP;
END $$;

ANALYZE temp_transaction_ids;

SELECT
    'referenced_transaction_ids' AS section,
    count(*) AS referenced_tx_rows
FROM temp_transaction_ids;

SELECT
    'rebuild_plan' AS section,
    (SELECT reltuples::bigint FROM pg_class WHERE oid = 'public.transaction'::regclass)
        AS estimated_current_tx_rows,
    (SELECT count(*) FROM temp_transaction_ids) AS tx_rows_to_keep,
    (SELECT reltuples::bigint FROM pg_class WHERE oid = 'public.transaction'::regclass)
        - (SELECT count(*) FROM temp_transaction_ids) AS estimated_tx_rows_to_remove;

DROP TABLE IF EXISTS public.transaction_new;

CREATE TABLE public.transaction_new (
    LIKE public.transaction
    INCLUDING DEFAULTS
    INCLUDING GENERATED
    INCLUDING IDENTITY
    INCLUDING STORAGE
    INCLUDING COMMENTS
);

INSERT INTO public.transaction_new
SELECT t.*
FROM public.transaction t
JOIN temp_transaction_ids keep
    ON keep.id = t.id;

ALTER TABLE public.transaction_new
    ALTER COLUMN id SET NOT NULL,
    ALTER COLUMN hash SET NOT NULL,
    ALTER COLUMN "from" SET NOT NULL,
    ALTER COLUMN "to" SET NOT NULL,
    ALTER COLUMN "index" SET NOT NULL,
    ALTER COLUMN block_id SET NOT NULL,
    ALTER COLUMN inserted_ts SET NOT NULL,
    ALTER COLUMN modified_ts SET NOT NULL;

ALTER TABLE public.transaction_new
    ALTER COLUMN id SET DEFAULT nextval('public.transaction_id_seq'::regclass),
    ALTER COLUMN inserted_ts SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN modified_ts SET DEFAULT CURRENT_TIMESTAMP;

CREATE UNIQUE INDEX transaction_new_pkey ON public.transaction_new(id);
CREATE UNIQUE INDEX transaction_new_hash_key ON public.transaction_new(hash);
CREATE UNIQUE INDEX transaction_new_index_block_id_key
    ON public.transaction_new("index", block_id);
CREATE INDEX idx_transaction_new_block_id ON public.transaction_new(block_id);

ALTER TABLE public.transaction_new
    ADD CONSTRAINT transaction_new_pkey PRIMARY KEY USING INDEX transaction_new_pkey,
    ADD CONSTRAINT transaction_new_hash_key UNIQUE USING INDEX transaction_new_hash_key,
    ADD CONSTRAINT transaction_new_index_block_id_key
        UNIQUE USING INDEX transaction_new_index_block_id_key,
    ADD CONSTRAINT transaction_new_block_id_fkey
        FOREIGN KEY (block_id) REFERENCES public.block(id) ON DELETE CASCADE;

ANALYZE public.transaction_new;

-- Remove FKs to the old table before swapping.
DO $$
DECLARE
    fk record;
BEGIN
    FOR fk IN
        SELECT referencing_table, conname
        FROM temp_transaction_fks
        ORDER BY referencing_table::text, conname
    LOOP
        RAISE NOTICE 'Dropping FK %.%', fk.referencing_table, fk.conname;
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk.referencing_table, fk.conname);
    END LOOP;
END $$;

ALTER SEQUENCE public.transaction_id_seq OWNED BY NONE;

ALTER TABLE public.transaction RENAME TO transaction_old;
ALTER TABLE public.transaction_new RENAME TO transaction;

-- Drop the old table now to release the canonical index names
-- transaction_pkey / transaction_hash_key / transaction_index_block_id_key.
-- If any later statement fails, the surrounding transaction rolls this back.
DROP TABLE public.transaction_old;

-- Normalize constraint/index names expected by migrations and tooling.
ALTER TABLE public.transaction
    RENAME CONSTRAINT transaction_new_pkey TO transaction_pkey;
ALTER TABLE public.transaction
    RENAME CONSTRAINT transaction_new_hash_key TO transaction_hash_key;
ALTER TABLE public.transaction
    RENAME CONSTRAINT transaction_new_index_block_id_key TO transaction_index_block_id_key;
ALTER TABLE public.transaction
    RENAME CONSTRAINT transaction_new_block_id_fkey TO transaction_block_id_fkey;
ALTER INDEX public.idx_transaction_new_block_id RENAME TO idx_transaction_block_id;

ALTER TABLE public.transaction
    ALTER COLUMN id SET DEFAULT nextval('public.transaction_id_seq'::regclass);
ALTER SEQUENCE public.transaction_id_seq OWNED BY public.transaction.id;

SELECT setval(
    'public.transaction_id_seq'::regclass,
    GREATEST(
        COALESCE((SELECT max(id) FROM public.transaction), 1),
        COALESCE((SELECT last_value FROM public.transaction_id_seq), 1)
    ),
    true
);

CREATE TRIGGER update_modtime_transaction
    BEFORE UPDATE ON public.transaction
    FOR EACH ROW
    EXECUTE FUNCTION public.update_modified_column();

-- Recreate FKs so they point to the new public.transaction table.
DO $$
DECLARE
    fk record;
BEGIN
    FOR fk IN
        SELECT referencing_table, conname, constraint_def
        FROM temp_transaction_fks
        ORDER BY referencing_table::text, conname
    LOOP
        RAISE NOTICE 'Recreating FK %.%', fk.referencing_table, fk.conname;
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I %s',
            fk.referencing_table,
            fk.conname,
            fk.constraint_def
        );
    END LOOP;
END $$;

SELECT
    'post_swap_transaction_size_before_drop_old' AS section,
    count(*) AS tx_rows,
    pg_size_pretty(pg_total_relation_size('public.transaction')) AS transaction_total_size
FROM public.transaction;

ANALYZE public.transaction;

SELECT
    'post_rebuild_transaction_size' AS section,
    count(*) AS tx_rows,
    pg_size_pretty(pg_total_relation_size('public.transaction')) AS transaction_total_size
FROM public.transaction;

COMMIT;
