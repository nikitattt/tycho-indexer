CREATE OR REPLACE FUNCTION public.clean_transaction_table_incremental()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    batch_size constant integer := 200000;
    v_has_lock boolean;
    v_scan_after bigint;
    v_scanned integer := 0;
    v_deleted integer := 0;
    v_last_candidate_id bigint := 0;
    v_attempt integer;
BEGIN
    v_has_lock := pg_try_advisory_xact_lock(907755001, 1);

    IF NOT v_has_lock THEN
        RAISE NOTICE 'transaction cleanup skipped: another cleanup is already running';
        RETURN 0;
    END IF;

    SELECT last_seen_tx_id
    INTO v_scan_after
    FROM public.transaction_cleanup_state
    WHERE singleton_key = true
    FOR UPDATE;

    IF v_scan_after IS NULL THEN
        RAISE EXCEPTION 'transaction_cleanup_state is not initialized';
    END IF;

    FOR v_attempt IN 1..2 LOOP
        WITH candidates AS MATERIALIZED (
            SELECT tx.id
            FROM public."transaction" tx
            WHERE tx.id > v_scan_after
            ORDER BY tx.id
            LIMIT batch_size
        ),
        candidate_stats AS (
            SELECT
                count(*)::integer AS scanned_rows,
                COALESCE(max(id), v_scan_after)::bigint AS last_candidate_id
            FROM candidates
        ),
        orphan_candidates AS MATERIALIZED (
            SELECT c.id
            FROM candidates c
            WHERE NOT EXISTS (
                    SELECT 1 FROM public.contract_code cc WHERE cc.modify_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.protocol_component pc WHERE pc.creation_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.protocol_component pc WHERE pc.deletion_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.account a WHERE a.creation_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.account a WHERE a.deletion_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.account_balance ab WHERE ab.modify_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.component_balance cb WHERE cb.modify_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.protocol_state ps WHERE ps.modify_tx = c.id
                )
              AND NOT EXISTS (
                    SELECT 1 FROM public.contract_storage cs WHERE cs.modify_tx = c.id
                )
        ),
        deleted AS (
            DELETE FROM public."transaction" tx
            USING orphan_candidates orphan
            WHERE tx.id = orphan.id
            RETURNING tx.id
        )
        SELECT
            stats.scanned_rows,
            stats.last_candidate_id,
            delete_stats.deleted_rows
        INTO
            v_scanned,
            v_last_candidate_id,
            v_deleted
        FROM candidate_stats stats
        CROSS JOIN (
            SELECT count(*)::integer AS deleted_rows
            FROM deleted
        ) delete_stats;

        IF v_scanned > 0 OR v_scan_after = 0 THEN
            EXIT;
        END IF;

        v_scan_after := 0;
    END LOOP;

    IF v_scanned = 0 THEN
        v_last_candidate_id := 0;
    END IF;

    UPDATE public.transaction_cleanup_state
    SET last_seen_tx_id = v_last_candidate_id,
        updated_at = now()
    WHERE singleton_key = true;

    RAISE NOTICE
        'transaction cleanup scanned %, deleted %, next scan starts after tx id %',
        v_scanned,
        v_deleted,
        v_last_candidate_id;

    RETURN v_deleted;
END;
$$;
