\set ON_ERROR_STOP on

-- Keep scheduling separate from the cleanup function definition.
-- This intentionally only changes the pg_cron job.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM cron.job
        WHERE jobname = 'clean_transaction_table'
    ) THEN
        UPDATE cron.job
        SET schedule = '0 * * * *',
            command = 'SELECT clean_transaction_table();',
            active = true
        WHERE jobname = 'clean_transaction_table';
    ELSE
        PERFORM cron.schedule(
            'clean_transaction_table',
            '0 * * * *',
            'SELECT clean_transaction_table();'
        );
    END IF;
END;
$$;

SELECT
    jobid,
    jobname,
    schedule,
    database,
    username,
    active,
    command
FROM cron.job
WHERE jobname = 'clean_transaction_table';
