-- Debezium publication for log-based CDC of the order outbox (production path).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'helios_order_outbox') THEN
        CREATE PUBLICATION helios_order_outbox FOR TABLE outbox;
    END IF;
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'skipping publication (need rds_replication / superuser in prod)';
END $$;
