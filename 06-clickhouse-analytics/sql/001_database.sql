-- 001_database.sql -- analytics database only.
--
-- P06 never creates, alters or drops md_ingest.market_events_raw: that table
-- belongs to Project 05 and FR-01 requires reading it without modification.
-- A schema preflight compares the live raw DDL against a pinned hash instead
-- (see 006_audit_queries.sql and src/md_analytics/schema_preflight.py).

CREATE DATABASE IF NOT EXISTS md_analytics;
