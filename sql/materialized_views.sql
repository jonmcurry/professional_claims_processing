-- Materialized views for analytics

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_claim_totals AS
SELECT
    service_to_date AS service_date,
    COUNT(*) AS claim_count,
    SUM(total_charge_amount) AS total_charge
FROM claims
GROUP BY service_to_date
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_failed_claims_summary AS
SELECT
    DATE_TRUNC('day', failed_at) AS fail_day,
    COUNT(*) AS failed_count
FROM failed_claims
GROUP BY DATE_TRUNC('day', failed_at)
WITH DATA;
