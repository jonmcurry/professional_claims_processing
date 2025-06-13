-- Indexes for PostgreSQL
CREATE INDEX IF NOT EXISTS idx_claims_claim_id ON claims (claim_id);
CREATE INDEX IF NOT EXISTS idx_failed_claims_claim_id ON failed_claims (claim_id);

-- Indexes for SQL Server
GO
CREATE INDEX idx_sql_claims_claim_id ON claims (claim_id);
GO
CREATE INDEX idx_sql_failed_claims_claim_id ON failed_claims (claim_id);
GO
