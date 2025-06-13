CREATE DATABASE staging_process;
\c staging_process;

CREATE TABLE business_rules (
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(50) NOT NULL,
    rule_logic JSONB NOT NULL,
    severity_level VARCHAR(20) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    applies_to_facilities JSONB,
    applies_to_financial_classes JSONB,
    execution_count BIGINT DEFAULT 0,
    failure_count BIGINT DEFAULT 0,
    average_execution_time_ms NUMERIC(10,2),
    created_by VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    version INTEGER DEFAULT 1,
    previous_version_id BIGINT
);

CREATE TABLE claims (
    claim_id VARCHAR(50),
    facility_id VARCHAR(20),
    department_id INTEGER,
    patient_account_number VARCHAR(5),
    patient_name VARCHAR(100),
    date_of_birth DATE,
    service_from_date DATE,
    service_to_date DATE,
    primary_diagnosis VARCHAR(10),
    financial_class VARCHAR(10),
    total_charge_amount NUMERIC(10,2),
    processing_status VARCHAR(20),
    processing_stage VARCHAR(50),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    raw_data JSONB,
    validation_results JSONB,
    ml_predictions JSONB,
    processing_metrics JSONB,
    error_details JSONB,
    priority VARCHAR(10),
    submitted_by VARCHAR(100),
    correlation_id VARCHAR(50)
) PARTITION BY RANGE (service_to_date);

CREATE TABLE claims_default PARTITION OF claims DEFAULT;

CREATE TABLE claims_line_items (
    claim_id VARCHAR(50) NOT NULL,
    line_number INTEGER NOT NULL,
    procedure_code VARCHAR(10) NOT NULL,
    modifier1 VARCHAR(2),
    modifier2 VARCHAR(2),
    modifier3 VARCHAR(2),
    modifier4 VARCHAR(2),
    units INTEGER NOT NULL DEFAULT 1,
    charge_amount NUMERIC(10,2) NOT NULL,
    service_from_date DATE,
    service_to_date DATE,
    diagnosis_pointer VARCHAR(4),
    place_of_service VARCHAR(2),
    revenue_code VARCHAR(4),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    rvu_value NUMERIC(8,4),
    reimbursement_amount NUMERIC(10,2)
);

CREATE TABLE claims_diagnosis_codes (
    claim_id VARCHAR(50) NOT NULL,
    diagnosis_sequence INTEGER NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    diagnosis_description VARCHAR(255),
    diagnosis_type VARCHAR(10),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE batch_metadata (
    batch_id VARCHAR(50) NOT NULL,
    submitted_by VARCHAR(100),
    submitted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    total_claims INTEGER NOT NULL DEFAULT 0,
    valid_claims INTEGER NOT NULL DEFAULT 0,
    invalid_claims INTEGER NOT NULL DEFAULT 0,
    processed_claims INTEGER NOT NULL DEFAULT 0,
    failed_claims INTEGER NOT NULL DEFAULT 0,
    priority VARCHAR(10) DEFAULT 'normal',
    status VARCHAR(20) DEFAULT 'queued',
    processing_options JSONB,
    validation_errors TEXT,
    processing_errors TEXT,
    additional_data JSONB
);

CREATE TABLE processing_metrics (
    metric_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    batch_id VARCHAR(50),
    stage_name VARCHAR(50) NOT NULL,
    worker_id VARCHAR(50),
    records_processed INTEGER NOT NULL DEFAULT 0,
    records_failed INTEGER NOT NULL DEFAULT 0,
    processing_time_seconds NUMERIC(10,3) NOT NULL,
    throughput_per_second NUMERIC(10,2) NOT NULL DEFAULT 0,
    memory_usage_mb INTEGER,
    cpu_usage_percent NUMERIC(5,2),
    additional_metrics JSONB
);

CREATE TABLE ml_models (
    model_name VARCHAR(100) NOT NULL,
    model_version VARCHAR(50) NOT NULL,
    model_path VARCHAR(500),
    model_type VARCHAR(50),
    accuracy NUMERIC(5,4),
    precision_score NUMERIC(5,4),
    recall_score NUMERIC(5,4),
    f1_score NUMERIC(5,4),
    deployed_at TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT FALSE,
    created_by VARCHAR(100),
    training_data_info JSONB,
    model_parameters JSONB,
    feature_columns JSONB,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Foreign key constraints
ALTER TABLE claims_line_items
    ADD CONSTRAINT fk_claim_line_claim FOREIGN KEY (claim_id) REFERENCES claims (claim_id);

ALTER TABLE claims_diagnosis_codes
    ADD CONSTRAINT fk_diagnosis_claim FOREIGN KEY (claim_id) REFERENCES claims (claim_id);

-- Archive procedure for old claims
CREATE TABLE IF NOT EXISTS archived_claims (LIKE claims INCLUDING ALL);

CREATE OR REPLACE PROCEDURE archive_old_claims(cutoff_date DATE)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO archived_claims SELECT * FROM claims WHERE service_to_date < cutoff_date;
    DELETE FROM claims WHERE service_to_date < cutoff_date;
END;
$$;
