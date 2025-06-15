-- Updated PostgreSQL Schema for create_postgres_schema.sql
-- CLEAN ARCHITECTURE: Only staging and processing tables
-- NO reference data duplication (facilities, financial_classes, rvu_data all in SQL Server only)

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
    patient_account_number VARCHAR(50),
    patient_name VARCHAR(100),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    medical_record_number VARCHAR(50),
    date_of_birth DATE,
    gender VARCHAR(1),
    service_from_date DATE,
    service_to_date DATE,
    primary_diagnosis VARCHAR(10),
    financial_class VARCHAR(10),
    secondary_insurance VARCHAR(10),
    total_charge_amount NUMERIC(10,2),
    processing_status VARCHAR(20),
    processing_stage VARCHAR(50),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    raw_data JSONB,
    validation_results JSONB,
    ml_predictions JSONB,
    processing_metrics JSONB,
    error_details JSONB,
    priority VARCHAR(10),
    submitted_by VARCHAR(100),
    correlation_id VARCHAR(50),
    PRIMARY KEY (claim_id, service_to_date)
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
    service_to_date DATE NOT NULL,
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
    additional_data JSONB,
    PRIMARY KEY (batch_id)
);

CREATE TABLE processing_metrics (
    metric_id SERIAL PRIMARY KEY,
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

CREATE TABLE dead_letter_queue (
    dlq_id SERIAL PRIMARY KEY,
    claim_id VARCHAR(50) NOT NULL,
    reason TEXT NOT NULL,
    data TEXT NOT NULL,
    inserted_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE processing_checkpoints (
    claim_id VARCHAR(50) NOT NULL,
    stage VARCHAR(50) NOT NULL,
    checkpoint_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (claim_id, stage)
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

-- Table to store claims that failed processing
CREATE TABLE failed_claims (
    claim_id VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50),
    facility_id VARCHAR(20),
    patient_account_number VARCHAR(50),
    original_data TEXT,
    failure_reason TEXT NOT NULL,
    failure_category VARCHAR(50) NOT NULL,
    processing_stage VARCHAR(50) NOT NULL,
    failed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    repair_suggestions TEXT,
    resolution_status VARCHAR(20) DEFAULT 'pending',
    assigned_to VARCHAR(100),
    resolved_at TIMESTAMPTZ,
    resolution_notes TEXT,
    resolution_action VARCHAR(50),
    error_pattern_id VARCHAR(50),
    priority_level VARCHAR(10) DEFAULT 'medium',
    impact_level VARCHAR(10),
    potential_revenue_loss NUMERIC(12,2),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX idx_claims_facility_id ON claims(facility_id);
CREATE INDEX idx_claims_status ON claims(processing_status);
CREATE INDEX idx_claims_service_date ON claims(service_from_date);
CREATE INDEX idx_claims_line_items_claim_id ON claims_line_items(claim_id);
CREATE INDEX idx_claims_diagnosis_claim_id ON claims_diagnosis_codes(claim_id);
CREATE INDEX idx_failed_claims_facility ON failed_claims(facility_id);
CREATE INDEX idx_failed_claims_category ON failed_claims(failure_category);
CREATE INDEX idx_failed_claims_status ON failed_claims(resolution_status);
CREATE INDEX idx_processing_metrics_batch ON processing_metrics(batch_id);
CREATE INDEX idx_processing_metrics_timestamp ON processing_metrics(metric_timestamp);

-- Comments for documentation
COMMENT ON DATABASE staging_process IS 'PostgreSQL staging database - CLEAN ARCHITECTURE: only processing tables, no reference data duplication';
COMMENT ON TABLE claims IS 'Claims being processed - references facilities/financial_classes in SQL Server via validation services';
COMMENT ON TABLE failed_claims IS 'Claims that failed validation or processing with detailed error information';