CREATE DATABASE smart_pro_claims;
GO
USE smart_pro_claims;
GO

CREATE TABLE audit_log (
    table_name VARCHAR(100) NOT NULL,
    record_id VARCHAR(50) NOT NULL,
    operation VARCHAR(20) NOT NULL,
    user_id VARCHAR(100) NULL,
    session_id VARCHAR(100) NULL,
    ip_address VARCHAR(45) NULL,
    user_agent VARCHAR(500) NULL,
    old_values NVARCHAR(MAX) NULL,
    new_values NVARCHAR(MAX) NULL,
    changed_columns NVARCHAR(500) NULL,
    operation_timestamp DATETIME2(7) NULL,
    reason VARCHAR(500) NULL,
    approval_required BIT NULL,
    approved_by VARCHAR(100) NULL,
    approved_at DATETIME2(7) NULL
);
GO
CREATE INDEX idx_audit_log_record ON audit_log(record_id, operation_timestamp);
-- Consider including table_name in a composite index if queries often
-- filter by table

CREATE TABLE daily_processing_summary (
    summary_date DATE NOT NULL,
    facility_id VARCHAR(20) NULL,
    total_claims_processed INT NULL,
    total_claims_failed INT NULL,
    total_line_items INT NULL,
    total_charge_amount DECIMAL(15,2) NULL,
    total_reimbursement_amount DECIMAL(15,2) NULL,
    average_reimbursement_rate DECIMAL(5,4) NULL,
    average_processing_time_seconds DECIMAL(8,2) NULL,
    throughput_claims_per_hour DECIMAL(10,2) NULL,
    error_rate_percentage DECIMAL(5,2) NULL,
    ml_accuracy_percentage DECIMAL(5,2) NULL,
    validation_pass_rate DECIMAL(5,2) NULL,
    created_at DATETIME2(7) NULL
);
GO
CREATE CLUSTERED COLUMNSTORE INDEX cci_daily_processing_summary
    ON daily_processing_summary;
GO

CREATE TABLE data_access_log (
    access_timestamp DATETIME2(7) NULL,
    user_id VARCHAR(100) NOT NULL,
    user_role VARCHAR(50) NULL,
    department VARCHAR(100) NULL,
    table_name VARCHAR(100) NOT NULL,
    record_id VARCHAR(50) NULL,
    access_type VARCHAR(20) NULL,
    data_classification VARCHAR(20) NULL,
    business_justification VARCHAR(500) NULL,
    patient_account_number VARCHAR(50) NULL,
    facility_id VARCHAR(20) NULL,
    ip_address VARCHAR(45) NULL,
    application_name VARCHAR(100) NULL,
    query_executed NVARCHAR(MAX) NULL
);
GO

CREATE TABLE facility_organization (
    org_id INT,
    org_name VARCHAR(100),
    installed_date DATETIME,
    updated_by INT
);
GO

CREATE TABLE facility_region (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(100)
);
GO

CREATE TABLE facilities (
    facility_id INT PRIMARY KEY,
    facility_name VARCHAR(100),
    facility_type VARCHAR(50),
    address VARCHAR(200),
    installed_date DATETIME,
    beds INT,
    city VARCHAR(24),
    state CHAR(2),
    zip_code VARCHAR(10),
    phone VARCHAR(20),
    active BIT DEFAULT 1,
    updated_date DATETIME,
    updated_by INT,
    region_id INT,
    fiscal_month INT
);
GO

ALTER TABLE facilities
    ADD CONSTRAINT fk_facility_region FOREIGN KEY (region_id)
        REFERENCES facility_region(region_id);

CREATE INDEX idx_facilities_region_id ON facilities (region_id);
GO

CREATE TABLE core_standard_payers (
    payer_id INT PRIMARY KEY,
    payer_name VARCHAR(20),
    payer_code CHAR(2)
);
GO

CREATE TABLE facility_financial_classes (
    facility_id INT,
    financial_class_id VARCHAR(10) PRIMARY KEY,
    financial_class_name VARCHAR(100),
    payer_id INT,
    reimbursement_rate DECIMAL(5,4),
    processing_priority VARCHAR(10),
    auto_posting_enabled BIT,
    active BIT,
    effective_date DATE,
    end_date DATE,
    created_at DATETIME,
    HCC CHAR(3)
);
GO

ALTER TABLE facility_financial_classes
    ADD CONSTRAINT fk_ffc_facility FOREIGN KEY (facility_id)
        REFERENCES facilities(facility_id);
ALTER TABLE facility_financial_classes
    ADD CONSTRAINT fk_ffc_payer FOREIGN KEY (payer_id)
        REFERENCES core_standard_payers(payer_id);

CREATE INDEX idx_facility_financial_classes_facility_id
    ON facility_financial_classes (facility_id);
CREATE INDEX idx_facility_financial_classes_payer_id
    ON facility_financial_classes (payer_id);
GO

CREATE TABLE facility_place_of_service (
    facility_id INT,
    place_of_service VARCHAR(2),
    place_of_service_name VARCHAR(30),
    origin INT
);
GO

ALTER TABLE facility_place_of_service
    ADD CONSTRAINT fk_pos_facility FOREIGN KEY (facility_id)
        REFERENCES facilities(facility_id);

CREATE INDEX idx_facility_place_of_service_facility_id
    ON facility_place_of_service (facility_id);
GO

CREATE TABLE facility_departments (
    department_code VARCHAR(10),
    department_name VARCHAR(50),
    facility_id INT,
    active BIT,
    created_at DATETIME,
    updated_at DATETIME
);
GO

ALTER TABLE facility_departments
    ADD CONSTRAINT fk_dept_facility FOREIGN KEY (facility_id)
        REFERENCES facilities(facility_id);

CREATE INDEX idx_facility_departments_facility_id
    ON facility_departments (facility_id);
GO

CREATE TABLE facility_coders (
    facility_id INT,
    coder_id VARCHAR(50) PRIMARY KEY,
    coder_last_name VARCHAR(50),
    coder_first_name VARCHAR(50)
);
GO

ALTER TABLE facility_coders
    ADD CONSTRAINT fk_coder_facility FOREIGN KEY (facility_id)
        REFERENCES facilities(facility_id);

CREATE INDEX idx_facility_coders_facility_id
    ON facility_coders (facility_id);
GO

CREATE TABLE physicians (
    rendering_provider_id VARCHAR(50) NOT NULL PRIMARY KEY,
    last_name VARCHAR(50),
    first_name VARCHAR(50)
);
GO

CREATE TABLE failed_claims (
    claim_id VARCHAR(50) NOT NULL,
    batch_id VARCHAR(50) NULL,
    facility_id VARCHAR(20) NULL,
    patient_account_number VARCHAR(50) NULL,
    original_data NVARCHAR(MAX) NULL,
    failure_reason NVARCHAR(1000) NOT NULL,
    failure_category VARCHAR(50) NOT NULL,
    processing_stage VARCHAR(50) NOT NULL,
    failed_at DATETIME2(7) NULL,
    repair_suggestions NVARCHAR(MAX) NULL,
    resolution_status VARCHAR(20) NULL,
    assigned_to VARCHAR(100) NULL,
    resolved_at DATETIME2(7) NULL,
    resolution_notes NVARCHAR(2000) NULL,
    resolution_action VARCHAR(50) NULL,
    error_pattern_id VARCHAR(50) NULL,
    priority_level VARCHAR(10) NULL,
    impact_level VARCHAR(10) NULL,
    potential_revenue_loss DECIMAL(12,2) NULL,
    created_at DATETIME2(7) NULL,
    updated_at DATETIME2(7) NULL,
    coder_id VARCHAR(50)
);
GO

ALTER TABLE failed_claims
    ADD CONSTRAINT fk_failed_claims_coder FOREIGN KEY (coder_id)
        REFERENCES facility_coders(coder_id);

CREATE TABLE failed_claims_patterns (
    pattern_id VARCHAR(50) NOT NULL,
    pattern_name VARCHAR(200) NOT NULL,
    pattern_description NVARCHAR(1000) NULL,
    failure_category VARCHAR(50) NULL,
    severity_level VARCHAR(20) NULL,
    frequency_score INT NULL,
    pattern_rules NVARCHAR(MAX) NULL,
    auto_repair_possible BIT NULL,
    repair_template NVARCHAR(MAX) NULL,
    occurrence_count INT NULL,
    resolution_rate DECIMAL(5,4) NULL,
    average_resolution_time_hours DECIMAL(8,2) NULL,
    created_at DATETIME2(7) NULL,
    updated_at DATETIME2(7) NULL
);
GO

CREATE TABLE claims (
    claim_id VARCHAR(50) PRIMARY KEY,
    facility_id VARCHAR(20) NOT NULL,
    department_id INT NULL,
    patient_account_number VARCHAR(50) NOT NULL,
    patient_name VARCHAR(100) NULL,
    first_name VARCHAR(50) NULL,
    last_name VARCHAR(50) NULL,
    medical_record_number VARCHAR(50) NULL,
    date_of_birth DATE NULL,
    gender VARCHAR(1) NULL,
    financial_class_id VARCHAR(10) NULL,
    secondary_insurance VARCHAR(10) NULL,
    service_from_date DATE NULL,
    service_to_date DATE NULL,
    primary_diagnosis VARCHAR(10) NULL,
    financial_class VARCHAR(10) NULL,
    total_charge_amount DECIMAL(10,2) NULL,
    processing_status VARCHAR(20) NULL,
    processing_stage VARCHAR(50) NULL,
    active BIT NULL,
    raw_data NVARCHAR(MAX) NULL,
    validation_results NVARCHAR(MAX) NULL,
    ml_predictions NVARCHAR(MAX) NULL,
    processing_metrics NVARCHAR(MAX) NULL,
    error_details NVARCHAR(MAX) NULL,
    priority VARCHAR(10) NULL,
    submitted_by VARCHAR(100) NULL,
    correlation_id VARCHAR(50) NULL,
    created_at DATETIME2(7) NULL,
    updated_at DATETIME2(7) NULL
);
GO
ALTER TABLE claims
    ADD CONSTRAINT fk_claims_financial_class FOREIGN KEY (financial_class_id)
        REFERENCES facility_financial_classes(financial_class_id);
ALTER TABLE claims
    ADD CONSTRAINT fk_claims_secondary_insurance FOREIGN KEY (secondary_insurance)
        REFERENCES facility_financial_classes(financial_class_id);

CREATE TABLE claims_diagnosis (
    claim_id VARCHAR(50) NOT NULL,
    diagnosis_sequence INT NOT NULL,
    diagnosis_code VARCHAR(20) NOT NULL,
    diagnosis_description VARCHAR(255) NULL,
    diagnosis_type VARCHAR(10) NULL,
    created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET()
);
GO

ALTER TABLE claims_diagnosis
    ADD CONSTRAINT fk_claim_diag_claim FOREIGN KEY (claim_id)
        REFERENCES claims(claim_id);

CREATE TABLE claims_line_items (
    claim_id VARCHAR(50) NOT NULL,
    line_number INT NOT NULL,
    procedure_code VARCHAR(10) NOT NULL,
    modifier1 VARCHAR(2) NULL,
    modifier2 VARCHAR(2) NULL,
    modifier3 VARCHAR(2) NULL,
    modifier4 VARCHAR(2) NULL,
    units INT NOT NULL DEFAULT 1,
    charge_amount DECIMAL(10,2) NOT NULL,
    service_from_date DATE NULL,
    service_to_date DATE NULL,
    diagnosis_pointer VARCHAR(4) NULL,
    place_of_service VARCHAR(2) NULL,
    revenue_code VARCHAR(4) NULL,
    created_at DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    rvu_value DECIMAL(8,4) NULL,
    reimbursement_amount DECIMAL(10,2) NULL,
    rendering_provider_id VARCHAR(50) NULL
);
GO

ALTER TABLE claims_line_items
    ADD CONSTRAINT fk_line_claim FOREIGN KEY (claim_id)
        REFERENCES claims(claim_id);
ALTER TABLE claims_line_items
    ADD CONSTRAINT fk_line_provider FOREIGN KEY (rendering_provider_id)
        REFERENCES physicians(rendering_provider_id);

CREATE TABLE batch_metadata (
    batch_id VARCHAR(50) NOT NULL PRIMARY KEY,
    submitted_by VARCHAR(100) NULL,
    submitted_at DATETIME2(7) DEFAULT SYSDATETIME(),
    updated_at DATETIME2(7) DEFAULT SYSDATETIME(),
    completed_at DATETIME2(7) NULL,
    total_claims INT NOT NULL DEFAULT 0,
    valid_claims INT NOT NULL DEFAULT 0,
    invalid_claims INT NOT NULL DEFAULT 0,
    processed_claims INT NOT NULL DEFAULT 0,
    failed_claims INT NOT NULL DEFAULT 0,
    priority VARCHAR(10) DEFAULT 'normal',
    status VARCHAR(20) DEFAULT 'queued',
    processing_options NVARCHAR(MAX) NULL,
    validation_errors NVARCHAR(MAX) NULL,
    processing_errors NVARCHAR(MAX) NULL,
    additional_data NVARCHAR(MAX) NULL
);
GO

CREATE TABLE processing_metrics (
    metric_id INT IDENTITY(1,1) PRIMARY KEY,
    metric_timestamp DATETIME2(7) DEFAULT SYSDATETIME(),
    batch_id VARCHAR(50) NULL,
    stage_name VARCHAR(50) NOT NULL,
    worker_id VARCHAR(50) NULL,
    records_processed INT NOT NULL DEFAULT 0,
    records_failed INT NOT NULL DEFAULT 0,
    processing_time_seconds DECIMAL(10,3) NOT NULL,
    throughput_per_second DECIMAL(10,2) NOT NULL DEFAULT 0,
    memory_usage_mb INT NULL,
    cpu_usage_percent DECIMAL(5,2) NULL,
    additional_metrics NVARCHAR(MAX) NULL
);
GO

CREATE TABLE performance_metrics (
    metric_date DATETIME2(7) NULL,
    metric_type VARCHAR(50) NOT NULL,
    facility_id VARCHAR(20) NULL,
    claims_per_second DECIMAL(10,4) NULL,
    records_per_minute DECIMAL(10,2) NULL,
    cpu_usage_percent DECIMAL(5,2) NULL,
    memory_usage_mb INT NULL,
    database_response_time_ms DECIMAL(8,2) NULL,
    queue_depth INT NULL,
    error_rate DECIMAL(5,4) NULL,
    processing_accuracy DECIMAL(5,4) NULL,
    revenue_per_claim DECIMAL(10,2) NULL,
    additional_metrics NVARCHAR(MAX) NULL
);
GO
CREATE CLUSTERED COLUMNSTORE INDEX cci_performance_metrics
    ON performance_metrics;
GO

CREATE TABLE rvu_data (
    procedure_code VARCHAR(10) NOT NULL,
    description VARCHAR(500) NULL,
    category VARCHAR(50) NULL,
    subcategory VARCHAR(50) NULL,
    work_rvu DECIMAL(8,4) NULL,
    practice_expense_rvu DECIMAL(8,4) NULL,
    malpractice_rvu DECIMAL(8,4) NULL,
    total_rvu DECIMAL(8,4) NULL,
    conversion_factor DECIMAL(8,2) NULL,
    non_facility_pe_rvu DECIMAL(8,4) NULL,
    facility_pe_rvu DECIMAL(8,4) NULL,
    effective_date DATE NULL,
    end_date DATE NULL,
    status VARCHAR(20) NULL,
    global_period VARCHAR(10) NULL,
    professional_component BIT NULL,
    technical_component BIT NULL,
    bilateral_surgery BIT NULL,
    created_at DATETIME2(7) NULL,
    updated_at DATETIME2(7) NULL
);
GO
-- Partition failed_claims table by failed_at date
CREATE PARTITION FUNCTION pf_failed_at (DATETIME2(7))
AS RANGE RIGHT FOR VALUES ('2020-01-01', '2021-01-01', '2022-01-01', '2023-01-01');

CREATE PARTITION SCHEME ps_failed_at
AS PARTITION pf_failed_at
ALL TO ([PRIMARY]);

CREATE CLUSTERED INDEX cix_failed_claims_failed_at
ON failed_claims(failed_at)
ON ps_failed_at(failed_at);

-- Procedure to archive old failed claims
IF OBJECT_ID('archived_failed_claims', 'U') IS NULL
BEGIN
    CREATE TABLE archived_failed_claims (
        claim_id VARCHAR(50) NOT NULL,
        batch_id VARCHAR(50) NULL,
        facility_id VARCHAR(20) NULL,
        patient_account_number VARCHAR(50) NULL,
        original_data NVARCHAR(MAX) NULL,
        failure_reason NVARCHAR(1000) NOT NULL,
        failure_category VARCHAR(50) NOT NULL,
        processing_stage VARCHAR(50) NOT NULL,
        failed_at DATETIME2(7) NULL,
        repair_suggestions NVARCHAR(MAX) NULL,
        resolution_status VARCHAR(20) NULL,
        assigned_to VARCHAR(100) NULL,
        resolved_at DATETIME2(7) NULL,
        resolution_notes NVARCHAR(2000) NULL,
        resolution_action VARCHAR(50) NULL,
        error_pattern_id VARCHAR(50) NULL,
        priority_level VARCHAR(10) NULL,
        impact_level VARCHAR(10) NULL,
        potential_revenue_loss DECIMAL(12,2) NULL,
        created_at DATETIME2(7) NULL,
        updated_at DATETIME2(7) NULL,
        coder_id VARCHAR(50)
    );
END;
GO

CREATE PROCEDURE sp_archive_failed_claims @cutoff DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO archived_failed_claims SELECT * FROM failed_claims WHERE failed_at < @cutoff;
    DELETE FROM failed_claims WHERE failed_at < @cutoff;
END
GO

GO
CREATE INDEX idx_sql_claims_claim_id ON claims (claim_id);
GO
CREATE INDEX idx_sql_claims_patient_account ON claims (patient_account_number);
GO
CREATE INDEX idx_sql_claims_financial_class_id ON claims (financial_class_id);
GO
CREATE INDEX idx_sql_claims_secondary_insurance ON claims (secondary_insurance);
GO
CREATE INDEX idx_sql_failed_claims_claim_id ON failed_claims (claim_id);
GO
CREATE INDEX idx_sql_failed_claims_failed_at ON failed_claims (failed_at);
GO
CREATE INDEX ix_claims_account_facility
    ON claims (patient_account_number, facility_id)
    INCLUDE (processing_status, processing_stage);
GO
CREATE INDEX ix_active_claims
    ON claims (patient_account_number)
    WHERE active = 1;
GO
CREATE INDEX ix_unresolved_failed_claims
    ON failed_claims (claim_id)
    WHERE (resolution_status IS NULL OR resolution_status <> 'resolved');
GO
ALTER TABLE archived_failed_claims REBUILD PARTITION = ALL
    WITH (DATA_COMPRESSION = PAGE);
GO

CREATE VIEW dbo.v_facility_totals WITH SCHEMABINDING AS
SELECT facility_id,
       SUM(ISNULL(total_charge_amount, 0)) AS total_charge,
       COUNT_BIG(*) AS cnt
FROM dbo.daily_processing_summary
GROUP BY facility_id;
GO
DROP INDEX IF EXISTS cix_v_facility_totals ON dbo.v_facility_totals;
CREATE UNIQUE CLUSTERED INDEX cix_v_facility_totals
    ON dbo.v_facility_totals (facility_id);
GO



-- Analytical views. If materialization is required, create a job to persist the
-- results or use indexed views.

CREATE VIEW dbo.v_daily_claim_totals AS
SELECT
    service_to_date AS service_date,
    COUNT(*) AS claim_count,
    SUM(total_charge_amount) AS total_charge
FROM dbo.claims
GROUP BY service_to_date;
GO

CREATE VIEW dbo.v_failed_claims_summary AS
SELECT
    CAST(failed_at AS DATE) AS fail_day,
    COUNT(*) AS failed_count
FROM dbo.failed_claims
GROUP BY CAST(failed_at AS DATE);
GO

-- SQL Server configuration for high throughput
ALTER DATABASE smart_pro_claims SET RECOVERY SIMPLE;  -- During bulk operations
ALTER DATABASE smart_pro_claims SET AUTO_UPDATE_STATISTICS_ASYNC ON;

-- Bulk insert optimizations
EXEC sp_configure 'max degree of parallelism', 0;
EXEC sp_configure 'cost threshold for parallelism', 5;
RECONFIGURE;