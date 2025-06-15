-- SQL Server Schema Additions for Intelligent Financial Class System
-- Add these tables to your existing create_sqlserver_schema.sql file

-- 1. Add org_id column to facilities table (if not already exists)
IF NOT EXISTS (
    SELECT * FROM sys.columns 
    WHERE object_id = OBJECT_ID('facilities') 
    AND name = 'org_id'
)
BEGIN
    ALTER TABLE facilities ADD org_id INT;
    
    -- Add foreign key constraint
    IF NOT EXISTS (
        SELECT * FROM sys.foreign_keys 
        WHERE name = 'fk_facility_organization'
    )
    BEGIN
        ALTER TABLE facilities
        ADD CONSTRAINT fk_facility_organization 
        FOREIGN KEY (org_id) REFERENCES facility_organization(org_id);
    END
END
GO

-- 2. Financial Class Defaults Table (replaces facility-specific entries)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'financial_class_defaults')
BEGIN
    CREATE TABLE financial_class_defaults (
        financial_class_id VARCHAR(10) PRIMARY KEY,
        financial_class_name VARCHAR(100) NOT NULL,
        payer_id INT NOT NULL,
        base_dollar_amount DECIMAL(10,2) NOT NULL,
        base_percentage_rate DECIMAL(5,4) NOT NULL,
        processing_priority VARCHAR(10) DEFAULT 'NORMAL',
        auto_posting_enabled BIT DEFAULT 1,
        active BIT DEFAULT 1,
        effective_date DATE DEFAULT GETDATE(),
        created_at DATETIME DEFAULT GETDATE(),
        CONSTRAINT fk_fcd_payer FOREIGN KEY (payer_id) REFERENCES core_standard_payers(payer_id)
    );
    
    CREATE INDEX idx_financial_class_defaults_payer_id ON financial_class_defaults(payer_id);
    CREATE INDEX idx_financial_class_defaults_active ON financial_class_defaults(active);
END
GO

-- 3. Organization Rate Tiers Table (5 entries instead of 50+ facilities)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'organization_rate_tiers')
BEGIN
    CREATE TABLE organization_rate_tiers (
        org_id INT PRIMARY KEY,
        tier_name VARCHAR(50) NOT NULL,
        tier_level INT NOT NULL,
        dollar_amount_multiplier DECIMAL(5,4) NOT NULL,
        percentage_rate_multiplier DECIMAL(5,4) NOT NULL,
        negotiation_power_score INT NOT NULL,
        effective_date DATE DEFAULT GETDATE(),
        created_at DATETIME DEFAULT GETDATE(),
        CONSTRAINT fk_ort_organization FOREIGN KEY (org_id) REFERENCES facility_organization(org_id)
    );
    
    CREATE INDEX idx_organization_rate_tiers_tier_level ON organization_rate_tiers(tier_level);
    CREATE INDEX idx_organization_rate_tiers_negotiation_power ON organization_rate_tiers(negotiation_power_score);
END
GO

-- 4. Geographic Rate Zones Table (3-5 entries for all states)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'geographic_rate_zones')
BEGIN
    CREATE TABLE geographic_rate_zones (
        zone_id INT PRIMARY KEY,
        zone_name VARCHAR(50) NOT NULL,
        state_codes VARCHAR(100) NOT NULL,
        cost_of_living_index DECIMAL(5,2) NOT NULL,
        dollar_amount_multiplier DECIMAL(5,4) NOT NULL,
        percentage_rate_multiplier DECIMAL(5,4) NOT NULL,
        market_competitiveness VARCHAR(20) NOT NULL,
        effective_date DATE DEFAULT GETDATE(),
        created_at DATETIME DEFAULT GETDATE()
    );
    
    CREATE INDEX idx_geographic_rate_zones_states ON geographic_rate_zones(state_codes);
    CREATE INDEX idx_geographic_rate_zones_competitiveness ON geographic_rate_zones(market_competitiveness);
END
GO

-- 5. Facility Rate Overrides Table (only for special cases)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_rate_overrides')
BEGIN
    CREATE TABLE facility_rate_overrides (
        facility_id VARCHAR(20) NOT NULL,
        financial_class_id VARCHAR(10) NOT NULL,
        override_dollar_amount DECIMAL(10,2) NOT NULL,
        override_percentage_rate DECIMAL(5,4) NOT NULL,
        override_reason VARCHAR(200) NOT NULL,
        effective_date DATE DEFAULT GETDATE(),
        end_date DATE NULL,
        created_at DATETIME DEFAULT GETDATE(),
        PRIMARY KEY (facility_id, financial_class_id),
        CONSTRAINT fk_fro_facility FOREIGN KEY (facility_id) REFERENCES facilities(facility_id),
        CONSTRAINT fk_fro_financial_class FOREIGN KEY (financial_class_id) REFERENCES financial_class_defaults(financial_class_id)
    );
    
    CREATE INDEX idx_facility_rate_overrides_facility ON facility_rate_overrides(facility_id);
    CREATE INDEX idx_facility_rate_overrides_financial_class ON facility_rate_overrides(financial_class_id);
    CREATE INDEX idx_facility_rate_overrides_effective_date ON facility_rate_overrides(effective_date);
END
GO

-- 6. Dynamic Rate Calculation Function
CREATE OR ALTER FUNCTION dbo.GetEffectiveFinancialRate(
    @facility_id VARCHAR(20),
    @financial_class_id VARCHAR(10)
)
RETURNS TABLE
AS
RETURN
(
    SELECT 
        f.facility_id,
        f.facility_name,
        fo.org_id,
        fo.org_name,
        fcd.financial_class_id,
        fcd.financial_class_name,
        
        -- Check for facility-specific override first
        CASE 
            WHEN fro.facility_id IS NOT NULL THEN fro.override_dollar_amount
            ELSE 
                -- Calculate from base + org tier + geographic zone
                fcd.base_dollar_amount * 
                ISNULL(ort.dollar_amount_multiplier, 1.0) *
                ISNULL(grz.dollar_amount_multiplier, 1.0)
        END AS effective_dollar_amount,
        
        CASE 
            WHEN fro.facility_id IS NOT NULL THEN fro.override_percentage_rate
            ELSE 
                -- Calculate from base + org tier + geographic zone  
                fcd.base_percentage_rate * 
                ISNULL(ort.percentage_rate_multiplier, 1.0) *
                ISNULL(grz.percentage_rate_multiplier, 1.0)
        END AS effective_percentage_rate,
        
        -- Rate calculation source
        CASE 
            WHEN fro.facility_id IS NOT NULL THEN 'OVERRIDE: ' + fro.override_reason
            ELSE 'CALCULATED: Org ' + ort.tier_name + ' + Zone ' + grz.zone_name
        END AS rate_source,
        
        fcd.base_dollar_amount,
        fcd.base_percentage_rate,
        ort.tier_name,
        ort.dollar_amount_multiplier AS org_amount_multiplier,
        ort.percentage_rate_multiplier AS org_rate_multiplier,
        grz.zone_name,
        grz.dollar_amount_multiplier AS geo_amount_multiplier,
        grz.percentage_rate_multiplier AS geo_rate_multiplier
        
    FROM facilities f
    INNER JOIN facility_organization fo ON f.org_id = fo.org_id
    INNER JOIN financial_class_defaults fcd ON fcd.financial_class_id = @financial_class_id
    LEFT JOIN organization_rate_tiers ort ON f.org_id = ort.org_id
    LEFT JOIN geographic_rate_zones grz ON grz.state_codes LIKE '%' + f.state + '%'
    LEFT JOIN facility_rate_overrides fro ON f.facility_id = fro.facility_id 
                                           AND fcd.financial_class_id = fro.financial_class_id
    WHERE f.facility_id = @facility_id
      AND f.active = 1
      AND fcd.active = 1
)
GO

-- 7. Update Claims Table Foreign Key (if needed)
-- The claims table currently references facility_financial_classes, but now should reference financial_class_defaults
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_claims_financial_class')
BEGIN
    ALTER TABLE claims DROP CONSTRAINT fk_claims_financial_class;
END

IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_claims_financial_class_defaults')
BEGIN
    ALTER TABLE claims
    ADD CONSTRAINT fk_claims_financial_class_defaults 
    FOREIGN KEY (financial_class_id) REFERENCES financial_class_defaults(financial_class_id);
END
GO

-- 8. Add indexes for performance
CREATE INDEX idx_facilities_org_id ON facilities(org_id);
CREATE INDEX idx_facilities_state ON facilities(state);
CREATE INDEX idx_facilities_active ON facilities(active);
GO

-- 9. Add helpful views for common queries
CREATE OR ALTER VIEW dbo.v_facility_financial_rates AS
SELECT 
    f.facility_id,
    f.facility_name,
    f.facility_type,
    fo.org_name,
    ort.tier_name,
    fcd.financial_class_id,
    fcd.financial_class_name,
    fcd.base_dollar_amount,
    fcd.base_percentage_rate,
    
    -- Calculated effective rates
    CASE 
        WHEN fro.facility_id IS NOT NULL THEN fro.override_dollar_amount
        ELSE 
            fcd.base_dollar_amount * 
            ISNULL(ort.dollar_amount_multiplier, 1.0) *
            ISNULL(grz.dollar_amount_multiplier, 1.0)
    END AS effective_dollar_amount,
    
    CASE 
        WHEN fro.facility_id IS NOT NULL THEN fro.override_percentage_rate
        ELSE 
            fcd.base_percentage_rate * 
            ISNULL(ort.percentage_rate_multiplier, 1.0) *
            ISNULL(grz.percentage_rate_multiplier, 1.0)
    END AS effective_percentage_rate,
    
    CASE 
        WHEN fro.facility_id IS NOT NULL THEN 'Override'
        ELSE 'Calculated'
    END AS rate_type
    
FROM facilities f
INNER JOIN facility_organization fo ON f.org_id = fo.org_id
CROSS JOIN financial_class_defaults fcd
LEFT JOIN organization_rate_tiers ort ON f.org_id = ort.org_id
LEFT JOIN geographic_rate_zones grz ON grz.state_codes LIKE '%' + f.state + '%'
LEFT JOIN facility_rate_overrides fro ON f.facility_id = fro.facility_id 
                                       AND fcd.financial_class_id = fro.financial_class_id
WHERE f.active = 1 AND fcd.active = 1;
GO

-- 10. Comments for documentation
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Base financial class definitions with default dollar amounts and percentage rates',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'financial_class_defaults';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Organization-level rate multipliers based on negotiation power and size',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'organization_rate_tiers';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Geographic rate adjustments based on cost of living and market conditions',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'geographic_rate_zones';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Facility-specific rate overrides for special contracts (minimize usage)',
    @level0type = N'SCHEMA', @level0name = N'dbo',
    @level1type = N'TABLE', @level1name = N'facility_rate_overrides';
GO

PRINT 'SUCCESS: Added intelligent financial class tables and function to SQL Server schema';
PRINT 'BENEFIT: Reduced entries from 400+ to ~30 while maintaining full rate flexibility';
PRINT 'USAGE: Use GetEffectiveFinancialRate function or v_facility_financial_rates view for rate lookups';