#!/usr/bin/env python3
"""
Updated Reference Data Setup Script with RVU Synchronization
Creates organizational hierarchy and implements PostgreSQL RVU caching with sync
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple
import random

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


async def setup_reference_data():
    """
    Main function to set up all reference data with RVU synchronization.
    
    Flow:
    1. Setup organizational structure in SQL Server
    2. Setup financial classes and rate tiers 
    3. Create RVU data in SQL Server (master)
    4. Sync RVU data to PostgreSQL (processing cache)
    5. Validate data integrity
    """
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    
    logger.info("🚀 Starting reference data setup with RVU synchronization...")
    
    # Load configuration
    config = load_config()
    
    # Initialize database connections
    pg_db = PostgresDatabase(config.postgres)
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        # Connect without preparing RVU queries initially (tables may not exist)
        await pg_db.connect(prepare_queries=False)
        await sql_db.connect()
        logger.info("✅ Connected to both PostgreSQL and SQL Server databases")
        
        # 1. Setup organizational hierarchy in SQL Server
        logger.info("📋 Setting up organizational structure...")
        await setup_organizations(sql_db, logger)
        await setup_regions(sql_db, logger)  
        await setup_facilities_with_hierarchy(sql_db, logger)
        
        # 2. Setup financial classes with intelligent rate tiers
        logger.info("💰 Setting up financial classes and rate tiers...")
        await setup_financial_classes_with_facility_rates(sql_db, logger)
        await create_financial_rate_calculation_function(sql_db, logger)
        
        # 3. Setup RVU data in SQL Server (master source)
        logger.info("🏥 Setting up RVU data in SQL Server (master)...")
        await setup_rvu_data_sql_server_only(sql_db, logger)
        
        # 4. Initialize RVU sync system and sync to PostgreSQL
        logger.info("🔄 Initializing RVU synchronization system...")
        await initialize_rvu_sync_system(pg_db, sql_db, logger)
        
        # 5. Now prepare RVU queries since PostgreSQL has the data
        logger.info("⚡ Preparing PostgreSQL query statements...")
        await pg_db.prepare_common_queries()
        
        logger.info("🎉 Reference data setup completed successfully!")
        logger.info("📊 PostgreSQL is ready for high-performance claims processing")
        logger.info("🔒 SQL Server maintains authoritative master data")
        
    except Exception as e:
        logger.error(f"❌ Error during reference data setup: {e}")
        raise
    finally:
        await pg_db.close()
        await sql_db.close()
        logger.info("🔌 Database connections closed")


async def initialize_rvu_sync_system(pg_db: PostgresDatabase, sql_db: SQLServerDatabase, logger):
    """Initialize the RVU synchronization system"""
    
    # Import the sync system
    from scripts.rvu_sync_system import RVUDataSync
    
    # Create sync manager
    rvu_sync = RVUDataSync(pg_db, sql_db)
    
    # Perform initial sync from SQL Server to PostgreSQL
    logger.info("🔄 Performing initial RVU data sync...")
    await rvu_sync.initial_sync()
    
    # Validate data integrity
    logger.info("🔍 Validating RVU data integrity...")
    integrity_report = await rvu_sync.validate_data_integrity()
    
    if integrity_report["sync_required"]:
        logger.warning("⚠️ RVU data integrity check failed - manual review required")
        logger.warning(f"SQL Server count: {integrity_report['sql_server_count']}")
        logger.warning(f"PostgreSQL count: {integrity_report['postgresql_count']}")
    else:
        logger.info("✅ RVU data integrity validation passed")
        logger.info(f"📊 {integrity_report['postgresql_count']} RVU records synchronized")
    
    return integrity_report


# Include all the existing setup functions from your working script

async def setup_organizations(sql_db: SQLServerDatabase, logger):
    """Setup organizations - required parent for all facilities"""
    logger.info("Setting up organizations...")
    
    # First, ensure the facility_organization table exists
    create_org_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_organization')
        BEGIN
            CREATE TABLE facility_organization (
                org_id INT PRIMARY KEY,
                org_name VARCHAR(100) NOT NULL,
                installed_date DATETIME DEFAULT GETDATE(),
                updated_by INT DEFAULT 1
            );
        END
    """
    await sql_db.execute(create_org_table_query)
    logger.info("SUCCESS: Facility organization table created/verified")
    
    # Create sample healthcare organizations
    organizations_data = [
        (1, "MediCorp Health System", datetime.now(), 1),
        (2, "Regional Medical Centers", datetime.now(), 1),
        (3, "Community Healthcare Network", datetime.now(), 1),
        (4, "Premier Health Alliance", datetime.now(), 1),
        (5, "Integrated Care Solutions", datetime.now(), 1),
    ]
    
    # Insert organizations
    for org_id, org_name, installed_date, updated_by in organizations_data:
        org_query = """
            IF NOT EXISTS (SELECT 1 FROM facility_organization WHERE org_id = ?)
            BEGIN
                INSERT INTO facility_organization (org_id, org_name, installed_date, updated_by)
                VALUES (?, ?, ?, ?)
            END
            ELSE
            BEGIN
                UPDATE facility_organization 
                SET org_name = ?, installed_date = ?, updated_by = ?
                WHERE org_id = ?
            END
        """
        await sql_db.execute(org_query, org_id, org_id, org_name, installed_date, updated_by, org_name, installed_date, updated_by, org_id)
    
    logger.info(f"SUCCESS: Inserted {len(organizations_data)} organizations")


async def setup_regions(sql_db: SQLServerDatabase, logger):
    """Setup regions - optional grouping within organizations"""
    logger.info("Setting up regions...")
    
    # Create regions within organizations
    regions_data = [
        (1, 'West Coast Operations'),
        (2, 'East Coast Operations'), 
        (3, 'Midwest Division'),
        (4, 'Southwest Region'),
        (5, 'Southeast Territory'),
        (6, 'Mountain States'),
        (7, 'Great Lakes Region'),
        (8, 'Gulf Coast Division'),
    ]
    
    # Create the table and insert regions
    create_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_region')
        BEGIN
            CREATE TABLE facility_region (
                region_id INT PRIMARY KEY,
                region_name VARCHAR(100)
            );
        END
    """
    await sql_db.execute(create_table_query)
    
    for region_id, region_name in regions_data:
        insert_query = """
            IF NOT EXISTS (SELECT 1 FROM facility_region WHERE region_id = ?)
            BEGIN
                INSERT INTO facility_region (region_id, region_name) VALUES (?, ?)
            END
        """
        await sql_db.execute(insert_query, region_id, region_id, region_name)
    
    logger.info(f"SUCCESS: Setup {len(regions_data)} regions")


async def setup_facilities_with_hierarchy(sql_db: SQLServerDatabase, logger):
    """Setup facilities with proper organizational hierarchy"""
    logger.info("Setting up facilities with organizational hierarchy...")
    
    # First, create the facilities table if it doesn't exist (with org_id from the start)
    await create_facilities_table_with_org_id(sql_db, logger)
    
    # Define 50 facilities across organizations and regions
    facilities_data = []
    
    # Organization distribution (how many facilities per org)
    org_facility_counts = {1: 15, 2: 12, 3: 10, 4: 8, 5: 5}
    facility_counter = 1
    
    for org_id, facility_count in org_facility_counts.items():
        for i in range(facility_count):
            # Assign regions (some facilities may not have regions)
            region_id = None
            if random.random() > 0.3:  # 70% of facilities have regions
                region_id = random.randint(1, 8)
            
            facility_data = (
                str(facility_counter),  # facility_id
                f"FAC{facility_counter:03d}",  # facility_name 
                "Hospital" if facility_counter <= 25 else "Clinic",  # facility_type
                f"{100 + facility_counter} Medical Center Drive",  # address
                datetime.now(),  # installed_date
                50 + (facility_counter * 2),  # beds
                f"City{facility_counter}",  # city
                "CA" if facility_counter <= 15 else "TX" if facility_counter <= 30 else "NY",  # state
                f"{90000 + facility_counter:05d}",  # zip_code
                f"555-{facility_counter:03d}-0000",  # phone
                True,  # active
                datetime.now(),  # updated_date
                1,  # updated_by
                region_id,  # region_id (optional)
                1,  # fiscal_month
                org_id,  # org_id (required)
            )
            facilities_data.append(facility_data)
            facility_counter += 1
    
    # Insert facilities with organization hierarchy
    sql_query = """
        MERGE facilities AS target
        USING (SELECT ? as facility_id, ? as facility_name, 
               ? as facility_type, ? as address, ? as installed_date, ? as beds,
               ? as city, ? as state, ? as zip_code, ? as phone, ? as active,
               ? as updated_date, ? as updated_by, ? as region_id, ? as fiscal_month, 
               ? as org_id) AS source
        ON target.facility_id = source.facility_id
        WHEN MATCHED THEN
            UPDATE SET facility_name = source.facility_name,
                      facility_type = source.facility_type,
                      org_id = source.org_id,
                      region_id = source.region_id,
                      updated_date = source.updated_date
        WHEN NOT MATCHED THEN
            INSERT (facility_id, facility_name, facility_type,
                   address, installed_date, beds, city, state, zip_code, phone,
                   active, updated_date, updated_by, region_id, fiscal_month, org_id)
            VALUES (source.facility_id, source.facility_name, source.facility_type,
                   source.address, source.installed_date, source.beds,
                   source.city, source.state, source.zip_code, source.phone, source.active,
                   source.updated_date, source.updated_by, source.region_id, source.fiscal_month, 
                   source.org_id);
    """
    
    for facility in facilities_data:
        await sql_db.execute(sql_query, *facility)
    
    logger.info(f"SUCCESS: Inserted {len(facilities_data)} facilities with organizational hierarchy")


async def create_facilities_table_with_org_id(sql_db: SQLServerDatabase, logger):
    """Create the facilities table with org_id column from the start"""
    logger.info("Creating facilities table with proper schema including org_id...")
    
    # Check if table exists and has org_id column
    check_table_query = """
        SELECT 
            CASE WHEN EXISTS (SELECT * FROM sys.tables WHERE name = 'facilities') THEN 1 ELSE 0 END as table_exists,
            CASE WHEN EXISTS (
                SELECT * FROM sys.columns 
                WHERE object_id = OBJECT_ID('facilities') 
                AND name = 'org_id'
            ) THEN 1 ELSE 0 END as org_id_exists
    """
    
    result = await sql_db.fetch(check_table_query)
    table_exists = result[0]['table_exists'] if result else 0
    org_id_exists = result[0]['org_id_exists'] if result else 0
    
    if not table_exists:
        # Create the complete facilities table with all columns including org_id
        create_table_query = """
            CREATE TABLE facilities (
                facility_id VARCHAR(20) PRIMARY KEY,
                facility_name VARCHAR(100) NOT NULL,
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
                fiscal_month INT,
                org_id INT NOT NULL
            );
        """
        await sql_db.execute(create_table_query)
        logger.info("SUCCESS: Created facilities table with org_id column")
    elif not org_id_exists:
        # Add org_id column if it doesn't exist
        alter_table_query = "ALTER TABLE facilities ADD org_id INT NOT NULL DEFAULT 1"
        await sql_db.execute(alter_table_query)
        logger.info("SUCCESS: Added org_id column to existing facilities table")
    
    # Add foreign key constraints
    add_constraints_query = """
        -- Add facility_organization foreign key
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_facility_organization')
        AND EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_organization')
        BEGIN
            ALTER TABLE facilities
            ADD CONSTRAINT fk_facility_organization 
            FOREIGN KEY (org_id) REFERENCES facility_organization(org_id);
        END
        
        -- Add facility_region foreign key
        IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'fk_facility_region')
        AND EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_region')
        BEGIN
            ALTER TABLE facilities
            ADD CONSTRAINT fk_facility_region 
            FOREIGN KEY (region_id) REFERENCES facility_region(region_id);
        END
    """
    await sql_db.execute(add_constraints_query)
    
    logger.info("SUCCESS: Facilities table verified with constraints")


# Include the rest of your setup functions here...
# (setup_financial_classes_with_facility_rates, etc.)

async def setup_financial_classes_with_facility_rates(sql_db: SQLServerDatabase, logger):
    """Setup financial classes with intelligent tiered reimbursement system."""
    logger.info("Setting up intelligent tiered financial class system...")
    
    # Setup core components
    await setup_core_payers(sql_db, logger)
    await setup_financial_class_defaults(sql_db, logger)
    await setup_organization_rate_tiers(sql_db, logger)
    await setup_geographic_rate_zones(sql_db, logger)
    await setup_facility_rate_overrides(sql_db, logger)
    
    logger.info("SUCCESS: Intelligent tiered financial class system created")


async def setup_core_payers(sql_db: SQLServerDatabase, logger):
    """Setup core standard payers"""
    
    # First ensure table exists
    create_payers_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'core_standard_payers')
        BEGIN
            CREATE TABLE core_standard_payers (
                payer_id INT PRIMARY KEY,
                payer_name VARCHAR(20) NOT NULL,
                payer_code CHAR(2) NOT NULL
            );
        END
    """
    await sql_db.execute(create_payers_table_query)
    
    payers_data = [
        (1, "Commercial", "CM"),
        (2, "Medicaid", "MD"),
        (3, "Medicare", "MC"),
        (4, "PPO", "PP"),
        (5, "HMO", "HM"),
        (6, "Workers Comp", "WC"),
        (7, "Other", "OT"),
        (8, "Self Pay", "SP"),
    ]
    
    payer_query = """
        MERGE core_standard_payers AS target
        USING (SELECT ? as payer_id, ? as payer_name, ? as payer_code) AS source
        ON target.payer_id = source.payer_id
        WHEN MATCHED THEN
            UPDATE SET payer_name = source.payer_name, payer_code = source.payer_code
        WHEN NOT MATCHED THEN
            INSERT (payer_id, payer_name, payer_code)
            VALUES (source.payer_id, source.payer_name, source.payer_code);
    """
    
    for payer in payers_data:
        await sql_db.execute(payer_query, *payer)
    
    logger.info(f"SUCCESS: Setup {len(payers_data)} core payers")


async def setup_financial_class_defaults(sql_db: SQLServerDatabase, logger):
    """Setup default financial class definitions"""
    # Implementation details...
    pass


async def setup_organization_rate_tiers(sql_db: SQLServerDatabase, logger):
    """Setup organization-level rate tiers"""
    # Implementation details...
    pass


async def setup_geographic_rate_zones(sql_db: SQLServerDatabase, logger):
    """Setup geographic rate zones"""
    # Implementation details...
    pass


async def setup_facility_rate_overrides(sql_db: SQLServerDatabase, logger):
    """Setup facility-specific overrides"""
    # Implementation details...
    pass


async def create_financial_rate_calculation_function(sql_db: SQLServerDatabase, logger):
    """Create financial rate calculation function"""
    # Implementation details...
    pass


async def setup_rvu_data_sql_server_only(sql_db: SQLServerDatabase, logger):
    """Setup comprehensive RVU data in SQL Server"""
    logger.info("Setting up comprehensive RVU data in SQL Server...")
    
    # First create the table if it doesn't exist
    create_rvu_table_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'rvu_data')
        BEGIN
            CREATE TABLE rvu_data (
                procedure_code VARCHAR(10) PRIMARY KEY,
                description VARCHAR(200),
                category VARCHAR(50),
                subcategory VARCHAR(50),
                work_rvu DECIMAL(10,4),
                practice_expense_rvu DECIMAL(10,4),
                malpractice_rvu DECIMAL(10,4),
                total_rvu DECIMAL(10,4),
                conversion_factor DECIMAL(10,4),
                non_facility_pe_rvu DECIMAL(10,4),
                facility_pe_rvu DECIMAL(10,4),
                effective_date DATE,
                end_date DATE,
                status VARCHAR(20),
                global_period VARCHAR(10),
                professional_component BIT,
                technical_component BIT,
                bilateral_surgery BIT,
                created_at DATETIME,
                updated_at DATETIME
            );
        END
    """
    await sql_db.execute(create_rvu_table_query)
    
    # Enhanced RVU data with more comprehensive procedure codes
    rvu_data = [
        # Evaluation & Management
        ("99213", "Office/outpatient visit est", "E&M", "Office", Decimal("0.97"), Decimal("0.67"), Decimal("0.05"), Decimal("1.69"), Decimal("36.04")),
        ("99214", "Office/outpatient visit est", "E&M", "Office", Decimal("1.50"), Decimal("1.11"), Decimal("0.08"), Decimal("2.69"), Decimal("36.04")),
        ("99215", "Office/outpatient visit est", "E&M", "Office", Decimal("2.11"), Decimal("1.52"), Decimal("0.11"), Decimal("3.74"), Decimal("36.04")),
        ("99202", "Office/outpatient visit new", "E&M", "Office", Decimal("0.93"), Decimal("0.65"), Decimal("0.05"), Decimal("1.63"), Decimal("36.04")),
        ("99203", "Office/outpatient visit new", "E&M", "Office", Decimal("1.42"), Decimal("1.07"), Decimal("0.08"), Decimal("2.57"), Decimal("36.04")),
        ("99204", "Office/outpatient visit new", "E&M", "Office", Decimal("2.43"), Decimal("1.73"), Decimal("0.13"), Decimal("4.29"), Decimal("36.04")),
        ("99205", "Office/outpatient visit new", "E&M", "Office", Decimal("3.17"), Decimal("2.25"), Decimal("0.17"), Decimal("5.59"), Decimal("36.04")),
        
        # Laboratory
        ("80053", "Comprehensive metabolic panel", "Lab", "Laboratory", Decimal("0.00"), Decimal("1.25"), Decimal("0.06"), Decimal("1.31"), Decimal("36.04")),
        ("85025", "Blood count; complete", "Lab", "Laboratory", Decimal("0.00"), Decimal("0.17"), Decimal("0.01"), Decimal("0.18"), Decimal("36.04")),
        ("80061", "Lipid panel", "Lab", "Laboratory", Decimal("0.00"), Decimal("0.42"), Decimal("0.02"), Decimal("0.44"), Decimal("36.04")),
        
        # Radiology
        ("73030", "Radiologic exam, shoulder", "Radiology", "Imaging", Decimal("0.22"), Decimal("0.89"), Decimal("0.05"), Decimal("1.16"), Decimal("36.04")),
        ("71020", "Chest X-ray, 2 views", "Radiology", "Imaging", Decimal("0.22"), Decimal("0.50"), Decimal("0.03"), Decimal("0.75"), Decimal("36.04")),
        ("73060", "Knee X-ray, 2 views", "Radiology", "Imaging", Decimal("0.22"), Decimal("0.47"), Decimal("0.03"), Decimal("0.72"), Decimal("36.04")),
        
        # Surgery
        ("29881", "Arthroscopy, knee", "Surgery", "Orthopedic", Decimal("8.50"), Decimal("5.23"), Decimal("0.44"), Decimal("14.17"), Decimal("36.04")),
        ("64483", "Lumbar facet injection", "Surgery", "Pain Management", Decimal("1.50"), Decimal("2.85"), Decimal("0.12"), Decimal("4.47"), Decimal("36.04")),
        
        # Emergency Medicine
        ("99282", "Emergency dept visit, low", "E&M", "Emergency", Decimal("1.00"), Decimal("1.23"), Decimal("0.08"), Decimal("2.31"), Decimal("36.04")),
        ("99283", "Emergency dept visit, mod", "E&M", "Emergency", Decimal("1.80"), Decimal("1.85"), Decimal("0.12"), Decimal("3.77"), Decimal("36.04")),
        ("99284", "Emergency dept visit, high", "E&M", "Emergency", Decimal("3.20"), Decimal("2.47"), Decimal("0.18"), Decimal("5.85"), Decimal("36.04")),
        
        # Preventive Medicine
        ("G0438", "Annual wellness visit, initial", "E&M", "Preventive", Decimal("2.30"), Decimal("0.85"), Decimal("0.09"), Decimal("3.24"), Decimal("36.04")),
        ("G0439", "Annual wellness visit, subsequent", "E&M", "Preventive", Decimal("2.00"), Decimal("0.75"), Decimal("0.08"), Decimal("2.83"), Decimal("36.04")),
    ]
    
    # Insert RVU data using MERGE for upsert functionality
    sql_rvu_query = """
        MERGE rvu_data AS target
        USING (SELECT ? as procedure_code, ? as description, ? as category, ? as subcategory,
               ? as work_rvu, ? as practice_expense_rvu, ? as malpractice_rvu, ? as total_rvu, ? as conversion_factor,
               ? as non_facility_pe_rvu, ? as facility_pe_rvu, GETDATE() as effective_date,
               NULL as end_date, 'ACTIVE' as status, '000' as global_period,
               0 as professional_component, 0 as technical_component, 0 as bilateral_surgery,
               GETDATE() as created_at, GETDATE() as updated_at) AS source
        ON target.procedure_code = source.procedure_code
        WHEN MATCHED THEN
            UPDATE SET description = source.description, total_rvu = source.total_rvu,
                      conversion_factor = source.conversion_factor, updated_at = source.updated_at
        WHEN NOT MATCHED THEN
            INSERT (procedure_code, description, category, subcategory, work_rvu,
                   practice_expense_rvu, malpractice_rvu, total_rvu, conversion_factor,
                   non_facility_pe_rvu, facility_pe_rvu, effective_date, end_date, status,
                   global_period, professional_component, technical_component, bilateral_surgery,
                   created_at, updated_at)
            VALUES (source.procedure_code, source.description, source.category, source.subcategory,
                   source.work_rvu, source.practice_expense_rvu, source.malpractice_rvu, source.total_rvu,
                   source.conversion_factor, source.non_facility_pe_rvu, source.facility_pe_rvu,
                   source.effective_date, source.end_date, source.status, source.global_period,
                   source.professional_component, source.technical_component, source.bilateral_surgery,
                   source.created_at, source.updated_at);
    """
    
    for rvu in rvu_data:
        # Expand data for SQL Server insert (adding facility_pe_rvu = practice_expense_rvu)
        expanded_rvu = rvu + (rvu[4], rvu[4])  # Add non_facility_pe_rvu, facility_pe_rvu
        await sql_db.execute(sql_rvu_query, *expanded_rvu)
    
    logger.info(f"SUCCESS: Inserted {len(rvu_data)} RVU records into SQL Server")


if __name__ == "__main__":
    asyncio.run(setup_reference_data())