#!/usr/bin/env python3
"""
Updated Reference Data Setup Script
Creates and populates reference data tables required for claims validation.
NOW WITH CLEANER ARCHITECTURE:
- Facilities: SQL Server ONLY (single source of truth)
- Financial classes: Both databases (PostgreSQL for fast validation cache)
- RVU data: SQL Server primary, PostgreSQL cache
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


async def setup_reference_data():
    """
    Main function to set up all reference data required for claims processing.
    Creates and populates:
    - Facilities (SQL Server ONLY - single source of truth)
    - Financial classes (both databases - PostgreSQL for validation caching)
    - RVU data (SQL Server primary, PostgreSQL cache)
    """
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting reference data setup with cleaner architecture...")
    logger.info("Facilities: SQL Server ONLY")
    logger.info("Financial Classes: Both databases (PostgreSQL cache)")
    logger.info("RVU Data: SQL Server primary, PostgreSQL cache")
    
    # Load configuration
    config = load_config()
    
    # Initialize database connections
    pg_db = PostgresDatabase(config.postgres)
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        # Connect to databases
        await pg_db.connect()
        await sql_db.connect()
        logger.info("Connected to both PostgreSQL and SQL Server databases")
        
        # Setup facilities in SQL Server ONLY
        await setup_facilities_sql_server_only(sql_db, logger)
        
        # Setup financial classes in both databases (PostgreSQL for validation cache)
        await setup_financial_classes(pg_db, sql_db, logger)
        
        # Setup RVU data in SQL Server and cache in PostgreSQL
        await setup_rvu_data(sql_db, pg_db, logger)
        
        logger.info("Reference data setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during reference data setup: {e}")
        raise
    finally:
        await pg_db.close()
        await sql_db.close()
        logger.info("Database connections closed")


async def setup_facilities_sql_server_only(sql_db: SQLServerDatabase, logger):
    """Setup facilities in SQL Server ONLY - single source of truth"""
    logger.info("Setting up facilities in SQL Server ONLY...")
    
    # First, ensure required prerequisite tables exist in SQL Server
    await setup_prerequisite_tables(sql_db, logger)
    
    # Define facility data - 50 facilities as referenced in the sample data
    facilities_data = []
    for i in range(1, 51):
        facility_data = (
            str(i),  # facility_id as VARCHAR(20)
            f"FAC{i:03d}",  # facility_name (FAC001, FAC002, etc.)
            f"Test Facility {i}",  # facility_description
            "Hospital" if i <= 25 else "Clinic",  # facility_type
            f"{100 + i} Medical Center Drive",  # address
            datetime.now(),  # installed_date
            50 + (i * 2),  # beds
            f"City{i}",  # city
            "CA" if i <= 25 else "NY",  # state
            f"{90000 + i:05d}",  # zip_code
            f"555-{i:03d}-0000",  # phone
            True,  # active
            datetime.now(),  # updated_date
            1,  # updated_by
            (i - 1) % 5 + 1,  # region_id (1-5)
            1,  # fiscal_month
        )
        facilities_data.append(facility_data)
    
    # Insert into SQL Server ONLY
    sql_query = """
        MERGE facilities AS target
        USING (SELECT ? as facility_id, ? as facility_name, ? as facility_description, 
               ? as facility_type, ? as address, ? as installed_date, ? as beds,
               ? as city, ? as state, ? as zip_code, ? as phone, ? as active,
               ? as updated_date, ? as updated_by, ? as region_id, ? as fiscal_month) AS source
        ON target.facility_id = source.facility_id
        WHEN MATCHED THEN
            UPDATE SET facility_name = source.facility_name,
                      facility_description = source.facility_description,
                      updated_date = source.updated_date
        WHEN NOT MATCHED THEN
            INSERT (facility_id, facility_name, facility_description, facility_type,
                   address, installed_date, beds, city, state, zip_code, phone,
                   active, updated_date, updated_by, region_id, fiscal_month)
            VALUES (source.facility_id, source.facility_name, source.facility_description,
                   source.facility_type, source.address, source.installed_date, source.beds,
                   source.city, source.state, source.zip_code, source.phone, source.active,
                   source.updated_date, source.updated_by, source.region_id, source.fiscal_month);
    """
    
    for facility in facilities_data:
        await sql_db.execute(sql_query, facility)
    
    logger.info(f"✅ Inserted {len(facilities_data)} facilities into SQL Server")
    logger.info("✅ Facilities are now managed ONLY in SQL Server (single source of truth)")


async def setup_prerequisite_tables(sql_db: SQLServerDatabase, logger):
    """Ensure prerequisite tables exist in SQL Server"""
    logger.info("Setting up prerequisite tables in SQL Server...")
    
    # Create facility_region table if it doesn't exist
    region_query = """
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'facility_region')
        BEGIN
            CREATE TABLE facility_region (
                region_id INT PRIMARY KEY,
                region_name VARCHAR(100)
            );
            
            -- Insert default regions
            INSERT INTO facility_region (region_id, region_name) VALUES
            (1, 'West Coast'),
            (2, 'East Coast'), 
            (3, 'Midwest'),
            (4, 'Southwest'),
            (5, 'Southeast');
        END
    """
    await sql_db.execute(region_query)
    logger.info("✅ Facility regions table setup completed")


async def setup_financial_classes(pg_db: PostgresDatabase, sql_db: SQLServerDatabase, logger):
    """Setup financial classes in both databases (PostgreSQL for validation cache)"""
    logger.info("Setting up financial classes in both databases...")
    
    # Define financial class data
    financial_classes = [
        ("COM", "Commercial Insurance", 1, Decimal("0.8500")),
        ("MED", "Medicaid", 2, Decimal("0.6000")),
        ("MCR", "Medicare", 3, Decimal("0.7500")),
        ("PPO", "PPO Insurance", 4, Decimal("0.8000")),
        ("HMO", "HMO Insurance", 5, Decimal("0.7800")),
        ("WRK", "Workers Compensation", 6, Decimal("0.9000")),
        ("OTH", "Other Insurance", 7, Decimal("0.7000")),
        ("SELF", "Self Pay", 8, Decimal("1.0000")),
    ]
    
    # Setup core standard payers first (for SQL Server foreign key)
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
    
    # Insert payers into SQL Server
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
        await sql_db.execute(payer_query, payer)
    
    logger.info(f"✅ Inserted {len(payers_data)} payers into SQL Server")
    
    # Insert financial classes into SQL Server
    sql_financial_class_query = """
        MERGE facility_financial_classes AS target
        USING (SELECT ? as facility_id, ? as financial_class_id, ? as financial_class_name,
               ? as payer_id, ? as reimbursement_rate, 'HIGH' as processing_priority,
               1 as auto_posting_enabled, 1 as active, GETDATE() as effective_date,
               NULL as end_date, GETDATE() as created_at, 'ACT' as HCC) AS source
        ON target.financial_class_id = source.financial_class_id
        WHEN MATCHED THEN
            UPDATE SET financial_class_name = source.financial_class_name,
                      reimbursement_rate = source.reimbursement_rate
        WHEN NOT MATCHED THEN
            INSERT (facility_id, financial_class_id, financial_class_name, payer_id,
                   reimbursement_rate, processing_priority, auto_posting_enabled, active,
                   effective_date, end_date, created_at, HCC)
            VALUES ('1', source.financial_class_id, source.financial_class_name, source.payer_id,
                   source.reimbursement_rate, source.processing_priority, source.auto_posting_enabled,
                   source.active, source.effective_date, source.end_date, source.created_at, source.HCC);
    """
    
    for fc in financial_classes:
        await sql_db.execute(sql_financial_class_query, fc)
    
    # Insert simplified financial classes into PostgreSQL for validation cache
    pg_financial_class_query = """
        INSERT INTO financial_classes (financial_class_id, financial_class_name, active)
        VALUES ($1, $2, $3)
        ON CONFLICT (financial_class_id) DO UPDATE SET
            financial_class_name = EXCLUDED.financial_class_name,
            updated_at = CURRENT_TIMESTAMP
    """
    
    for fc in financial_classes:
        await pg_db.execute(pg_financial_class_query, fc[0], fc[1], True)
    
    logger.info(f"✅ Inserted {len(financial_classes)} financial classes into SQL Server")
    logger.info(f"✅ Cached {len(financial_classes)} financial classes in PostgreSQL for fast validation")


async def setup_rvu_data(sql_db: SQLServerDatabase, pg_db: PostgresDatabase, logger):
    """Setup RVU data in SQL Server and cache in PostgreSQL"""
    logger.info("Setting up RVU data in SQL Server and caching in PostgreSQL...")
    
    # Define common RVU data for testing
    rvu_data = [
        ("99213", "Office/outpatient visit est", "E&M", "Office", Decimal("0.97"), Decimal("0.67"), Decimal("0.05"), Decimal("1.69"), Decimal("36.04")),
        ("99214", "Office/outpatient visit est", "E&M", "Office", Decimal("1.50"), Decimal("1.11"), Decimal("0.08"), Decimal("2.69"), Decimal("36.04")),
        ("99215", "Office/outpatient visit est", "E&M", "Office", Decimal("2.11"), Decimal("1.52"), Decimal("0.11"), Decimal("3.74"), Decimal("36.04")),
        ("99202", "Office/outpatient visit new", "E&M", "Office", Decimal("0.93"), Decimal("0.65"), Decimal("0.05"), Decimal("1.63"), Decimal("36.04")),
        ("99203", "Office/outpatient visit new", "E&M", "Office", Decimal("1.42"), Decimal("1.07"), Decimal("0.08"), Decimal("2.57"), Decimal("36.04")),
        ("99204", "Office/outpatient visit new", "E&M", "Office", Decimal("2.43"), Decimal("1.73"), Decimal("0.13"), Decimal("4.29"), Decimal("36.04")),
        ("99205", "Office/outpatient visit new", "E&M", "Office", Decimal("3.17"), Decimal("2.25"), Decimal("0.17"), Decimal("5.59"), Decimal("36.04")),
        ("80053", "Comprehensive metabolic panel", "Lab", "Laboratory", Decimal("0.00"), Decimal("1.25"), Decimal("0.06"), Decimal("1.31"), Decimal("36.04")),
        ("85025", "Blood count; complete", "Lab", "Laboratory", Decimal("0.00"), Decimal("0.17"), Decimal("0.01"), Decimal("0.18"), Decimal("36.04")),
        ("73030", "Radiologic exam, shoulder", "Radiology", "Imaging", Decimal("0.22"), Decimal("0.89"), Decimal("0.05"), Decimal("1.16"), Decimal("36.04")),
    ]
    
    # Insert into SQL Server (primary)
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
        # Expand data for SQL Server insert
        expanded_rvu = rvu + (rvu[4], rvu[4])  # Use work_rvu for both non_facility and facility PE
        await sql_db.execute(sql_rvu_query, expanded_rvu)
    
    # Cache in PostgreSQL for fast access during processing
    pg_rvu_query = """
        INSERT INTO rvu_data (procedure_code, description, category, subcategory, work_rvu,
                             practice_expense_rvu, malpractice_rvu, total_rvu, conversion_factor,
                             non_facility_pe_rvu, facility_pe_rvu, effective_date, status,
                             global_period, professional_component, technical_component, bilateral_surgery,
                             created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ON CONFLICT (procedure_code) DO UPDATE SET
            description = EXCLUDED.description,
            total_rvu = EXCLUDED.total_rvu,
            conversion_factor = EXCLUDED.conversion_factor,
            updated_at = CURRENT_TIMESTAMP
    """
    
    for rvu in rvu_data:
        # Prepare data for PostgreSQL insert
        pg_rvu_record = rvu + (
            rvu[4], rvu[4],  # non_facility_pe_rvu, facility_pe_rvu
            datetime.now().date(),  # effective_date
            'ACTIVE',  # status
            '000',  # global_period
            False, False, False,  # professional_component, technical_component, bilateral_surgery
            datetime.now(), datetime.now()  # created_at, updated_at
        )
        await pg_db.execute(pg_rvu_query, *pg_rvu_record)
    
    logger.info(f"✅ Inserted {len(rvu_data)} RVU records into SQL Server")
    logger.info(f"✅ Cached {len(rvu_data)} RVU records in PostgreSQL for fast reimbursement calculations")


if __name__ == "__main__":
    asyncio.run(setup_reference_data())