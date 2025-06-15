#!/usr/bin/env python3
"""
Reference Data Setup Script
Creates and populates reference data tables required for claims validation.
This includes facilities, financial classes, and RVU data.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Tuple

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sqlserver import SQLServerDatabase


async def setup_reference_data():
    """
    Main function to set up all reference data required for claims processing.
    Creates and populates:
    - Facilities (PostgreSQL and SQL Server)
    - Financial classes 
    - RVU data (SQL Server)
    """
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting reference data setup...")
    
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
        
        # Setup facilities in both databases
        await setup_facilities(pg_db, sql_db, logger)
        
        # Setup financial classes
        await setup_financial_classes(pg_db, sql_db, logger)
        
        # Setup RVU data in SQL Server
        await setup_rvu_data(sql_db, logger)
        
        logger.info("Reference data setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during reference data setup: {e}")
        raise
    finally:
        await pg_db.close()
        await sql_db.close()
        logger.info("Database connections closed")


async def setup_facilities(pg_db: PostgresDatabase, sql_db: SQLServerDatabase, logger):
    """Setup facilities in both PostgreSQL and SQL Server"""
    logger.info("Setting up facilities...")
    
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
    
    # Insert into PostgreSQL
    pg_query = """
        INSERT INTO facilities (
            facility_id, facility_name, facility_description, facility_type,
            address, installed_date, beds, city, state, zip_code, phone,
            active, updated_date, updated_by, region_id, fiscal_month
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
        ON CONFLICT (facility_id) DO UPDATE SET
            facility_name = EXCLUDED.facility_name,
            facility_description = EXCLUDED.facility_description,
            updated_date = EXCLUDED.updated_date
    """
    
    for facility in facilities_data:
        await pg_db.execute(pg_query, *facility)
    
    logger.info(f"Inserted {len(facilities_data)} facilities into PostgreSQL")
    
    # Insert into SQL Server
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
    
    logger.info(f"Inserted {len(facilities_data)} facilities into SQL Server")


async def setup_financial_classes(pg_db: PostgresDatabase, sql_db: SQLServerDatabase, logger):
    """Setup financial classes"""
    logger.info("Setting up financial classes...")
    
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
        WHEN NOT MATCHED THEN
            INSERT (payer_id, payer_name, payer_code)
            VALUES (source.payer_id, source.payer_name, source.payer_code);
    """
    
    for payer in payers_data:
        await sql_db.execute(payer_query, payer)
    
    logger.info(f"Inserted {len(payers_data)} payers into SQL Server")
    
    # Now insert financial classes for each facility
    for facility_id in range(1, 51):
        facility_id_str = str(facility_id)
        
        for fc_code, fc_name, payer_id, reimb_rate in financial_classes:
            # PostgreSQL - simplified financial classes
            pg_query = """
                INSERT INTO financial_classes (
                    financial_class_code, financial_class_name, active, created_at
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT (financial_class_code) DO NOTHING
            """
            await pg_db.execute(pg_query, fc_code, fc_name, True, datetime.now())
            
            # SQL Server - facility-specific financial classes
            sql_query = """
                MERGE facility_financial_classes AS target
                USING (SELECT ? as facility_id, ? as financial_class_id, ? as financial_class_name,
                       ? as payer_id, ? as reimbursement_rate, ? as processing_priority,
                       ? as auto_posting_enabled, ? as active, ? as effective_date,
                       ? as created_at, ? as HCC) AS source
                ON target.facility_id = source.facility_id AND target.financial_class_id = source.financial_class_id
                WHEN NOT MATCHED THEN
                    INSERT (facility_id, financial_class_id, financial_class_name, payer_id,
                           reimbursement_rate, processing_priority, auto_posting_enabled, active,
                           effective_date, created_at, HCC)
                    VALUES (source.facility_id, source.financial_class_id, source.financial_class_name,
                           source.payer_id, source.reimbursement_rate, source.processing_priority,
                           source.auto_posting_enabled, source.active, source.effective_date,
                           source.created_at, source.HCC);
            """
            
            await sql_db.execute(
                sql_query,
                facility_id_str,  # facility_id
                fc_code,  # financial_class_id
                fc_name,  # financial_class_name
                payer_id,  # payer_id
                reimb_rate,  # reimbursement_rate
                "normal",  # processing_priority
                True,  # auto_posting_enabled
                True,  # active
                datetime.now().date(),  # effective_date
                datetime.now(),  # created_at
                "N/A",  # HCC
            )
    
    logger.info("Inserted financial classes into both databases")


async def setup_rvu_data(sql_db: SQLServerDatabase, logger):
    """Setup RVU data in SQL Server"""
    logger.info("Setting up RVU data...")
    
    # Common procedure codes with their RVU values
    rvu_data = [
        # Office visits
        ("99213", Decimal("1.30"), "Office visit, established patient, low complexity"),
        ("99214", Decimal("2.00"), "Office visit, established patient, moderate complexity"),
        ("99215", Decimal("2.80"), "Office visit, established patient, high complexity"),
        ("99203", Decimal("1.60"), "Office visit, new patient, low complexity"),
        ("99204", Decimal("2.60"), "Office visit, new patient, moderate complexity"),
        ("99205", Decimal("3.50"), "Office visit, new patient, high complexity"),
        
        # Emergency visits
        ("99282", Decimal("1.00"), "Emergency department visit, low complexity"),
        ("99283", Decimal("1.80"), "Emergency department visit, moderate complexity"),
        ("99284", Decimal("3.20"), "Emergency department visit, high complexity"),
        ("99285", Decimal("5.00"), "Emergency department visit, very high complexity"),
        
        # Lab procedures
        ("36415", Decimal("0.25"), "Venipuncture"),
        ("85025", Decimal("0.30"), "Complete blood count"),
        ("80053", Decimal("0.50"), "Comprehensive metabolic panel"),
        ("80061", Decimal("0.40"), "Lipid panel"),
        
        # Radiology
        ("71020", Decimal("0.70"), "Chest X-ray, 2 views"),
        ("71030", Decimal("0.90"), "Chest X-ray, complete"),
        ("73060", Decimal("0.60"), "Knee X-ray, 2 views"),
        ("73070", Decimal("0.80"), "Ankle X-ray, 2 views"),
        
        # Surgery
        ("29881", Decimal("8.50"), "Arthroscopy, knee, surgical"),
        ("29882", Decimal("10.20"), "Arthroscopy, knee, with meniscectomy"),
        ("64483", Decimal("4.50"), "Injection, lumbar facet joint"),
        ("64484", Decimal("5.20"), "Injection, lumbar facet joint, additional"),
        
        # Mental health
        ("90834", Decimal("1.80"), "Psychotherapy, 45 minutes"),
        ("90837", Decimal("2.40"), "Psychotherapy, 60 minutes"),
        ("90853", Decimal("1.20"), "Group psychotherapy"),
        ("96116", Decimal("2.50"), "Neuropsychological testing"),
        
        # Preventive care
        ("G0438", Decimal("2.30"), "Annual wellness visit, initial"),
        ("G0439", Decimal("2.00"), "Annual wellness visit, subsequent"),
        ("99490", Decimal("1.50"), "Chronic care management, 20 minutes"),
        ("99491", Decimal("2.00"), "Chronic care management, 30 minutes"),
    ]
    
    # Insert RVU data
    rvu_query = """
        MERGE rvu_data AS target
        USING (SELECT ? as procedure_code, ? as work_rvu, ? as description,
               ? as effective_date, ? as active) AS source
        ON target.procedure_code = source.procedure_code
        WHEN MATCHED THEN
            UPDATE SET work_rvu = source.work_rvu,
                      description = source.description,
                      effective_date = source.effective_date
        WHEN NOT MATCHED THEN
            INSERT (procedure_code, work_rvu, description, effective_date, active)
            VALUES (source.procedure_code, source.work_rvu, source.description,
                   source.effective_date, source.active);
    """
    
    for proc_code, rvu_value, description in rvu_data:
        await sql_db.execute(
            rvu_query,
            proc_code,
            rvu_value,
            description,
            datetime.now().date(),
            True
        )
    
    logger.info(f"Inserted {len(rvu_data)} RVU records into SQL Server")


if __name__ == "__main__":
    """Allow the script to be run standalone"""
    asyncio.run(setup_reference_data())