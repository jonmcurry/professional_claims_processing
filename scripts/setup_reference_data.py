#!/usr/bin/env python3
"""
Reference Data Setup Script
Creates reference data with integer facility IDs to match the original schema.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sqlserver import SQLServerDatabase

# Reference data definitions - UPDATED: Use integer facility IDs
FACILITIES_DATA = [
    {
        "facility_id": i,  # Integer instead of string
        "facility_name": f"Healthcare Facility {i:03d}",
        "facility_type": "Hospital" if i % 3 == 0 else "Clinic",
        "address": f"{1000 + i} Medical Way",
        "city": f"City{i}",
        "state": "CA" if i % 2 == 0 else "NY",
        "zip_code": f"{90000 + i:05d}",
        "phone": f"555-{i:03d}-{(i*37) % 10000:04d}",
        "active": True,
    }
    for i in range(1, 51)  # 50 facilities: IDs 1-50
]

FINANCIAL_CLASSES_DATA = [
    {"class_code": "COM", "class_name": "Commercial Insurance", "description": "Private commercial insurance plans"},
    {"class_code": "MED", "class_name": "Medicaid", "description": "State Medicaid program"},
    {"class_code": "MCR", "class_name": "Medicare", "description": "Federal Medicare program"},
    {"class_code": "PPO", "class_name": "PPO Plan", "description": "Preferred Provider Organization"},
    {"class_code": "HMO", "class_name": "HMO Plan", "description": "Health Maintenance Organization"},
    {"class_code": "WRK", "class_name": "Workers Comp", "description": "Workers compensation insurance"},
    {"class_code": "OTH", "class_name": "Other Insurance", "description": "Other insurance types"},
    {"class_code": "SELF", "class_name": "Self Pay", "description": "Self-pay patients"},
]

RVU_DATA = [
    {"procedure_code": "99213", "total_rvu": 1.3, "work_rvu": 0.97, "practice_expense_rvu": 0.28, "malpractice_rvu": 0.05},
    {"procedure_code": "99214", "total_rvu": 2.0, "work_rvu": 1.5, "practice_expense_rvu": 0.44, "malpractice_rvu": 0.06},
    {"procedure_code": "99215", "total_rvu": 2.8, "work_rvu": 2.11, "practice_expense_rvu": 0.62, "malpractice_rvu": 0.07},
    {"procedure_code": "99203", "total_rvu": 1.6, "work_rvu": 1.21, "practice_expense_rvu": 0.34, "malpractice_rvu": 0.05},
    {"procedure_code": "99204", "total_rvu": 2.4, "work_rvu": 1.92, "practice_expense_rvu": 0.42, "malpractice_rvu": 0.06},
    {"procedure_code": "99205", "total_rvu": 3.2, "work_rvu": 2.56, "practice_expense_rvu": 0.57, "malpractice_rvu": 0.07},
    {"procedure_code": "36415", "total_rvu": 0.17, "work_rvu": 0.17, "practice_expense_rvu": 0.0, "malpractice_rvu": 0.0},
    {"procedure_code": "85025", "total_rvu": 0.28, "work_rvu": 0.0, "practice_expense_rvu": 0.27, "malpractice_rvu": 0.01},
    {"procedure_code": "80053", "total_rvu": 0.54, "work_rvu": 0.0, "practice_expense_rvu": 0.53, "malpractice_rvu": 0.01},
    {"procedure_code": "71020", "total_rvu": 0.22, "work_rvu": 0.22, "practice_expense_rvu": 0.0, "malpractice_rvu": 0.0},
]


async def setup_postgres_simple():
    """PostgreSQL reference data setup with integer facility IDs"""
    logger = logging.getLogger(__name__)
    
    config = load_config()
    pg_db = PostgresDatabase(config.postgres)
    
    try:
        await pg_db.connect(prepare_queries=False)
        logger.info("Connected to PostgreSQL database")

        # Setup facilities table - UPDATED: facility_id as INTEGER
        logger.info("Creating facilities table...")
        try:
            await pg_db.execute("""
                CREATE TABLE IF NOT EXISTS facilities (
                    facility_id INTEGER PRIMARY KEY,
                    facility_name VARCHAR(200) NOT NULL,
                    facility_type VARCHAR(50),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip_code VARCHAR(10),
                    phone VARCHAR(20),
                    active BOOLEAN DEFAULT true,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✓ Facilities table created/verified")
        except Exception as e:
            logger.warning(f"Facilities table creation warning: {e}")

        # Insert facilities data with integer IDs
        logger.info("Inserting facilities data...")
        success_count = 0
        for facility in FACILITIES_DATA:
            try:
                await pg_db.execute("""
                    INSERT INTO facilities (facility_id, facility_name, facility_type, address, city, 
                                          state, zip_code, phone, active)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (facility_id) DO NOTHING
                """, 
                facility['facility_id'], facility['facility_name'], facility['facility_type'],
                facility['address'], facility['city'], facility['state'], 
                facility['zip_code'], facility['phone'], facility['active'])
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert facility {facility['facility_id']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FACILITIES_DATA)} facilities")

        # Setup financial classes table
        logger.info("Creating financial_classes table...")
        try:
            await pg_db.execute("""
                CREATE TABLE IF NOT EXISTS financial_classes (
                    class_code VARCHAR(10) PRIMARY KEY,
                    class_name VARCHAR(100) NOT NULL,
                    description VARCHAR(200),
                    active BOOLEAN DEFAULT true,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✓ Financial classes table created/verified")
        except Exception as e:
            logger.warning(f"Financial classes table creation warning: {e}")

        # Insert financial classes data
        logger.info("Inserting financial classes data...")
        success_count = 0
        for fc in FINANCIAL_CLASSES_DATA:
            try:
                await pg_db.execute("""
                    INSERT INTO financial_classes (class_code, class_name, description, active)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (class_code) DO NOTHING
                """, fc['class_code'], fc['class_name'], fc['description'], True)
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert financial class {fc['class_code']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FINANCIAL_CLASSES_DATA)} financial classes")

    except Exception as e:
        logger.error(f"PostgreSQL setup failed: {e}")
        raise
    finally:
        await pg_db.close()


async def setup_sqlserver_simple():
    """SQL Server reference data setup with integer facility IDs"""
    logger = logging.getLogger(__name__)
    
    config = load_config()
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        await sql_db.connect()
        logger.info("Connected to SQL Server database")

        # First, ensure core_standard_payers table exists with sample data
        logger.info("Setting up core_standard_payers table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'core_standard_payers')
                CREATE TABLE core_standard_payers (
                    payer_id INT PRIMARY KEY,
                    payer_name VARCHAR(100) NOT NULL,
                    payer_code VARCHAR(10) NOT NULL
                )
            """)

            # Insert sample payer data
            sample_payers = [
                (1, 'Medicare', 'MC'),
                (2, 'Medicaid', 'MD'),
                (3, 'Commercial', 'COM'),
                (4, 'Blue Cross', 'BC'),
                (5, 'Aetna', 'AET'),
                (6, 'United Health', 'UNH'),
                (7, 'Workers Comp', 'WC'),
                (8, 'Self Pay', 'SP')
            ]

            for payer_id, payer_name, payer_code in sample_payers:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM core_standard_payers WHERE payer_id = ?)
                    INSERT INTO core_standard_payers (payer_id, payer_name, payer_code)
                    VALUES (?, ?, ?)
                """, payer_id, payer_id, payer_name, payer_code)

            logger.info("✓ Core payers table setup completed")
        except Exception as e:
            logger.warning(f"Core payers setup warning: {e}")

        # Setup facilities table with INTEGER facility_id (original schema)
        logger.info("Creating facilities table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'facilities')
                CREATE TABLE facilities (
                    facility_id INT PRIMARY KEY,
                    facility_name VARCHAR(200) NOT NULL,
                    facility_type VARCHAR(50),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip_code VARCHAR(10),
                    phone VARCHAR(20),
                    active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            logger.info("✓ Facilities table created/verified")
        except Exception as e:
            logger.warning(f"Facilities table creation warning: {e}")

        # Insert facilities data with integer IDs
        logger.info("Inserting facilities data...")
        success_count = 0
        for facility in FACILITIES_DATA:
            try:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM facilities WHERE facility_id = ?)
                    INSERT INTO facilities (facility_id, facility_name, facility_type, address, city, 
                                          state, zip_code, phone, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
                """, 
                facility['facility_id'],  # Integer for the EXISTS check
                facility['facility_id'], facility['facility_name'], facility['facility_type'],
                facility['address'], facility['city'], facility['state'], 
                facility['zip_code'], facility['phone'])
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert facility {facility['facility_id']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FACILITIES_DATA)} facilities")

        # Setup financial classes table - UPDATED: facility_id as INT to match facilities table
        logger.info("Creating facility_financial_classes table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'facility_financial_classes')
                CREATE TABLE facility_financial_classes (
                    financial_class_id VARCHAR(10) PRIMARY KEY,
                    class_name VARCHAR(100) NOT NULL,
                    description VARCHAR(200),
                    facility_id INT,
                    payer_id INT,
                    reimbursement_rate DECIMAL(5,4) DEFAULT 0.8000,
                    processing_priority VARCHAR(10) DEFAULT 'Normal',
                    auto_posting_enabled BIT DEFAULT 1,
                    active BIT DEFAULT 1,
                    effective_date DATE DEFAULT GETDATE(),
                    end_date DATE,
                    created_at DATETIME2 DEFAULT GETDATE(),
                    FOREIGN KEY (facility_id) REFERENCES facilities(facility_id),
                    FOREIGN KEY (payer_id) REFERENCES core_standard_payers(payer_id)
                )
            """)
            logger.info("✓ Financial classes table created/verified")
        except Exception as e:
            logger.warning(f"Financial classes table creation warning: {e}")

        # Insert financial classes data
        logger.info("Inserting financial classes data...")
        success_count = 0
        for fc in FINANCIAL_CLASSES_DATA:
            try:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM facility_financial_classes WHERE financial_class_id = ?)
                    INSERT INTO facility_financial_classes (financial_class_id, class_name, description, payer_id, active)
                    VALUES (?, ?, ?, ?, 1)
                """, 
                fc['class_code'],  # for the EXISTS check
                fc['class_code'], fc['class_name'], fc['description'], 1)  # Default to payer_id 1 (Medicare)
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert financial class {fc['class_code']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FINANCIAL_CLASSES_DATA)} financial classes")

        # Setup RVU data table
        logger.info("Creating rvu_data table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rvu_data')
                CREATE TABLE rvu_data (
                    procedure_code VARCHAR(10) PRIMARY KEY,
                    total_rvu DECIMAL(8,4) NOT NULL,
                    work_rvu DECIMAL(8,4) NOT NULL,
                    practice_expense_rvu DECIMAL(8,4) NOT NULL,
                    malpractice_rvu DECIMAL(8,4) NOT NULL,
                    effective_date DATE DEFAULT GETDATE(),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            logger.info("✓ RVU data table created/verified")
        except Exception as e:
            logger.warning(f"RVU data table creation warning: {e}")

        # Insert RVU data
        logger.info("Inserting RVU data...")
        success_count = 0
        for rvu in RVU_DATA:
            try:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM rvu_data WHERE procedure_code = ?)
                    INSERT INTO rvu_data (procedure_code, total_rvu, work_rvu, practice_expense_rvu, malpractice_rvu)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    rvu["procedure_code"],  # for the EXISTS check
                    rvu["procedure_code"], rvu["total_rvu"], rvu["work_rvu"],
                    rvu["practice_expense_rvu"], rvu["malpractice_rvu"]
                )
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert RVU {rvu['procedure_code']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(RVU_DATA)} RVU records")

    except Exception as e:
        logger.error(f"SQL Server setup failed: {e}")
        raise
    finally:
        await sql_db.close()


async def setup_reference_data():
    """Run reference data setup for both databases."""
    logger = logging.getLogger(__name__)

    logger.info("Setting up PostgreSQL reference data...")
    await setup_postgres_simple()

    logger.info("Setting up SQL Server reference data...")
    await setup_sqlserver_simple()


async def main():
    """Main entrypoint when executing as a script."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("REFERENCE DATA SETUP - INTEGER FACILITY IDS")
    logger.info("=" * 60)

    try:
        await setup_reference_data()

        logger.info("=" * 60)
        logger.info("✓ REFERENCE DATA SETUP COMPLETED SUCCESSFULLY!")
        logger.info("Generated facility IDs: 1-50 (integers)")
        logger.info("✓ Data compatible with original schema")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())