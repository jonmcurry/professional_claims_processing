#!/usr/bin/env python3
"""
Simple Reference Data Setup Script
Creates reference data with minimal error checking to get things working quickly.
"""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase

# Reference data definitions
FACILITIES_DATA = [
    {
        "facility_id": f"FAC{i:03d}",
        "facility_name": f"Healthcare Facility {i:03d}",
        "facility_type": "Hospital" if i % 3 == 0 else "Clinic",
        "address": f"{1000 + i} Medical Way",
        "city": f"City{i}",
        "state": "CA" if i % 2 == 0 else "NY",
        "zip_code": f"{90000 + i:05d}",
        "phone": f"555-{i:03d}-{(i*37) % 10000:04d}",
        "active": True,
    }
    for i in range(1, 51)  # 50 facilities
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
    {"procedure_code": "99215", "total_rvu": 2.8, "work_rvu": 2.11, "practice_expense_rvu": 0.61, "malpractice_rvu": 0.08},
    {"procedure_code": "36415", "total_rvu": 0.17, "work_rvu": 0.17, "practice_expense_rvu": 0.00, "malpractice_rvu": 0.00},
    {"procedure_code": "85025", "total_rvu": 0.28, "work_rvu": 0.00, "practice_expense_rvu": 0.28, "malpractice_rvu": 0.00},
]


async def setup_postgres_simple():
    """Simple PostgreSQL reference data setup"""
    logger = logging.getLogger(__name__)
    
    config = load_config()
    pg_db = PostgresDatabase(config.postgres)
    
    try:
        await pg_db.connect()
        logger.info("Connected to PostgreSQL database")

        # Create facilities table (ignore if exists)
        logger.info("Creating facilities table...")
        try:
            await pg_db.execute("""
                CREATE TABLE IF NOT EXISTS facilities (
                    facility_id VARCHAR(20) PRIMARY KEY,
                    facility_name VARCHAR(200) NOT NULL,
                    facility_type VARCHAR(50),
                    address VARCHAR(200),
                    city VARCHAR(100),
                    state VARCHAR(2),
                    zip_code VARCHAR(10),
                    phone VARCHAR(20),
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✓ Facilities table created/verified")
        except Exception as e:
            logger.warning(f"Facilities table creation failed: {e}")

        # Insert facilities data
        logger.info("Inserting facilities data...")
        success_count = 0
        for facility in FACILITIES_DATA:
            try:
                await pg_db.execute("""
                    INSERT INTO facilities (facility_id, facility_name, facility_type, 
                                          address, city, state, zip_code, phone, active)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (facility_id) DO NOTHING
                """,
                    facility["facility_id"], facility["facility_name"], facility["facility_type"],
                    facility["address"], facility["city"], facility["state"],
                    facility["zip_code"], facility["phone"], facility["active"]
                )
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert facility {facility['facility_id']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FACILITIES_DATA)} facilities")

        # Create financial_classes table (ignore if exists)
        logger.info("Creating financial_classes table...")
        try:
            await pg_db.execute("""
                CREATE TABLE IF NOT EXISTS financial_classes (
                    class_code VARCHAR(10) PRIMARY KEY,
                    class_name VARCHAR(100) NOT NULL,
                    description VARCHAR(255)
                )
            """)
            logger.info("✓ Financial classes table created/verified")
        except Exception as e:
            logger.warning(f"Financial classes table creation failed: {e}")

        # Insert financial classes data
        logger.info("Inserting financial classes data...")
        success_count = 0
        for fc in FINANCIAL_CLASSES_DATA:
            try:
                await pg_db.execute("""
                    INSERT INTO financial_classes (class_code, class_name, description)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (class_code) DO NOTHING
                """,
                    fc["class_code"], fc["class_name"], fc["description"]
                )
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
    """Simple SQL Server reference data setup"""
    logger = logging.getLogger(__name__)
    
    config = load_config()
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        await sql_db.connect()
        logger.info("Connected to SQL Server database")

        # Create facilities table (ignore if exists)
        logger.info("Creating facilities table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'facilities')
                CREATE TABLE facilities (
                    facility_id NVARCHAR(20) PRIMARY KEY,
                    facility_name NVARCHAR(200) NOT NULL,
                    facility_type NVARCHAR(50),
                    address NVARCHAR(200),
                    city NVARCHAR(100),
                    state NVARCHAR(2),
                    zip_code NVARCHAR(10),
                    phone NVARCHAR(20),
                    active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            logger.info("✓ Facilities table created/verified")
        except Exception as e:
            logger.warning(f"Facilities table creation failed: {e}")

        # Insert facilities data
        logger.info("Inserting facilities data...")
        success_count = 0
        for facility in FACILITIES_DATA:
            try:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM facilities WHERE facility_id = ?)
                    INSERT INTO facilities (facility_id, facility_name, facility_type, address, city, 
                                          state, zip_code, phone, active)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    facility["facility_id"],  # For the EXISTS check
                    facility["facility_id"], facility["facility_name"], facility["facility_type"],
                    facility["address"], facility["city"], facility["state"],
                    facility["zip_code"], facility["phone"], facility["active"]
                )
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert facility {facility['facility_id']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FACILITIES_DATA)} facilities")

        # Create financial_classes table (ignore if exists)
        logger.info("Creating facility_financial_classes table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'facility_financial_classes')
                CREATE TABLE facility_financial_classes (
                    financial_class_id NVARCHAR(10) PRIMARY KEY,
                    class_name NVARCHAR(100) NOT NULL,
                    description NVARCHAR(255)
                )
            """)
            logger.info("✓ Financial classes table created/verified")
        except Exception as e:
            logger.warning(f"Financial classes table creation failed: {e}")

        # Insert financial classes data
        logger.info("Inserting financial classes data...")
        success_count = 0
        for fc in FINANCIAL_CLASSES_DATA:
            try:
                await sql_db.execute("""
                    IF NOT EXISTS (SELECT * FROM facility_financial_classes WHERE financial_class_id = ?)
                    INSERT INTO facility_financial_classes (financial_class_id, class_name, description)
                    VALUES (?, ?, ?)
                """,
                    fc["class_code"],  # For the EXISTS check
                    fc["class_code"], fc["class_name"], fc["description"]
                )
                success_count += 1
            except Exception as e:
                logger.warning(f"Failed to insert financial class {fc['class_code']}: {e}")
        
        logger.info(f"✓ Inserted {success_count}/{len(FINANCIAL_CLASSES_DATA)} financial classes")

        # Create rvu_data table (ignore if exists)
        logger.info("Creating rvu_data table...")
        try:
            await sql_db.execute("""
                IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'rvu_data')
                CREATE TABLE rvu_data (
                    procedure_code NVARCHAR(10) PRIMARY KEY,
                    total_rvu DECIMAL(8,4) NOT NULL,
                    work_rvu DECIMAL(8,4) NOT NULL,
                    practice_expense_rvu DECIMAL(8,4) NOT NULL,
                    malpractice_rvu DECIMAL(8,4) NOT NULL,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
            logger.info("✓ RVU data table created/verified")
        except Exception as e:
            logger.warning(f"RVU data table creation failed: {e}")

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
                    rvu["procedure_code"],  # For the EXISTS check
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


async def main():
    """Main function"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("SIMPLE REFERENCE DATA SETUP")
    logger.info("=" * 60)

    try:
        logger.info("Setting up PostgreSQL reference data...")
        await setup_postgres_simple()
        
        logger.info("Setting up SQL Server reference data...")
        await setup_sqlserver_simple()
        
        logger.info("=" * 60)
        logger.info("✓ REFERENCE DATA SETUP COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Setup failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())