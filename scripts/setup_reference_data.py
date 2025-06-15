#!/usr/bin/env python3
"""
Setup Reference Data Script
Creates necessary reference data (facilities, financial classes, etc.) 
that the sample claims will reference for validation.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal

from src.config.config import load_config
from src.database.postgres import PostgresDatabase
from src.database.sqlserver import SQLServerDatabase


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
        "active": True
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
    {"class_code": "SELF", "class_name": "Self Pay", "description": "Self-pay patients"}
]

# RVU data for procedure codes used in sample generation
RVU_DATA = [
    # Office visits
    {"procedure_code": "99213", "total_rvu": 1.3, "work_rvu": 0.97, "practice_expense_rvu": 0.28, "malpractice_rvu": 0.05},
    {"procedure_code": "99214", "total_rvu": 2.0, "work_rvu": 1.5, "practice_expense_rvu": 0.43, "malpractice_rvu": 0.07},
    {"procedure_code": "99215", "total_rvu": 2.8, "work_rvu": 2.11, "practice_expense_rvu": 0.59, "malpractice_rvu": 0.10},
    {"procedure_code": "99203", "total_rvu": 1.6, "work_rvu": 1.13, "practice_expense_rvu": 0.39, "malpractice_rvu": 0.08},
    {"procedure_code": "99204", "total_rvu": 2.6, "work_rvu": 1.92, "practice_expense_rvu": 0.58, "malpractice_rvu": 0.10},
    {"procedure_code": "99205", "total_rvu": 3.5, "work_rvu": 2.61, "practice_expense_rvu": 0.77, "malpractice_rvu": 0.12},
    
    # Emergency visits
    {"procedure_code": "99282", "total_rvu": 1.4, "work_rvu": 0.93, "practice_expense_rvu": 0.39, "malpractice_rvu": 0.08},
    {"procedure_code": "99283", "total_rvu": 2.2, "work_rvu": 1.42, "practice_expense_rvu": 0.68, "malpractice_rvu": 0.10},
    {"procedure_code": "99284", "total_rvu": 3.8, "work_rvu": 2.57, "practice_expense_rvu": 1.11, "malpractice_rvu": 0.12},
    {"procedure_code": "99285", "total_rvu": 5.9, "work_rvu": 4.15, "practice_expense_rvu": 1.62, "malpractice_rvu": 0.13},
    
    # Lab procedures
    {"procedure_code": "36415", "total_rvu": 0.17, "work_rvu": 0.17, "practice_expense_rvu": 0.00, "malpractice_rvu": 0.00},
    {"procedure_code": "85025", "total_rvu": 0.17, "work_rvu": 0.00, "practice_expense_rvu": 0.16, "malpractice_rvu": 0.01},
    {"procedure_code": "80053", "total_rvu": 0.17, "work_rvu": 0.00, "practice_expense_rvu": 0.16, "malpractice_rvu": 0.01},
    {"procedure_code": "80061", "total_rvu": 0.17, "work_rvu": 0.00, "practice_expense_rvu": 0.16, "malpractice_rvu": 0.01},
    
    # Radiology
    {"procedure_code": "71020", "total_rvu": 0.22, "work_rvu": 0.15, "practice_expense_rvu": 0.06, "malpractice_rvu": 0.01},
    {"procedure_code": "71030", "total_rvu": 0.26, "work_rvu": 0.18, "practice_expense_rvu": 0.07, "malpractice_rvu": 0.01},
    {"procedure_code": "73060", "total_rvu": 0.22, "work_rvu": 0.15, "practice_expense_rvu": 0.06, "malpractice_rvu": 0.01},
    {"procedure_code": "73070", "total_rvu": 0.26, "work_rvu": 0.18, "practice_expense_rvu": 0.07, "malpractice_rvu": 0.01},
    
    # Surgery
    {"procedure_code": "29881", "total_rvu": 7.75, "work_rvu": 5.84, "practice_expense_rvu": 1.73, "malpractice_rvu": 0.18},
    {"procedure_code": "29882", "total_rvu": 10.93, "work_rvu": 8.25, "practice_expense_rvu": 2.43, "malpractice_rvu": 0.25},
    {"procedure_code": "64483", "total_rvu": 1.33, "work_rvu": 1.05, "practice_expense_rvu": 0.25, "malpractice_rvu": 0.03},
    {"procedure_code": "64484", "total_rvu": 0.75, "work_rvu": 0.75, "practice_expense_rvu": 0.00, "malpractice_rvu": 0.00},
    
    # Mental health
    {"procedure_code": "90834", "total_rvu": 1.35, "work_rvu": 1.23, "practice_expense_rvu": 0.11, "malpractice_rvu": 0.01},
    {"procedure_code": "90837", "total_rvu": 2.06, "work_rvu": 1.89, "practice_expense_rvu": 0.16, "malpractice_rvu": 0.01},
    {"procedure_code": "90853", "total_rvu": 1.64, "work_rvu": 1.50, "practice_expense_rvu": 0.13, "malpractice_rvu": 0.01},
    {"procedure_code": "96116", "total_rvu": 1.0, "work_rvu": 0.89, "practice_expense_rvu": 0.10, "malpractice_rvu": 0.01},
    
    # Preventive care
    {"procedure_code": "G0438", "total_rvu": 2.39, "work_rvu": 1.79, "practice_expense_rvu": 0.54, "malpractice_rvu": 0.06},
    {"procedure_code": "G0439", "total_rvu": 3.18, "work_rvu": 2.39, "practice_expense_rvu": 0.71, "malpractice_rvu": 0.08},
    {"procedure_code": "99490", "total_rvu": 0.61, "work_rvu": 0.61, "practice_expense_rvu": 0.00, "malpractice_rvu": 0.00},
    {"procedure_code": "99491", "total_rvu": 1.3, "work_rvu": 1.30, "practice_expense_rvu": 0.00, "malpractice_rvu": 0.00},
]


async def setup_postgres_reference_data(db: PostgresDatabase):
    """Setup reference data in PostgreSQL staging database"""
    logger = logging.getLogger(__name__)
    
    try:
        # Check if we need to create facilities table
        table_exists = await db.fetch("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'facilities'
            )
        """)
        
        if not table_exists[0]['exists']:
            logger.info("Creating facilities table...")
            await db.execute("""
                CREATE TABLE facilities (
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
        
        # Insert facilities data
        logger.info("Inserting facilities data...")
        for facility in FACILITIES_DATA:
            await db.execute("""
                INSERT INTO facilities (
                    facility_id, facility_name, facility_type, address,
                    city, state, zip_code, phone, active
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (facility_id) DO UPDATE SET
                    facility_name = EXCLUDED.facility_name,
                    updated_at = CURRENT_TIMESTAMP
            """, facility["facility_id"], facility["facility_name"], 
                facility["facility_type"], facility["address"], facility["city"],
                facility["state"], facility["zip_code"], facility["phone"], facility["active"])
        
        logger.info(f"Inserted {len(FACILITIES_DATA)} facilities")
        
        # Check if we need to create financial_classes table
        table_exists = await db.fetch("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'financial_classes'
            )
        """)
        
        if not table_exists[0]['exists']:
            logger.info("Creating financial_classes table...")
            await db.execute("""
                CREATE TABLE financial_classes (
                    class_code VARCHAR(10) PRIMARY KEY,
                    class_name VARCHAR(100) NOT NULL,
                    description VARCHAR(255),
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        # Insert financial classes data
        logger.info("Inserting financial classes data...")
        for fc in FINANCIAL_CLASSES_DATA:
            await db.execute("""
                INSERT INTO financial_classes (class_code, class_name, description)
                VALUES ($1, $2, $3)
                ON CONFLICT (class_code) DO UPDATE SET
                    class_name = EXCLUDED.class_name,
                    description = EXCLUDED.description
            """, fc["class_code"], fc["class_name"], fc["description"])
        
        logger.info(f"Inserted {len(FINANCIAL_CLASSES_DATA)} financial classes")
        
    except Exception as e:
        logger.error(f"Error setting up PostgreSQL reference data: {e}")
        raise


async def setup_sqlserver_reference_data(db: SQLServerDatabase):
    """Setup reference data in SQL Server production database"""
    logger = logging.getLogger(__name__)
    
    try:
        # Check if facilities table exists
        table_exists = await db.fetch("""
            SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'facilities'
        """)
        
        if table_exists[0]['count'] == 0:
            logger.info("Creating facilities table in SQL Server...")
            await db.execute("""
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
        
        # Insert facilities data
        logger.info("Inserting facilities data into SQL Server...")
        for facility in FACILITIES_DATA:
            await db.execute("""
                MERGE facilities AS target
                USING (SELECT ? as facility_id, ? as facility_name, ? as facility_type,
                             ? as address, ? as city, ? as state, ? as zip_code,
                             ? as phone, ? as active) AS source
                ON target.facility_id = source.facility_id
                WHEN MATCHED THEN
                    UPDATE SET facility_name = source.facility_name
                WHEN NOT MATCHED THEN
                    INSERT (facility_id, facility_name, facility_type, address,
                           city, state, zip_code, phone, active)
                    VALUES (source.facility_id, source.facility_name, source.facility_type,
                           source.address, source.city, source.state, source.zip_code,
                           source.phone, source.active);
            """, facility["facility_id"], facility["facility_name"], 
                facility["facility_type"], facility["address"], facility["city"],
                facility["state"], facility["zip_code"], facility["phone"], facility["active"])
        
        logger.info(f"Inserted {len(FACILITIES_DATA)} facilities into SQL Server")
        
        # Check if RVU table exists
        table_exists = await db.fetch("""
            SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'rvu_data'
        """)
        
        if table_exists[0]['count'] == 0:
            logger.info("Creating RVU data table in SQL Server...")
            await db.execute("""
                CREATE TABLE rvu_data (
                    procedure_code NVARCHAR(10) PRIMARY KEY,
                    total_rvu DECIMAL(8,4) NOT NULL,
                    work_rvu DECIMAL(8,4),
                    practice_expense_rvu DECIMAL(8,4),
                    malpractice_rvu DECIMAL(8,4),
                    conversion_factor DECIMAL(8,2) DEFAULT 36.04,
                    effective_date DATE DEFAULT GETDATE(),
                    active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
        
        # Insert RVU data
        logger.info("Inserting RVU data into SQL Server...")
        for rvu in RVU_DATA:
            await db.execute("""
                MERGE rvu_data AS target
                USING (SELECT ? as procedure_code, ? as total_rvu, ? as work_rvu,
                             ? as practice_expense_rvu, ? as malpractice_rvu) AS source
                ON target.procedure_code = source.procedure_code
                WHEN MATCHED THEN
                    UPDATE SET total_rvu = source.total_rvu,
                              work_rvu = source.work_rvu,
                              practice_expense_rvu = source.practice_expense_rvu,
                              malpractice_rvu = source.malpractice_rvu
                WHEN NOT MATCHED THEN
                    INSERT (procedure_code, total_rvu, work_rvu, practice_expense_rvu, malpractice_rvu)
                    VALUES (source.procedure_code, source.total_rvu, source.work_rvu,
                           source.practice_expense_rvu, source.malpractice_rvu);
            """, rvu["procedure_code"], rvu["total_rvu"], rvu["work_rvu"],
                rvu["practice_expense_rvu"], rvu["malpractice_rvu"])
        
        logger.info(f"Inserted {len(RVU_DATA)} RVU records into SQL Server")
        
        # Check if financial_classes table exists
        table_exists = await db.fetch("""
            SELECT COUNT(*) as count FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'financial_classes'
        """)
        
        if table_exists[0]['count'] == 0:
            logger.info("Creating financial_classes table in SQL Server...")
            await db.execute("""
                CREATE TABLE financial_classes (
                    class_code NVARCHAR(10) PRIMARY KEY,
                    class_name NVARCHAR(100) NOT NULL,
                    description NVARCHAR(255),
                    active BIT DEFAULT 1,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """)
        
        # Insert financial classes data
        logger.info("Inserting financial classes data into SQL Server...")
        for fc in FINANCIAL_CLASSES_DATA:
            await db.execute("""
                MERGE financial_classes AS target
                USING (SELECT ? as class_code, ? as class_name, ? as description) AS source
                ON target.class_code = source.class_code
                WHEN MATCHED THEN
                    UPDATE SET class_name = source.class_name, description = source.description
                WHEN NOT MATCHED THEN
                    INSERT (class_code, class_name, description)
                    VALUES (source.class_code, source.class_name, source.description);
            """, fc["class_code"], fc["class_name"], fc["description"])
        
        logger.info(f"Inserted {len(FINANCIAL_CLASSES_DATA)} financial classes into SQL Server")
        
    except Exception as e:
        logger.error(f"Error setting up SQL Server reference data: {e}")
        raise


async def setup_reference_data():
    """Main function to setup all reference data"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Setting up reference data for sample claims generation...")
    
    # Load configuration
    config = load_config()
    
    # Setup PostgreSQL reference data
    pg_db = PostgresDatabase(config.postgres)
    try:
        await pg_db.connect()
        logger.info("Connected to PostgreSQL database")
        await setup_postgres_reference_data(pg_db)
    finally:
        await pg_db.close()
        logger.info("PostgreSQL connection closed")
    
    # Setup SQL Server reference data
    sql_db = SQLServerDatabase(config.sqlserver)
    try:
        await sql_db.connect()
        logger.info("Connected to SQL Server database")
        await setup_sqlserver_reference_data(sql_db)
    finally:
        await sql_db.close()
        logger.info("SQL Server connection closed")
    
    logger.info("Reference data setup completed successfully!")


if __name__ == "__main__":
    asyncio.run(setup_reference_data())