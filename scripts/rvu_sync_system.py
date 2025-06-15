#!/usr/bin/env python3
"""
RVU Data Synchronization System
Manages RVU data between SQL Server (master) and PostgreSQL (processing cache)
"""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
import hashlib
import json

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


class RVUDataSync:
    """
    RVU Data Synchronization Manager
    
    Handles:
    - Initial population of PostgreSQL from SQL Server
    - Incremental updates when RVU data changes
    - Data integrity validation
    - Performance optimization for claims processing
    """
    
    def __init__(self, pg_db: PostgresDatabase, sql_db: SQLServerDatabase):
        self.pg_db = pg_db
        self.sql_db = sql_db
        self.logger = logging.getLogger(__name__)
    
    async def initial_sync(self) -> None:
        """Perform initial population of PostgreSQL RVU data from SQL Server"""
        self.logger.info("Starting initial RVU data sync from SQL Server to PostgreSQL...")
        
        # Get all RVU data from SQL Server (master)
        sql_rvu_data = await self._fetch_sql_server_rvu_data()
        
        if not sql_rvu_data:
            self.logger.warning("No RVU data found in SQL Server master")
            return
        
        # Clear existing PostgreSQL RVU data
        await self.pg_db.execute("TRUNCATE TABLE rvu_data")
        self.logger.info("Cleared existing PostgreSQL RVU data")
        
        # Bulk insert into PostgreSQL
        await self._bulk_insert_postgres_rvu(sql_rvu_data)
        
        # Create sync tracking record
        await self._record_sync_completion("initial_sync", len(sql_rvu_data))
        
        self.logger.info(f"Initial sync completed: {len(sql_rvu_data)} RVU records synced")
    
    async def incremental_sync(self) -> int:
        """Perform incremental sync of changes since last sync"""
        self.logger.info("Starting incremental RVU data sync...")
        
        # Get last sync timestamp
        last_sync = await self._get_last_sync_timestamp()
        
        # Get changed records from SQL Server
        changed_records = await self._fetch_changed_rvu_data(last_sync)
        
        if not changed_records:
            self.logger.info("No RVU changes detected since last sync")
            return 0
        
        # Apply changes to PostgreSQL
        changes_applied = await self._apply_rvu_changes(changed_records)
        
        # Record sync completion
        await self._record_sync_completion("incremental_sync", changes_applied)
        
        self.logger.info(f"Incremental sync completed: {changes_applied} changes applied")
        return changes_applied
    
    async def validate_data_integrity(self) -> Dict[str, Any]:
        """Validate that PostgreSQL and SQL Server RVU data are in sync"""
        self.logger.info("Validating RVU data integrity between databases...")
        
        # Get counts and checksums from both databases
        sql_stats = await self._get_sql_server_rvu_stats()
        pg_stats = await self._get_postgres_rvu_stats()
        
        # Compare data integrity
        integrity_report = {
            "sql_server_count": sql_stats["count"],
            "postgresql_count": pg_stats["count"],
            "count_match": sql_stats["count"] == pg_stats["count"],
            "sql_server_checksum": sql_stats["checksum"],
            "postgresql_checksum": pg_stats["checksum"],
            "checksum_match": sql_stats["checksum"] == pg_stats["checksum"],
            "last_validation": datetime.now().isoformat(),
            "sync_required": sql_stats["count"] != pg_stats["count"] or sql_stats["checksum"] != pg_stats["checksum"]
        }
        
        if not integrity_report["sync_required"]:
            self.logger.info("✅ RVU data integrity validation passed")
        else:
            self.logger.warning("⚠️ RVU data integrity validation failed - sync required")
        
        return integrity_report
    
    async def _fetch_sql_server_rvu_data(self) -> List[Dict[str, Any]]:
        """Fetch all active RVU data from SQL Server"""
        query = """
            SELECT 
                procedure_code,
                description,
                category,
                subcategory,
                work_rvu,
                practice_expense_rvu,
                malpractice_rvu,
                total_rvu,
                conversion_factor,
                non_facility_pe_rvu,
                facility_pe_rvu,
                effective_date,
                end_date,
                status,
                global_period,
                professional_component,
                technical_component,
                bilateral_surgery,
                created_at,
                updated_at
            FROM rvu_data 
            WHERE status = 'ACTIVE'
            ORDER BY procedure_code
        """
        
        return await self.sql_db.fetch(query)
    
    async def _fetch_changed_rvu_data(self, since_timestamp: datetime) -> List[Dict[str, Any]]:
        """Fetch RVU records changed since the given timestamp"""
        query = """
            SELECT 
                procedure_code,
                description,
                category,
                subcategory,
                work_rvu,
                practice_expense_rvu,
                malpractice_rvu,
                total_rvu,
                conversion_factor,
                non_facility_pe_rvu,
                facility_pe_rvu,
                effective_date,
                end_date,
                status,
                global_period,
                professional_component,
                technical_component,
                bilateral_surgery,
                created_at,
                updated_at,
                CASE WHEN end_date IS NOT NULL OR status != 'ACTIVE' THEN 'DELETE' ELSE 'UPSERT' END as sync_action
            FROM rvu_data 
            WHERE updated_at > ?
            ORDER BY updated_at, procedure_code
        """
        
        return await self.sql_db.fetch(query, since_timestamp)
    
    async def _bulk_insert_postgres_rvu(self, rvu_data: List[Dict[str, Any]]) -> None:
        """Bulk insert RVU data into PostgreSQL using optimized COPY"""
        
        if not rvu_data:
            return
        
        # Prepare data for PostgreSQL insert
        pg_records = []
        for record in rvu_data:
            pg_record = (
                record['procedure_code'],
                record['description'],
                record['category'],
                record['subcategory'],
                record['work_rvu'],
                record['practice_expense_rvu'],
                record['malpractice_rvu'],
                record['total_rvu'],
                record['conversion_factor'],
                record['non_facility_pe_rvu'],
                record['facility_pe_rvu'],
                record['effective_date'],
                record['end_date'],
                record['status'].lower(),  # PostgreSQL uses lowercase
                record['global_period'],
                bool(record['professional_component']),
                bool(record['technical_component']),
                bool(record['bilateral_surgery']),
                record['created_at'],
                record['updated_at']
            )
            pg_records.append(pg_record)
        
        # Use PostgreSQL COPY for high-performance bulk insert
        copy_query = """
            COPY rvu_data (
                procedure_code, description, category, subcategory,
                work_rvu, practice_expense_rvu, malpractice_rvu, total_rvu,
                conversion_factor, non_facility_pe_rvu, facility_pe_rvu,
                effective_date, end_date, status, global_period,
                professional_component, technical_component, bilateral_surgery,
                created_at, updated_at
            ) FROM STDIN
        """
        
        async with self.pg_db.pool.acquire() as conn:
            await conn.copy_records_to_table(
                'rvu_data',
                records=pg_records,
                columns=[
                    'procedure_code', 'description', 'category', 'subcategory',
                    'work_rvu', 'practice_expense_rvu', 'malpractice_rvu', 'total_rvu',
                    'conversion_factor', 'non_facility_pe_rvu', 'facility_pe_rvu',
                    'effective_date', 'end_date', 'status', 'global_period',
                    'professional_component', 'technical_component', 'bilateral_surgery',
                    'created_at', 'updated_at'
                ]
            )
        
        self.logger.info(f"Bulk inserted {len(pg_records)} RVU records into PostgreSQL")
    
    async def _apply_rvu_changes(self, changes: List[Dict[str, Any]]) -> int:
        """Apply incremental changes to PostgreSQL"""
        
        changes_applied = 0
        
        for change in changes:
            action = change.get('sync_action', 'UPSERT')
            procedure_code = change['procedure_code']
            
            if action == 'DELETE':
                # Remove from PostgreSQL
                await self.pg_db.execute(
                    "DELETE FROM rvu_data WHERE procedure_code = $1",
                    procedure_code
                )
                self.logger.debug(f"Deleted RVU record: {procedure_code}")
                
            else:  # UPSERT
                # Update or insert in PostgreSQL
                upsert_query = """
                    INSERT INTO rvu_data (
                        procedure_code, description, category, subcategory,
                        work_rvu, practice_expense_rvu, malpractice_rvu, total_rvu,
                        conversion_factor, non_facility_pe_rvu, facility_pe_rvu,
                        effective_date, end_date, status, global_period,
                        professional_component, technical_component, bilateral_surgery,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                    ON CONFLICT (procedure_code) DO UPDATE SET
                        description = EXCLUDED.description,
                        category = EXCLUDED.category,
                        subcategory = EXCLUDED.subcategory,
                        work_rvu = EXCLUDED.work_rvu,
                        practice_expense_rvu = EXCLUDED.practice_expense_rvu,
                        malpractice_rvu = EXCLUDED.malpractice_rvu,
                        total_rvu = EXCLUDED.total_rvu,
                        conversion_factor = EXCLUDED.conversion_factor,
                        non_facility_pe_rvu = EXCLUDED.non_facility_pe_rvu,
                        facility_pe_rvu = EXCLUDED.facility_pe_rvu,
                        effective_date = EXCLUDED.effective_date,
                        end_date = EXCLUDED.end_date,
                        status = EXCLUDED.status,
                        global_period = EXCLUDED.global_period,
                        professional_component = EXCLUDED.professional_component,
                        technical_component = EXCLUDED.technical_component,
                        bilateral_surgery = EXCLUDED.bilateral_surgery,
                        updated_at = EXCLUDED.updated_at
                """
                
                await self.pg_db.execute(
                    upsert_query,
                    change['procedure_code'],
                    change['description'],
                    change['category'],
                    change['subcategory'],
                    change['work_rvu'],
                    change['practice_expense_rvu'],
                    change['malpractice_rvu'],
                    change['total_rvu'],
                    change['conversion_factor'],
                    change['non_facility_pe_rvu'],
                    change['facility_pe_rvu'],
                    change['effective_date'],
                    change['end_date'],
                    change['status'].lower(),
                    change['global_period'],
                    bool(change['professional_component']),
                    bool(change['technical_component']),
                    bool(change['bilateral_surgery']),
                    change['created_at'],
                    change['updated_at']
                )
                
                self.logger.debug(f"Upserted RVU record: {procedure_code}")
            
            changes_applied += 1
        
        return changes_applied
    
    async def _get_sql_server_rvu_stats(self) -> Dict[str, Any]:
        """Get count and checksum of SQL Server RVU data"""
        query = """
            SELECT 
                COUNT(*) as rvu_count,
                CHECKSUM_AGG(CHECKSUM(*)) as data_checksum
            FROM rvu_data 
            WHERE status = 'ACTIVE'
        """
        
        result = await self.sql_db.fetch(query)
        return {
            "count": result[0]['rvu_count'],
            "checksum": str(result[0]['data_checksum'])
        }
    
    async def _get_postgres_rvu_stats(self) -> Dict[str, Any]:
        """Get count and checksum of PostgreSQL RVU data"""
        # PostgreSQL doesn't have CHECKSUM_AGG, so we'll create a hash
        query = """
            SELECT 
                COUNT(*) as rvu_count,
                string_agg(
                    md5(procedure_code || COALESCE(description, '') || COALESCE(total_rvu::text, '')), 
                    '' ORDER BY procedure_code
                ) as concatenated_hashes
            FROM rvu_data 
            WHERE status = 'active'
        """
        
        result = await self.pg_db.fetch(query)
        
        # Create a final hash of all concatenated hashes
        concat_hash = result[0]['concatenated_hashes'] or ''
        final_checksum = hashlib.md5(concat_hash.encode()).hexdigest()
        
        return {
            "count": result[0]['rvu_count'],
            "checksum": final_checksum
        }
    
    async def _get_last_sync_timestamp(self) -> datetime:
        """Get the timestamp of the last successful sync"""
        query = """
            SELECT MAX(sync_completed_at) as last_sync
            FROM rvu_sync_log 
            WHERE sync_status = 'completed'
        """
        
        try:
            result = await self.pg_db.fetch(query)
            last_sync = result[0]['last_sync'] if result and result[0]['last_sync'] else None
            
            if last_sync:
                return last_sync
            else:
                # If no previous sync, start from 30 days ago
                return datetime.now() - timedelta(days=30)
                
        except Exception:
            # If sync log table doesn't exist, start from 30 days ago
            return datetime.now() - timedelta(days=30)
    
    async def _record_sync_completion(self, sync_type: str, records_processed: int) -> None:
        """Record successful sync completion"""
        
        # Ensure sync log table exists
        await self._ensure_sync_log_table()
        
        insert_query = """
            INSERT INTO rvu_sync_log (
                sync_type, 
                sync_started_at, 
                sync_completed_at, 
                records_processed, 
                sync_status
            ) VALUES ($1, $2, $3, $4, $5)
        """
        
        now = datetime.now()
        await self.pg_db.execute(
            insert_query,
            sync_type,
            now,
            now,
            records_processed,
            'completed'
        )
    
    async def _ensure_sync_log_table(self) -> None:
        """Ensure the RVU sync log table exists"""
        create_table_query = """
            CREATE TABLE IF NOT EXISTS rvu_sync_log (
                sync_id SERIAL PRIMARY KEY,
                sync_type VARCHAR(50) NOT NULL,
                sync_started_at TIMESTAMPTZ NOT NULL,
                sync_completed_at TIMESTAMPTZ,
                records_processed INTEGER DEFAULT 0,
                sync_status VARCHAR(20) NOT NULL,
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
        """
        
        await self.pg_db.execute(create_table_query)


# Updated setup_reference_data.py to include RVU sync
async def setup_reference_data_with_rvu_sync():
    """
    Updated reference data setup with PostgreSQL RVU sync
    """
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting reference data setup with RVU synchronization...")
    
    # Load configuration
    config = load_config()
    
    # Initialize database connections
    pg_db = PostgresDatabase(config.postgres)
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        # Connect without preparing RVU queries initially
        await pg_db.connect(prepare_queries=False)
        await sql_db.connect()
        logger.info("Connected to both PostgreSQL and SQL Server databases")
        
        # Setup organizational hierarchy in SQL Server
        await setup_organizations(sql_db, logger)
        await setup_regions(sql_db, logger)  
        await setup_facilities_with_hierarchy(sql_db, logger)
        
        # Setup financial classes
        await setup_financial_classes_with_facility_rates(sql_db, logger)
        await create_financial_rate_calculation_function(sql_db, logger)
        
        # Setup RVU data in SQL Server (master source)
        await setup_rvu_data_sql_server_only(sql_db, logger)
        
        # Initialize RVU sync system and perform initial sync
        rvu_sync = RVUDataSync(pg_db, sql_db)
        await rvu_sync.initial_sync()
        
        # Now prepare RVU queries since PostgreSQL has the data
        await pg_db.prepare_common_queries()
        
        # Validate data integrity
        integrity_report = await rvu_sync.validate_data_integrity()
        logger.info(f"RVU data integrity: {integrity_report}")
        
        logger.info("Reference data setup with RVU sync completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during reference data setup: {e}")
        raise
    finally:
        await pg_db.close()
        await sql_db.close()
        logger.info("Database connections closed")


# Standalone RVU sync script
async def sync_rvu_data():
    """Standalone script to sync RVU data"""
    
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    config = load_config()
    pg_db = PostgresDatabase(config.postgres)
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        await pg_db.connect()
        await sql_db.connect()
        
        rvu_sync = RVUDataSync(pg_db, sql_db)
        
        # Perform incremental sync
        changes = await rvu_sync.incremental_sync()
        
        # Validate integrity
        integrity = await rvu_sync.validate_data_integrity()
        
        logger.info(f"Sync completed: {changes} changes, integrity: {integrity['sync_required']}")
        
    finally:
        await pg_db.close()
        await sql_db.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "sync":
        # Run incremental sync
        asyncio.run(sync_rvu_data())
    else:
        # Run full setup
        asyncio.run(setup_reference_data_with_rvu_sync())