#!/usr/bin/env python3
"""
RVU Data Synchronization System
Handles synchronization of RVU data between SQL Server (master) and PostgreSQL (cache)
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from decimal import Decimal


class RVUDataSync:
    """
    RVU Data Synchronization Manager
    Synchronizes RVU data from SQL Server (authoritative source) to PostgreSQL (cache)
    """
    
    def __init__(self, pg_db, sql_db):
        """
        Initialize RVU sync system
        
        Args:
            pg_db: PostgreSQL database instance
            sql_db: SQL Server database instance
        """
        self.pg_db = pg_db
        self.sql_db = sql_db
        self.logger = logging.getLogger(__name__)
        
    async def initial_sync(self) -> Dict[str, Any]:
        """
        Perform initial synchronization of RVU data from SQL Server to PostgreSQL
        
        Returns:
            Dict containing sync results and statistics
        """
        self.logger.info("Starting initial RVU data synchronization...")
        
        sync_start_time = datetime.now()
        
        try:
            # Start sync log entry
            sync_id = await self._start_sync_log("initial_sync")
            
            # Fetch all RVU data from SQL Server
            sql_server_data = await self._fetch_rvu_data_from_sql_server()
            
            if not sql_server_data:
                self.logger.warning("No RVU data found in SQL Server")
                await self._complete_sync_log(sync_id, 0, "completed", "No data found in SQL Server")
                return {
                    "success": True,
                    "records_synced": 0,
                    "message": "No RVU data found in SQL Server"
                }
            
            # Clear existing PostgreSQL data
            await self._clear_postgresql_rvu_data()
            
            # Insert data into PostgreSQL
            records_synced = await self._insert_rvu_data_to_postgresql(sql_server_data)
            
            # Complete sync log
            await self._complete_sync_log(sync_id, records_synced, "completed")
            
            sync_duration = (datetime.now() - sync_start_time).total_seconds()
            
            self.logger.info(f"Initial RVU sync completed successfully")
            self.logger.info(f"Records synced: {records_synced}")
            self.logger.info(f"Duration: {sync_duration:.2f} seconds")
            
            return {
                "success": True,
                "records_synced": records_synced,
                "duration_seconds": sync_duration,
                "sql_server_count": len(sql_server_data),
                "postgresql_count": records_synced
            }
            
        except Exception as e:
            self.logger.error(f"Initial RVU sync failed: {e}")
            try:
                await self._complete_sync_log(sync_id, 0, "failed", str(e))
            except:
                pass  # Don't fail on logging failure
            
            return {
                "success": False,
                "error": str(e),
                "records_synced": 0
            }
    
    async def validate_data_integrity(self) -> Dict[str, Any]:
        """
        Validate data integrity between SQL Server and PostgreSQL
        
        Returns:
            Dict containing validation results
        """
        self.logger.info("Validating RVU data integrity between databases...")
        
        try:
            # Get record counts from both databases
            sql_server_count = await self._get_sql_server_rvu_count()
            postgresql_count = await self._get_postgresql_rvu_count()
            
            # Get sample of data from both databases for comparison
            sql_server_sample = await self._get_sql_server_rvu_sample()
            postgresql_sample = await self._get_postgresql_rvu_sample()
            
            # Check if sync is required
            sync_required = sql_server_count != postgresql_count
            
            if sync_required:
                self.logger.warning(f"Data integrity check failed: SQL Server has {sql_server_count} records, PostgreSQL has {postgresql_count}")
            else:
                self.logger.info(f"Data integrity check passed: Both databases have {sql_server_count} records")
            
            # Compare sample data
            data_match = self._compare_sample_data(sql_server_sample, postgresql_sample)
            
            return {
                "sync_required": sync_required,
                "sql_server_count": sql_server_count,
                "postgresql_count": postgresql_count,
                "data_match": data_match,
                "last_validated": datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Data integrity validation failed: {e}")
            return {
                "sync_required": True,
                "error": str(e),
                "sql_server_count": 0,
                "postgresql_count": 0,
                "data_match": False
            }
    
    async def incremental_sync(self) -> Dict[str, Any]:
        """
        Perform incremental synchronization of updated RVU data
        
        Returns:
            Dict containing sync results
        """
        self.logger.info("Starting incremental RVU data synchronization...")
        
        try:
            sync_id = await self._start_sync_log("incremental_sync")
            
            # Get last sync timestamp
            last_sync_time = await self._get_last_sync_time()
            
            # Fetch updated records from SQL Server
            updated_records = await self._fetch_updated_rvu_data(last_sync_time)
            
            if not updated_records:
                self.logger.info("No updated RVU records found")
                await self._complete_sync_log(sync_id, 0, "completed", "No updates found")
                return {"success": True, "records_synced": 0, "message": "No updates found"}
            
            # Update PostgreSQL with changed records
            records_synced = await self._update_postgresql_rvu_data(updated_records)
            
            await self._complete_sync_log(sync_id, records_synced, "completed")
            
            self.logger.info(f"Incremental sync completed: {records_synced} records updated")
            
            return {
                "success": True,
                "records_synced": records_synced,
                "last_sync_time": last_sync_time
            }
            
        except Exception as e:
            self.logger.error(f"Incremental RVU sync failed: {e}")
            return {"success": False, "error": str(e), "records_synced": 0}
    
    # Private helper methods
    
    async def _fetch_rvu_data_from_sql_server(self) -> List[Dict[str, Any]]:
        """Fetch all RVU data from SQL Server"""
        query = """
            SELECT 
                procedure_code, description, category, subcategory,
                work_rvu, practice_expense_rvu, malpractice_rvu, total_rvu,
                conversion_factor, non_facility_pe_rvu, facility_pe_rvu,
                effective_date, end_date, status, global_period,
                professional_component, technical_component, bilateral_surgery,
                created_at, updated_at
            FROM rvu_data 
            WHERE status = 'ACTIVE'
            ORDER BY procedure_code
        """
        
        try:
            rows = await self.sql_db.fetch(query)
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            self.logger.warning(f"Error fetching RVU data from SQL Server: {e}")
            return []
    
    async def _clear_postgresql_rvu_data(self) -> None:
        """Clear existing RVU data from PostgreSQL"""
        try:
            await self.pg_db.execute("DELETE FROM rvu_data")
            self.logger.debug("Cleared existing RVU data from PostgreSQL")
        except Exception as e:
            self.logger.warning(f"Error clearing PostgreSQL RVU data: {e}")
            # Don't fail on this - table might not exist yet
    
    async def _insert_rvu_data_to_postgresql(self, rvu_data: List[Dict[str, Any]]) -> int:
        """Insert RVU data into PostgreSQL in batches"""
        if not rvu_data:
            return 0
        
        insert_query = """
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
        
        records_inserted = 0
        batch_size = 1000
        
        for i in range(0, len(rvu_data), batch_size):
            batch = rvu_data[i:i + batch_size]
            
            for record in batch:
                try:
                    # Convert values and handle None fields
                    values = (
                        record.get('procedure_code'),
                        record.get('description'),
                        record.get('category'),
                        record.get('subcategory'),
                        self._safe_decimal(record.get('work_rvu')),
                        self._safe_decimal(record.get('practice_expense_rvu')),
                        self._safe_decimal(record.get('malpractice_rvu')),
                        self._safe_decimal(record.get('total_rvu')),
                        self._safe_decimal(record.get('conversion_factor')),
                        self._safe_decimal(record.get('non_facility_pe_rvu')),
                        self._safe_decimal(record.get('facility_pe_rvu')),
                        record.get('effective_date'),
                        record.get('end_date'),
                        record.get('status', 'active'),
                        record.get('global_period'),
                        record.get('professional_component', False),
                        record.get('technical_component', False),
                        record.get('bilateral_surgery', False),
                        record.get('created_at', datetime.now()),
                        record.get('updated_at', datetime.now())
                    )
                    
                    await self.pg_db.execute(insert_query, *values)
                    records_inserted += 1
                    
                except Exception as e:
                    self.logger.warning(f"Error inserting RVU record {record.get('procedure_code', 'unknown')}: {e}")
                    continue
            
            if i > 0 and i % (batch_size * 5) == 0:
                self.logger.debug(f"Inserted {records_inserted} RVU records so far...")
        
        return records_inserted
    
    async def _get_sql_server_rvu_count(self) -> int:
        """Get count of active RVU records in SQL Server"""
        try:
            result = await self.sql_db.fetch("SELECT COUNT(*) as count FROM rvu_data WHERE status = 'ACTIVE'")
            return result[0]['count'] if result else 0
        except Exception as e:
            self.logger.warning(f"Error getting SQL Server RVU count: {e}")
            return 0
    
    async def _get_postgresql_rvu_count(self) -> int:
        """Get count of active RVU records in PostgreSQL"""
        try:
            result = await self.pg_db.fetch("SELECT COUNT(*) as count FROM rvu_data WHERE status = 'active'")
            return result[0]['count'] if result else 0
        except Exception as e:
            self.logger.warning(f"Error getting PostgreSQL RVU count: {e}")
            return 0
    
    async def _get_sql_server_rvu_sample(self) -> List[Dict[str, Any]]:
        """Get sample RVU data from SQL Server for comparison"""
        try:
            query = """
                SELECT TOP 10 procedure_code, total_rvu, status
                FROM rvu_data 
                WHERE status = 'ACTIVE'
                ORDER BY procedure_code
            """
            rows = await self.sql_db.fetch(query)
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            self.logger.warning(f"Error getting SQL Server RVU sample: {e}")
            return []
    
    async def _get_postgresql_rvu_sample(self) -> List[Dict[str, Any]]:
        """Get sample RVU data from PostgreSQL for comparison"""
        try:
            query = """
                SELECT procedure_code, total_rvu, status
                FROM rvu_data 
                WHERE status = 'active'
                ORDER BY procedure_code
                LIMIT 10
            """
            rows = await self.pg_db.fetch(query)
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            self.logger.warning(f"Error getting PostgreSQL RVU sample: {e}")
            return []
    
    def _compare_sample_data(self, sql_sample: List[Dict], pg_sample: List[Dict]) -> bool:
        """Compare sample data between databases"""
        if len(sql_sample) != len(pg_sample):
            return False
        
        for sql_row, pg_row in zip(sql_sample, pg_sample):
            if (sql_row.get('procedure_code') != pg_row.get('procedure_code') or
                abs(float(sql_row.get('total_rvu', 0)) - float(pg_row.get('total_rvu', 0))) > 0.01):
                return False
        
        return True
    
    async def _start_sync_log(self, sync_type: str) -> int:
        """Start a sync log entry and return sync_id"""
        try:
            query = """
                INSERT INTO rvu_sync_log (sync_type, sync_started_at, sync_status, created_at)
                VALUES ($1, $2, $3, $4)
                RETURNING sync_id
            """
            result = await self.pg_db.fetch(query, sync_type, datetime.now(), 'started', datetime.now())
            return result[0]['sync_id'] if result else 0
        except Exception as e:
            self.logger.warning(f"Error starting sync log: {e}")
            return 0
    
    async def _complete_sync_log(self, sync_id: int, records_processed: int, status: str, error_message: str = None) -> None:
        """Complete a sync log entry"""
        try:
            query = """
                UPDATE rvu_sync_log 
                SET sync_completed_at = $1, records_processed = $2, sync_status = $3, error_message = $4
                WHERE sync_id = $5
            """
            await self.pg_db.execute(query, datetime.now(), records_processed, status, error_message, sync_id)
        except Exception as e:
            self.logger.warning(f"Error completing sync log: {e}")
    
    async def _get_last_sync_time(self) -> Optional[datetime]:
        """Get timestamp of last successful sync"""
        try:
            query = """
                SELECT MAX(sync_completed_at) as last_sync
                FROM rvu_sync_log 
                WHERE sync_status = 'completed'
            """
            result = await self.pg_db.fetch(query)
            return result[0]['last_sync'] if result and result[0]['last_sync'] else None
        except Exception as e:
            self.logger.warning(f"Error getting last sync time: {e}")
            return None
    
    async def _fetch_updated_rvu_data(self, since: Optional[datetime]) -> List[Dict[str, Any]]:
        """Fetch RVU data updated since given timestamp"""
        if not since:
            # If no timestamp, fetch all data
            return await self._fetch_rvu_data_from_sql_server()
        
        try:
            query = """
                SELECT 
                    procedure_code, description, category, subcategory,
                    work_rvu, practice_expense_rvu, malpractice_rvu, total_rvu,
                    conversion_factor, non_facility_pe_rvu, facility_pe_rvu,
                    effective_date, end_date, status, global_period,
                    professional_component, technical_component, bilateral_surgery,
                    created_at, updated_at
                FROM rvu_data 
                WHERE status = 'ACTIVE' AND updated_at > ?
                ORDER BY procedure_code
            """
            rows = await self.sql_db.fetch(query, since)
            return [dict(row) for row in rows] if rows else []
        except Exception as e:
            self.logger.warning(f"Error fetching updated RVU data: {e}")
            return []
    
    async def _update_postgresql_rvu_data(self, updated_records: List[Dict[str, Any]]) -> int:
        """Update PostgreSQL with changed RVU records"""
        # For now, just use the same insert method which handles upserts
        return await self._insert_rvu_data_to_postgresql(updated_records)
    
    def _safe_decimal(self, value: Any) -> Optional[Decimal]:
        """Safely convert value to Decimal"""
        if value is None:
            return None
        try:
            return Decimal(str(value))
        except (ValueError, TypeError):
            return None


async def main():
    """Main function for standalone testing"""
    import logging
    from src.config.config import load_config
    from src.db.postgres import PostgresDatabase
    from src.db.sql_server import SQLServerDatabase
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Testing RVU sync system...")
    
    # Load configuration
    config = load_config()
    
    # Initialize databases
    pg_db = PostgresDatabase(config.postgres)
    sql_db = SQLServerDatabase(config.sqlserver)
    
    try:
        await pg_db.connect(prepare_queries=False)
        await sql_db.connect()
        
        # Create sync manager and test
        rvu_sync = RVUDataSync(pg_db, sql_db)
        
        # Test initial sync
        result = await rvu_sync.initial_sync()
        logger.info(f"Initial sync result: {result}")
        
        # Test validation
        validation = await rvu_sync.validate_data_integrity()
        logger.info(f"Validation result: {validation}")
        
    except Exception as e:
        logger.error(f"Error testing RVU sync: {e}")
    finally:
        try:
            await pg_db.close()
            await sql_db.close()
        except:
            pass


if __name__ == "__main__":
    asyncio.run(main())