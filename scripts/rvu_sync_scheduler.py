#!/usr/bin/env python3
"""
RVU Data Sync Scheduler
Provides scheduled synchronization and monitoring services
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import json

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


class RVUSyncScheduler:
    """
    Scheduled RVU synchronization service
    
    Features:
    - Configurable sync intervals
    - Health monitoring
    - Error handling and recovery
    - Performance metrics
    """
    
    def __init__(self, sync_interval_minutes: int = 60):
        self.sync_interval = sync_interval_minutes
        self.logger = logging.getLogger(__name__)
        self.is_running = False
        self.last_sync_time: Optional[datetime] = None
        self.sync_stats = {
            "total_syncs": 0,
            "successful_syncs": 0,
            "failed_syncs": 0,
            "last_error": None,
            "average_sync_time": 0.0
        }
    
    async def start_scheduler(self) -> None:
        """Start the scheduled sync service"""
        self.logger.info(f"Starting RVU sync scheduler (interval: {self.sync_interval} minutes)")
        self.is_running = True
        
        while self.is_running:
            try:
                await self._perform_sync_cycle()
                
                # Wait for next sync interval
                await asyncio.sleep(self.sync_interval * 60)
                
            except asyncio.CancelledError:
                self.logger.info("RVU sync scheduler cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in sync scheduler: {e}")
                self.sync_stats["failed_syncs"] += 1
                self.sync_stats["last_error"] = str(e)
                
                # Wait before retrying (shorter interval on error)
                await asyncio.sleep(300)  # 5 minutes
    
    async def stop_scheduler(self) -> None:
        """Stop the scheduled sync service"""
        self.logger.info("Stopping RVU sync scheduler")
        self.is_running = False
    
    async def force_sync(self) -> Dict[str, Any]:
        """Force an immediate sync and return results"""
        self.logger.info("Forcing immediate RVU sync")
        return await self._perform_sync_cycle()
    
    async def get_sync_status(self) -> Dict[str, Any]:
        """Get current sync status and statistics"""
        return {
            "is_running": self.is_running,
            "sync_interval_minutes": self.sync_interval,
            "last_sync_time": self.last_sync_time.isoformat() if self.last_sync_time else None,
            "next_sync_time": (self.last_sync_time + timedelta(minutes=self.sync_interval)).isoformat() 
                             if self.last_sync_time else None,
            "stats": self.sync_stats.copy()
        }
    
    async def _perform_sync_cycle(self) -> Dict[str, Any]:
        """Perform a complete sync cycle"""
        start_time = datetime.now()
        sync_result = {
            "sync_started": start_time.isoformat(),
            "sync_completed": None,
            "duration_seconds": 0,
            "changes_applied": 0,
            "integrity_status": {},
            "status": "failed",
            "error": None
        }
        
        config = load_config()
        pg_db = PostgresDatabase(config.postgres)
        sql_db = SQLServerDatabase(config.sqlserver)
        
        try:
            # Connect to databases
            await pg_db.connect()
            await sql_db.connect()
            
            # Import here to avoid circular imports
            from .rvu_sync_system import RVUDataSync
            rvu_sync = RVUDataSync(pg_db, sql_db)
            
            # Perform incremental sync
            changes_applied = await rvu_sync.incremental_sync()
            sync_result["changes_applied"] = changes_applied
            
            # Validate data integrity
            integrity_status = await rvu_sync.validate_data_integrity()
            sync_result["integrity_status"] = integrity_status
            
            # Update timing and status
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            sync_result.update({
                "sync_completed": end_time.isoformat(),
                "duration_seconds": duration,
                "status": "success"
            })
            
            # Update statistics
            self.sync_stats["total_syncs"] += 1
            self.sync_stats["successful_syncs"] += 1
            self.sync_stats["average_sync_time"] = (
                (self.sync_stats["average_sync_time"] * (self.sync_stats["successful_syncs"] - 1) + duration) 
                / self.sync_stats["successful_syncs"]
            )
            self.last_sync_time = end_time
            
            self.logger.info(f"RVU sync completed: {changes_applied} changes in {duration:.2f}s")
            
        except Exception as e:
            error_msg = str(e)
            sync_result["error"] = error_msg
            self.sync_stats["failed_syncs"] += 1
            self.sync_stats["last_error"] = error_msg
            self.logger.error(f"RVU sync failed: {error_msg}")
            
        finally:
            await pg_db.close()
            await sql_db.close()
        
        return sync_result


class RVUManagementService:
    """
    High-level RVU data management service
    
    Provides:
    - Manual RVU updates
    - Data validation
    - Performance monitoring
    - Emergency sync procedures
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def update_single_rvu(self, procedure_code: str, rvu_data: Dict[str, Any]) -> bool:
        """Update a single RVU record in both databases"""
        config = load_config()
        pg_db = PostgresDatabase(config.postgres)
        sql_db = SQLServerDatabase(config.sqlserver)
        
        try:
            await pg_db.connect()
            await sql_db.connect()
            
            # Update SQL Server first (master)
            sql_update = """
                UPDATE rvu_data SET
                    description = ?,
                    total_rvu = ?,
                    work_rvu = ?,
                    practice_expense_rvu = ?,
                    malpractice_rvu = ?,
                    conversion_factor = ?,
                    updated_at = GETDATE()
                WHERE procedure_code = ?
            """
            
            await sql_db.execute(
                sql_update,
                rvu_data.get("description"),
                rvu_data.get("total_rvu"),
                rvu_data.get("work_rvu"),
                rvu_data.get("practice_expense_rvu"),
                rvu_data.get("malpractice_rvu"),
                rvu_data.get("conversion_factor"),
                procedure_code
            )
            
            # Update PostgreSQL cache
            pg_update = """
                UPDATE rvu_data SET
                    description = $1,
                    total_rvu = $2,
                    work_rvu = $3,
                    practice_expense_rvu = $4,
                    malpractice_rvu = $5,
                    conversion_factor = $6,
                    updated_at = CURRENT_TIMESTAMP
                WHERE procedure_code = $7
            """
            
            await pg_db.execute(
                pg_update,
                rvu_data.get("description"),
                rvu_data.get("total_rvu"),
                rvu_data.get("work_rvu"),
                rvu_data.get("practice_expense_rvu"),
                rvu_data.get("malpractice_rvu"),
                rvu_data.get("conversion_factor"),
                procedure_code
            )
            
            self.logger.info(f"Updated RVU data for procedure code: {procedure_code}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update RVU for {procedure_code}: {e}")
            return False
            
        finally:
            await pg_db.close()
            await sql_db.close()
    
    async def bulk_update_rvus(self, rvu_updates: List[Dict[str, Any]]) -> Dict[str, int]:
        """Bulk update multiple RVU records"""
        results = {"successful": 0, "failed": 0}
        
        for rvu_update in rvu_updates:
            procedure_code = rvu_update.get("procedure_code")
            if not procedure_code:
                results["failed"] += 1
                continue
            
            success = await self.update_single_rvu(procedure_code, rvu_update)
            if success:
                results["successful"] += 1
            else:
                results["failed"] += 1
        
        self.logger.info(f"Bulk RVU update completed: {results}")
        return results
    
    async def emergency_resync(self) -> Dict[str, Any]:
        """Emergency full resync of all RVU data"""
        self.logger.warning("Starting emergency RVU resync")
        
        config = load_config()
        pg_db = PostgresDatabase(config.postgres)
        sql_db = SQLServerDatabase(config.sqlserver)
        
        try:
            await pg_db.connect()
            await sql_db.connect()
            
            from .rvu_sync_system import RVUDataSync
            rvu_sync = RVUDataSync(pg_db, sql_db)
            
            # Perform full resync
            await rvu_sync.initial_sync()
            
            # Validate integrity
            integrity_report = await rvu_sync.validate_data_integrity()
            
            result = {
                "status": "completed",
                "integrity_report": integrity_report,
                "timestamp": datetime.now().isoformat()
            }
            
            self.logger.info("Emergency RVU resync completed successfully")
            return result
            
        except Exception as e:
            self.logger.error(f"Emergency resync failed: {e}")
            return {
                "status": "failed",
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
            
        finally:
            await pg_db.close()
            await sql_db.close()


# Configuration for RVU sync service
class RVUSyncConfig:
    """Configuration for RVU synchronization"""
    
    DEFAULT_SYNC_INTERVAL_MINUTES = 60  # Sync every hour
    EMERGENCY_SYNC_INTERVAL_MINUTES = 5  # Emergency sync interval
    MAX_RETRY_ATTEMPTS = 3
    SYNC_TIMEOUT_SECONDS = 300  # 5 minutes
    
    @staticmethod
    def load_from_config() -> Dict[str, Any]:
        """Load RVU sync configuration from main config"""
        config = load_config()
        
        # Check if RVU sync config exists in main config
        rvu_config = getattr(config, 'rvu_sync', {})
        
        return {
            "sync_interval_minutes": rvu_config.get("sync_interval_minutes", RVUSyncConfig.DEFAULT_SYNC_INTERVAL_MINUTES),
            "auto_start": rvu_config.get("auto_start", True),
            "enable_monitoring": rvu_config.get("enable_monitoring", True),
            "integrity_check_interval": rvu_config.get("integrity_check_interval", 24),  # hours
            "max_sync_failures": rvu_config.get("max_sync_failures", 5)
        }


# CLI interface for RVU sync management
async def main():
    """Command-line interface for RVU sync operations"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python rvu_sync_scheduler.py [sync|status|emergency|schedule]")
        return
    
    command = sys.argv[1].lower()
    
    if command == "sync":
        # Perform one-time sync
        from .rvu_sync_system import sync_rvu_data
        await sync_rvu_data()
        
    elif command == "status":
        # Show sync status
        scheduler = RVUSyncScheduler()
        status = await scheduler.get_sync_status()
        print(json.dumps(status, indent=2))
        
    elif command == "emergency":
        # Emergency resync
        service = RVUManagementService()
        result = await service.emergency_resync()
        print(json.dumps(result, indent=2))
        
    elif command == "schedule":
        # Start scheduler
        config = RVUSyncConfig.load_from_config()
        scheduler = RVUSyncScheduler(config["sync_interval_minutes"])
        
        try:
            await scheduler.start_scheduler()
        except KeyboardInterrupt:
            await scheduler.stop_scheduler()
            print("RVU sync scheduler stopped")
    
    else:
        print(f"Unknown command: {command}")


if __name__ == "__main__":
    asyncio.run(main())