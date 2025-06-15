#!/usr/bin/env python3
"""
RVU Integration Service
Integrates RVU sync system with your main claims processing application
"""

import asyncio
import logging
from typing import Optional
from datetime import datetime, timedelta

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


class RVUIntegrationService:
    """
    RVU Integration Service for Claims Processing System
    
    Features:
    - Auto-starts with main application
    - Background sync scheduling
    - Health monitoring integration
    - Performance metrics integration
    """
    
    def __init__(self):
        self.config = load_config()
        self.logger = logging.getLogger(__name__)
        self.scheduler_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.rvu_config = getattr(self.config, 'rvu_sync', {})
    
    async def start(self) -> None:
        """Start the RVU integration service"""
        if self.rvu_config.get('auto_start', True):
            self.logger.info("🚀 Starting RVU synchronization service...")
            
            # Perform initial integrity check
            await self._initial_integrity_check()
            
            # Start background scheduler if enabled
            if self.rvu_config.get('enable_monitoring', True):
                await self._start_background_scheduler()
            
            self.is_running = True
            self.logger.info("✅ RVU integration service started successfully")
        else:
            self.logger.info("⏭️  RVU auto-start disabled in configuration")
    
    async def stop(self) -> None:
        """Stop the RVU integration service"""
        self.logger.info("🛑 Stopping RVU synchronization service...")
        
        if self.scheduler_task and not self.scheduler_task.done():
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        self.is_running = False
        self.logger.info("✅ RVU integration service stopped")
    
    async def get_health_status(self) -> dict:
        """Get health status for monitoring dashboards"""
        try:
            config = load_config()
            pg_db = PostgresDatabase(config.postgres)
            sql_db = SQLServerDatabase(config.sqlserver)
            
            async with pg_db, sql_db:
                await pg_db.connect()
                await sql_db.connect()
                
                from scripts.rvu_sync_system import RVUDataSync
                rvu_sync = RVUDataSync(pg_db, sql_db)
                
                integrity = await rvu_sync.validate_data_integrity()
                
                return {
                    "service_running": self.is_running,
                    "data_integrity": integrity,
                    "last_check": datetime.now().isoformat(),
                    "status": "healthy" if not integrity.get("sync_required", True) else "needs_attention"
                }
                
        except Exception as e:
            return {
                "service_running": self.is_running,
                "error": str(e),
                "last_check": datetime.now().isoformat(),
                "status": "error"
            }
    
    async def force_sync_if_needed(self) -> dict:
        """Force sync if data integrity issues detected"""
        try:
            config = load_config()
            pg_db = PostgresDatabase(config.postgres)
            sql_db = SQLServerDatabase(config.sqlserver)
            
            async with pg_db, sql_db:
                await pg_db.connect()
                await sql_db.connect()
                
                from scripts.rvu_sync_system import RVUDataSync
                rvu_sync = RVUDataSync(pg_db, sql_db)
                
                # Check integrity
                integrity = await rvu_sync.validate_data_integrity()
                
                if integrity.get("sync_required", False):
                    self.logger.warning("⚠️ RVU data integrity issues detected, forcing sync...")
                    
                    # Perform incremental sync first
                    changes = await rvu_sync.incremental_sync()
                    
                    # Re-check integrity
                    post_sync_integrity = await rvu_sync.validate_data_integrity()
                    
                    return {
                        "sync_performed": True,
                        "changes_applied": changes,
                        "pre_sync_integrity": integrity,
                        "post_sync_integrity": post_sync_integrity,
                        "timestamp": datetime.now().isoformat()
                    }
                else:
                    return {
                        "sync_performed": False,
                        "reason": "No sync required - data integrity OK",
                        "integrity": integrity,
                        "timestamp": datetime.now().isoformat()
                    }
                    
        except Exception as e:
            self.logger.error(f"❌ Force sync failed: {e}")
            return {
                "sync_performed": False,
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
    
    async def _initial_integrity_check(self) -> None:
        """Perform initial integrity check on startup"""
        self.logger.info("🔍 Performing initial RVU data integrity check...")
        
        try:
            config = load_config()
            pg_db = PostgresDatabase(config.postgres)
            sql_db = SQLServerDatabase(config.sqlserver)
            
            async with pg_db, sql_db:
                await pg_db.connect()
                await sql_db.connect()
                
                from scripts.rvu_sync_system import RVUDataSync
                rvu_sync = RVUDataSync(pg_db, sql_db)
                
                integrity = await rvu_sync.validate_data_integrity()
                
                if integrity.get("sync_required", False):
                    self.logger.warning("⚠️ RVU data integrity issues detected on startup")
                    
                    if self.rvu_config.get('emergency_resync_enabled', True):
                        self.logger.info("🔄 Performing emergency sync...")
                        await rvu_sync.incremental_sync()
                        self.logger.info("✅ Emergency sync completed")
                    else:
                        self.logger.warning("⚠️ Emergency resync disabled - manual intervention required")
                else:
                    self.logger.info("✅ RVU data integrity check passed")
                    
        except Exception as e:
            self.logger.error(f"❌ Initial integrity check failed: {e}")
    
    async def _start_background_scheduler(self) -> None:
        """Start background sync scheduler"""
        from scripts.rvu_sync_scheduler import RVUSyncScheduler
        
        sync_interval = self.rvu_config.get('sync_interval_minutes', 60)
        scheduler = RVUSyncScheduler(sync_interval)
        
        self.scheduler_task = asyncio.create_task(scheduler.start_scheduler())
        self.logger.info(f"⏰ Background RVU sync scheduler started (interval: {sync_interval} minutes)")


# Integration with FastAPI application
class RVUWebIntegration:
    """Web endpoints for RVU management in FastAPI app"""
    
    def __init__(self, app):
        self.app = app
        self.integration_service = RVUIntegrationService()
        self._setup_routes()
    
    def _setup_routes(self):
        """Setup FastAPI routes for RVU management"""
        
        @self.app.get("/rvu/health")
        async def rvu_health():
            """RVU system health endpoint"""
            return await self.integration_service.get_health_status()
        
        @self.app.post("/rvu/sync")
        async def force_rvu_sync():
            """Force RVU synchronization"""
            return await self.integration_service.force_sync_if_needed()
        
        @self.app.get("/rvu/status")
        async def rvu_status():
            """Get detailed RVU status"""
            try:
                from scripts.rvu_manager import RVUManager
                async with RVUManager() as manager:
                    return await manager.show_status()
            except Exception as e:
                return {"error": str(e), "status": "failed"}
        
        @self.app.get("/rvu/stats")
        async def rvu_stats():
            """Get RVU statistics"""
            try:
                from scripts.rvu_manager import RVUManager
                async with RVUManager() as manager:
                    return await manager.show_stats()
            except Exception as e:
                return {"error": str(e), "status": "failed"}


# Integration with main processing application
async def integrate_with_main_app():
    """
    Integration function to add to your main application startup
    
    Add this to your main processing application's startup routine:
    
    ```python
    from scripts.rvu_integration import integrate_with_main_app
    
    async def main():
        # Your existing startup code...
        
        # Add RVU integration
        await integrate_with_main_app()
        
        # Continue with your processing...
    ```
    """
    
    integration_service = RVUIntegrationService()
    
    try:
        await integration_service.start()
        
        # Return the service instance for shutdown handling
        return integration_service
        
    except Exception as e:
        logging.error(f"❌ Failed to start RVU integration: {e}")
        raise


# Example usage in your main application
class ExampleMainApp:
    """Example of how to integrate with your main claims processing app"""
    
    def __init__(self):
        self.rvu_service: Optional[RVUIntegrationService] = None
        self.logger = logging.getLogger(__name__)
    
    async def startup(self):
        """Application startup with RVU integration"""
        self.logger.info("🚀 Starting claims processing application...")
        
        # Start RVU integration service
        self.rvu_service = await integrate_with_main_app()
        
        # Your existing startup code here...
        self.logger.info("✅ Application startup completed")
    
    async def shutdown(self):
        """Application shutdown with RVU cleanup"""
        self.logger.info("🛑 Shutting down claims processing application...")
        
        # Stop RVU service
        if self.rvu_service:
            await self.rvu_service.stop()
        
        # Your existing shutdown code here...
        self.logger.info("✅ Application shutdown completed")
    
    async def get_system_health(self):
        """Get comprehensive system health including RVU status"""
        health = {
            "application": "healthy",  # Your app health logic
            "timestamp": datetime.now().isoformat()
        }
        
        # Add RVU health
        if self.rvu_service:
            health["rvu_sync"] = await self.rvu_service.get_health_status()
        
        return health


# CLI command to test integration
async def test_integration():
    """Test the RVU integration system"""
    print("🧪 Testing RVU integration system...")
    
    integration = RVUIntegrationService()
    
    try:
        # Test startup
        await integration.start()
        
        # Test health check
        health = await integration.get_health_status()
        print(f"Health Status: {health}")
        
        # Test force sync
        sync_result = await integration.force_sync_if_needed()
        print(f"Sync Result: {sync_result}")
        
        # Test shutdown
        await integration.stop()
        
        print("✅ Integration test completed successfully")
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")


if __name__ == "__main__":
    asyncio.run(test_integration())