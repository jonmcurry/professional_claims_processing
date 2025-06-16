#!/usr/bin/env python3
"""
RVU Management CLI Tool
Provides command-line interface for managing RVU data synchronization

Usage Examples:
    python scripts/rvu_manager.py status              # Show sync status
    python scripts/rvu_manager.py sync                # Perform one-time sync
    python scripts/rvu_manager.py sync --force        # Force full resync
    python scripts/rvu_manager.py validate            # Validate data integrity  
    python scripts/rvu_manager.py schedule start      # Start sync scheduler
    python scripts/rvu_manager.py schedule stop       # Stop sync scheduler
    python scripts/rvu_manager.py update 99213        # Update specific RVU
    python scripts/rvu_manager.py stats               # Show sync statistics
"""

import asyncio
import argparse
import json
import sys
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

from src.config.config import load_config
from src.db.postgres import PostgresDatabase  
from src.db.sql_server import SQLServerDatabase


class RVUManager:
    """Main RVU management interface"""
    
    def __init__(self):
        self.config = load_config()
        self.pg_db = PostgresDatabase(self.config.postgres)
        self.sql_db = SQLServerDatabase(self.config.sqlserver)
    
    async def __aenter__(self):
        await self.pg_db.connect()
        await self.sql_db.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pg_db.close()
        await self.sql_db.close()
    
    async def show_status(self) -> Dict[str, Any]:
        """Show current RVU sync status"""
        print("🔍 Checking RVU sync status...")
        
        try:
            # Import sync system
            from scripts.rvu_sync_system import RVUDataSync
            rvu_sync = RVUDataSync(self.pg_db, self.sql_db)
            
            # Get integrity status
            integrity = await rvu_sync.validate_data_integrity()
            
            # Get sync history
            sync_history = await self._get_sync_history()
            
            status = {
                "timestamp": datetime.now().isoformat(),
                "data_integrity": integrity,
                "sync_history": sync_history,
                "system_status": "operational" if not integrity["sync_required"] else "needs_sync"
            }
            
            # Pretty print status
            self._print_status(status)
            return status
            
        except Exception as e:
            error_status = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "system_status": "error"
            }
            print(f"❌ Error checking status: {e}")
            return error_status
    
    async def perform_sync(self, force: bool = False) -> Dict[str, Any]:
        """Perform RVU data synchronization"""
        
        sync_type = "force_resync" if force else "incremental_sync"
        print(f"🔄 Starting {sync_type}...")
        
        try:
            from scripts.rvu_sync_system import RVUDataSync
            rvu_sync = RVUDataSync(self.pg_db, self.sql_db)
            
            if force:
                # Force full resync
                await rvu_sync.initial_sync()
                print("✅ Force resync completed")
            else:
                # Incremental sync
                changes = await rvu_sync.incremental_sync()
                print(f"✅ Incremental sync completed: {changes} changes applied")
            
            # Validate after sync
            integrity = await rvu_sync.validate_data_integrity()
            
            result = {
                "sync_type": sync_type,
                "timestamp": datetime.now().isoformat(),
                "integrity_check": integrity,
                "status": "success"
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Sync failed: {e}")
            return {
                "sync_type": sync_type,
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "status": "failed"
            }
    
    async def validate_integrity(self) -> Dict[str, Any]:
        """Validate RVU data integrity between databases"""
        print("🔍 Validating RVU data integrity...")
        
        try:
            from scripts.rvu_sync_system import RVUDataSync
            rvu_sync = RVUDataSync(self.pg_db, self.sql_db)
            
            integrity = await rvu_sync.validate_data_integrity()
            
            if integrity["sync_required"]:
                print("⚠️  Data integrity issues detected:")
                print(f"   SQL Server records: {integrity['sql_server_count']}")
                print(f"   PostgreSQL records: {integrity['postgresql_count']}")
                print(f"   Checksum match: {integrity['checksum_match']}")
                print("   Recommendation: Run sync to fix issues")
            else:
                print("✅ Data integrity validation passed")
                print(f"   Both databases have {integrity['postgresql_count']} records")
            
            return integrity
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            return {"error": str(e), "status": "failed"}
    
    async def update_rvu(self, procedure_code: str, **kwargs) -> bool:
        """Update a specific RVU record"""
        print(f"📝 Updating RVU for procedure code: {procedure_code}")
        
        try:
            from scripts.rvu_sync_scheduler import RVUManagementService
            service = RVUManagementService()
            
            # Prepare update data
            update_data = {k: v for k, v in kwargs.items() if v is not None}
            
            success = await service.update_single_rvu(procedure_code, update_data)
            
            if success:
                print(f"✅ Successfully updated {procedure_code}")
            else:
                print(f"❌ Failed to update {procedure_code}")
            
            return success
            
        except Exception as e:
            print(f"❌ Update failed: {e}")
            return False
    
    async def show_stats(self) -> Dict[str, Any]:
        """Show comprehensive RVU statistics"""
        print("📊 Gathering RVU statistics...")
        
        try:
            # PostgreSQL stats
            pg_stats = await self.pg_db.fetch("""
                SELECT 
                    COUNT(*) as total_codes,
                    COUNT(CASE WHEN status = 'active' THEN 1 END) as active_codes,
                    AVG(total_rvu) as avg_rvu,
                    MIN(total_rvu) as min_rvu,
                    MAX(total_rvu) as max_rvu,
                    COUNT(DISTINCT category) as categories
                FROM rvu_data
            """)
            
            # SQL Server stats  
            sql_stats = await self.sql_db.fetch("""
                SELECT 
                    COUNT(*) as total_codes,
                    COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active_codes,
                    AVG(total_rvu) as avg_rvu,
                    MIN(total_rvu) as min_rvu,
                    MAX(total_rvu) as max_rvu,
                    COUNT(DISTINCT category) as categories
                FROM rvu_data
            """)
            
            # Sync history
            sync_history = await self._get_sync_history(limit=5)
            
            stats = {
                "timestamp": datetime.now().isoformat(),
                "postgresql": dict(pg_stats[0]) if pg_stats else {},
                "sql_server": dict(sql_stats[0]) if sql_stats else {},
                "recent_syncs": sync_history
            }
            
            self._print_stats(stats)
            return stats
            
        except Exception as e:
            print(f"❌ Failed to gather stats: {e}")
            return {"error": str(e)}
    
    async def list_recent_changes(self, hours: int = 24) -> list:
        """List recent RVU changes"""
        print(f"📋 Showing RVU changes in last {hours} hours...")
        
        try:
            # Get recent changes from SQL Server
            changes = await self.sql_db.fetch("""
                SELECT 
                    procedure_code,
                    description,
                    total_rvu,
                    updated_at,
                    status
                FROM rvu_data 
                WHERE updated_at >= DATEADD(hour, -?, GETDATE())
                ORDER BY updated_at DESC
            """, hours)
            
            if changes:
                print(f"\n🔄 {len(changes)} changes found:")
                for change in changes:
                    print(f"   {change['procedure_code']}: {change['description']} "
                          f"(RVU: {change['total_rvu']}) - {change['updated_at']}")
            else:
                print("   No recent changes found")
            
            return [dict(change) for change in changes]
            
        except Exception as e:
            print(f"❌ Failed to get recent changes: {e}")
            return []
    
    async def _get_sync_history(self, limit: int = 10) -> list:
        """Get recent sync history"""
        try:
            history = await self.pg_db.fetch("""
                SELECT 
                    sync_type,
                    sync_completed_at,
                    records_processed,
                    sync_status
                FROM rvu_sync_log 
                ORDER BY sync_completed_at DESC 
                LIMIT $1
            """, limit)
            
            return [dict(record) for record in history]
            
        except Exception:
            # Table might not exist yet
            return []
    
    def _print_status(self, status: Dict[str, Any]) -> None:
        """Pretty print status information"""
        print("\n" + "="*60)
        print("📊 RVU SYNCHRONIZATION STATUS")
        print("="*60)
        
        integrity = status.get("data_integrity", {})
        
        print(f"🕐 Status Time: {status.get('timestamp', 'Unknown')}")
        print(f"🔧 System Status: {status.get('system_status', 'Unknown').upper()}")
        print()
        
        if integrity:
            print("📊 DATA INTEGRITY:")
            print(f"   SQL Server Records: {integrity.get('sql_server_count', 'Unknown'):,}")
            print(f"   PostgreSQL Records: {integrity.get('postgresql_count', 'Unknown'):,}")
            print(f"   Records Match: {'✅ Yes' if integrity.get('count_match') else '❌ No'}")
            print(f"   Checksum Match: {'✅ Yes' if integrity.get('checksum_match') else '❌ No'}")
            print(f"   Sync Required: {'⚠️  Yes' if integrity.get('sync_required') else '✅ No'}")
        
        sync_history = status.get("sync_history", [])
        if sync_history:
            print("\n🔄 RECENT SYNC HISTORY:")
            for sync in sync_history[:3]:
                print(f"   {sync.get('sync_completed_at', 'Unknown')}: "
                      f"{sync.get('sync_type', 'Unknown')} - "
                      f"{sync.get('records_processed', 0)} records - "
                      f"{sync.get('sync_status', 'Unknown')}")
        
        print("="*60)
    
    def _print_stats(self, stats: Dict[str, Any]) -> None:
        """Pretty print statistics"""
        print("\n" + "="*60)
        print("📈 RVU DATA STATISTICS")
        print("="*60)
        
        pg_stats = stats.get("postgresql", {})
        sql_stats = stats.get("sql_server", {})
        
        if pg_stats and sql_stats:
            print("🗄️  DATABASE COMPARISON:")
            print(f"                          PostgreSQL    SQL Server")
            print(f"   Total Codes:          {pg_stats.get('total_codes', 0):>10,}   {sql_stats.get('total_codes', 0):>10,}")
            print(f"   Active Codes:         {pg_stats.get('active_codes', 0):>10,}   {sql_stats.get('active_codes', 0):>10,}")
            print(f"   Categories:           {pg_stats.get('categories', 0):>10,}   {sql_stats.get('categories', 0):>10,}")
            print(f"   Avg RVU:              {float(pg_stats.get('avg_rvu', 0)):>10.2f}   {float(sql_stats.get('avg_rvu', 0)):>10.2f}")
            print(f"   Min RVU:              {float(pg_stats.get('min_rvu', 0)):>10.2f}   {float(sql_stats.get('min_rvu', 0)):>10.2f}")
            print(f"   Max RVU:              {float(pg_stats.get('max_rvu', 0)):>10.2f}   {float(sql_stats.get('max_rvu', 0)):>10.2f}")
        
        recent_syncs = stats.get("recent_syncs", [])
        if recent_syncs:
            print("\n🔄 RECENT SYNC ACTIVITY:")
            for sync in recent_syncs:
                print(f"   {sync.get('sync_completed_at', 'Unknown')}: "
                      f"{sync.get('sync_type', 'Unknown')} - {sync.get('records_processed', 0)} records")
        
        print("="*60)


async def main():
    """Main CLI interface"""
    parser = argparse.ArgumentParser(
        description="RVU Data Management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s status                    Show sync status
  %(prog)s sync                      Perform incremental sync
  %(prog)s sync --force              Force full resync
  %(prog)s validate                  Validate data integrity
  %(prog)s stats                     Show statistics
  %(prog)s changes --hours 48        Show changes in last 48 hours
  %(prog)s update 99213 --rvu 1.75   Update specific RVU value
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Status command
    subparsers.add_parser('status', help='Show RVU sync status')
    
    # Sync command
    sync_parser = subparsers.add_parser('sync', help='Perform RVU synchronization')
    sync_parser.add_argument('--force', action='store_true', help='Force full resync')
    
    # Validate command
    subparsers.add_parser('validate', help='Validate data integrity')
    
    # Stats command
    subparsers.add_parser('stats', help='Show RVU statistics')
    
    # Changes command
    changes_parser = subparsers.add_parser('changes', help='Show recent changes')
    changes_parser.add_argument('--hours', type=int, default=24, help='Hours to look back (default: 24)')
    
    # Update command
    update_parser = subparsers.add_parser('update', help='Update specific RVU')
    update_parser.add_argument('procedure_code', help='Procedure code to update')
    update_parser.add_argument('--rvu', type=float, help='New total RVU value')
    update_parser.add_argument('--description', help='New description')
    update_parser.add_argument('--category', help='New category')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        async with RVUManager() as manager:
            
            if args.command == 'status':
                await manager.show_status()
                
            elif args.command == 'sync':
                await manager.perform_sync(force=args.force)
                
            elif args.command == 'validate':
                await manager.validate_integrity()
                
            elif args.command == 'stats':
                await manager.show_stats()
                
            elif args.command == 'changes':
                await manager.list_recent_changes(args.hours)
                
            elif args.command == 'update':
                update_data = {}
                if args.rvu:
                    update_data['total_rvu'] = Decimal(str(args.rvu))
                if args.description:
                    update_data['description'] = args.description
                if args.category:
                    update_data['category'] = args.category
                
                if not update_data:
                    print("❌ No update values provided")
                    return
                
                await manager.update_rvu(args.procedure_code, **update_data)
    
    except KeyboardInterrupt:
        print("\n👋 Goodbye!")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())