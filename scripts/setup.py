#!/usr/bin/env python3
"""
Database Setup and Data Generation Master Script

This script orchestrates the complete database setup process:
1. Creates databases if they don't exist
2. Loads database schema files for PostgreSQL and SQL Server
3. Runs setup_reference_data.py to create and populate reference tables
4. Runs generate_sample_data.py to create 100,000 sample claims

Usage:
    python scripts/setup_database_and_data.py [options]

Options:
    --skip-database     Skip database creation (assumes databases exist)
    --skip-schema       Skip schema creation (assumes tables exist)
    --skip-reference    Skip reference data setup
    --skip-sample-data  Skip sample data generation
    --claims N          Number of claims to generate (default: 100000)
    --batch-size N      Batch size for data operations (default: 1000)
    --pg-schema PATH    Path to PostgreSQL schema file (default: sql/create_postgres_schema.sql)
    --sql-schema PATH   Path to SQL Server schema file (default: sql/create_sqlserver_schema.sql)
"""

import argparse
import asyncio
import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
# Change working directory to project root to find config.yaml
project_root = Path(__file__).parent.parent
os.chdir(project_root)

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase


class DatabaseSetupOrchestrator:
    """Orchestrates the complete database setup and data generation process"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    async def _ensure_databases_exist(self):
        """Ensure both PostgreSQL and SQL Server databases exist"""
        self.logger.info("Checking and creating databases if needed...")
        
        # Check/Create PostgreSQL database
        await self._ensure_postgres_database()
        
        # Check/Create SQL Server database  
        await self._ensure_sqlserver_database()

    async def _ensure_postgres_database(self):
        """Ensure PostgreSQL database exists, create if it doesn't"""
        self.logger.info("Checking PostgreSQL database...")
        
        # Create a temporary config to connect to the 'postgres' database
        temp_config = self.config.postgres.__class__(
            host=self.config.postgres.host,
            port=self.config.postgres.port,
            user=self.config.postgres.user,
            password=self.config.postgres.password,
            database="postgres",  # Connect to default database
            replica_host=self.config.postgres.replica_host,
            replica_port=self.config.postgres.replica_port,
            min_pool_size=self.config.postgres.min_pool_size,
            max_pool_size=self.config.postgres.max_pool_size,
            threshold_ms=self.config.postgres.threshold_ms,
            retries=self.config.postgres.retries,
            retry_delay=self.config.postgres.retry_delay,
            retry_max_delay=self.config.postgres.retry_max_delay,
            retry_jitter=self.config.postgres.retry_jitter,
        )
        
        pg_db = PostgresDatabase(temp_config)
        try:
            await pg_db.connect()
            
            # Check if target database exists
            result = await pg_db.fetch(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.config.postgres.database
            )
            
            if not result:
                self.logger.info(f"Creating PostgreSQL database: {self.config.postgres.database}")
                # Note: CREATE DATABASE cannot be run in a transaction
                await pg_db.execute(f"CREATE DATABASE {self.config.postgres.database}")
                self.logger.info("PostgreSQL database created successfully")
            else:
                self.logger.info("PostgreSQL database already exists")
                
        except Exception as e:
            self.logger.error(f"Error ensuring PostgreSQL database exists: {e}")
            raise
        finally:
            await pg_db.close()

    async def _ensure_sqlserver_database(self):
        """Ensure SQL Server database exists, create if it doesn't"""
        self.logger.info("Checking SQL Server database...")
        
        # Create a temporary config to connect to the 'master' database
        temp_config = self.config.sqlserver.__class__(
            host=self.config.sqlserver.host,
            port=self.config.sqlserver.port,
            user=self.config.sqlserver.user,
            password=self.config.sqlserver.password,
            database="master",  # Connect to master database
            pool_size=self.config.sqlserver.pool_size,
            min_pool_size=self.config.sqlserver.min_pool_size,
            max_pool_size=self.config.sqlserver.max_pool_size,
            threshold_ms=self.config.sqlserver.threshold_ms,
            retries=self.config.sqlserver.retries,
            retry_delay=self.config.sqlserver.retry_delay,
            retry_max_delay=self.config.sqlserver.retry_max_delay,
            retry_jitter=self.config.sqlserver.retry_jitter,
        )
        
        sql_db = SQLServerDatabase(temp_config)
        try:
            await sql_db.connect()
            
            # Check if target database exists
            result = await sql_db.fetch(
                "SELECT 1 FROM sys.databases WHERE name = ?",
                self.config.sqlserver.database
            )
            
            if not result:
                self.logger.info(f"Creating SQL Server database: {self.config.sqlserver.database}")
                await sql_db.execute(f"CREATE DATABASE [{self.config.sqlserver.database}]")
                self.logger.info("SQL Server database created successfully")
            else:
                self.logger.info("SQL Server database already exists")
                
        except Exception as e:
            self.logger.error(f"Error ensuring SQL Server database exists: {e}")
            raise
        finally:
            await sql_db.close()

    async def run_schema_setup(self, pg_schema_path: str, sql_schema_path: str):
        """Load and execute database schema files"""
        self.logger.info("Starting database schema setup...")

        # Setup PostgreSQL schema
        await self._setup_postgres_schema(pg_schema_path)

        # Setup SQL Server schema
        await self._setup_sqlserver_schema(sql_schema_path)

        self.logger.info("Database schema setup completed successfully!")

    async def _setup_postgres_schema(self, schema_path: str):
        """Setup PostgreSQL database schema"""
        self.logger.info("Setting up PostgreSQL schema...")

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"PostgreSQL schema file not found: {schema_path}")

        # Read schema file
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        pg_db = PostgresDatabase(self.config.postgres)
        try:
            await pg_db.connect()
            self.logger.info("Connected to PostgreSQL database")

            # Split SQL statements and execute them
            statements = self._split_sql_statements(schema_sql)

            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        # Skip database creation and connection statements
                        if any(
                            skip_cmd in statement.upper()
                            for skip_cmd in ["CREATE DATABASE", "\\C", "USE "]
                        ):
                            self.logger.info(
                                f"Skipping statement {i+1}: Database/connection command"
                            )
                            continue

                        self.logger.debug(f"Executing PostgreSQL statement {i+1}")
                        await pg_db.execute(statement)

                    except Exception as e:
                        # Log error but continue with other statements
                        self.logger.warning(
                            f"Error executing PostgreSQL statement {i+1}: {e}"
                        )
                        self.logger.debug(f"Failed statement: {statement[:100]}...")

            self.logger.info("PostgreSQL schema setup completed")

        except Exception as e:
            self.logger.error(f"Error setting up PostgreSQL schema: {e}")
            raise
        finally:
            await pg_db.close()

    async def _setup_sqlserver_schema(self, schema_path: str):
        """Setup SQL Server database schema"""
        self.logger.info("Setting up SQL Server schema...")

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"SQL Server schema file not found: {schema_path}")

        # Read schema file
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        sql_db = SQLServerDatabase(self.config.sqlserver)
        try:
            await sql_db.connect()
            self.logger.info("Connected to SQL Server database")

            # Split SQL statements and execute them
            statements = self._split_sql_statements(schema_sql, sql_server=True)

            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        # Skip database creation and USE statements
                        if any(
                            skip_cmd in statement.upper()
                            for skip_cmd in ["CREATE DATABASE", "USE "]
                        ):
                            self.logger.info(
                                f"Skipping statement {i+1}: Database/connection command"
                            )
                            continue

                        self.logger.debug(f"Executing SQL Server statement {i+1}")
                        await sql_db.execute(statement)

                    except Exception as e:
                        # Log error but continue with other statements
                        self.logger.warning(
                            f"Error executing SQL Server statement {i+1}: {e}"
                        )
                        self.logger.debug(f"Failed statement: {statement[:100]}...")

            self.logger.info("SQL Server schema setup completed")

        except Exception as e:
            self.logger.error(f"Error setting up SQL Server schema: {e}")
            raise
        finally:
            await sql_db.close()

    def _split_sql_statements(self, sql_content: str, sql_server: bool = False) -> list:
        """Split SQL content into individual statements"""
        if sql_server:
            # SQL Server uses GO as batch separator
            statements = sql_content.split("GO")
        else:
            # PostgreSQL uses semicolons
            statements = sql_content.split(";")

        # Clean up statements
        cleaned_statements = []
        for stmt in statements:
            cleaned = stmt.strip()
            if cleaned and not cleaned.startswith("--"):  # Skip comments
                cleaned_statements.append(cleaned)

        return cleaned_statements

    async def run_reference_data_setup(self):
        """Run the reference data setup script"""
        self.logger.info("Starting reference data setup...")

        try:
            # Import and run the setup_reference_data module
            from scripts.setup_reference_data import setup_reference_data

            await setup_reference_data()
            self.logger.info("Reference data setup completed successfully!")

        except Exception as e:
            self.logger.error(f"Error during reference data setup: {e}")
            raise

    async def run_sample_data_generation(
        self, total_claims: int = 100000, batch_size: int = 1000
    ):
        """Run the sample data generation script"""
        self.logger.info(
            f"Starting sample data generation ({total_claims:,} claims)..."
        )

        try:
            # Import and run the generate_sample_data module
            from scripts.generate_sample_data import \
                generate_and_load_sample_data

            await generate_and_load_sample_data(total_claims, batch_size)
            self.logger.info("Sample data generation completed successfully!")

        except Exception as e:
            self.logger.error(f"Error during sample data generation: {e}")
            raise

    async def run_full_setup(
        self,
        pg_schema_path: str,
        sql_schema_path: str,
        skip_database: bool = False,
        skip_schema: bool = False,
        skip_reference: bool = False,
        skip_sample_data: bool = False,
        total_claims: int = 100000,
        batch_size: int = 1000,
    ):
        """Run the complete database setup process"""

        start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPLETE DATABASE SETUP AND DATA GENERATION")
        self.logger.info("=" * 80)

        try:
            # Step 0: Ensure databases exist
            if not skip_database:
                self.logger.info("\n" + "=" * 50)
                self.logger.info("STEP 0: ENSURING DATABASES EXIST")
                self.logger.info("=" * 50)
                await self._ensure_databases_exist()
            else:
                self.logger.info("Skipping database creation (--skip-database specified)")

            # Step 1: Schema Setup
            if not skip_schema:
                self.logger.info("\n" + "=" * 50)
                self.logger.info("STEP 1: DATABASE SCHEMA SETUP")
                self.logger.info("=" * 50)
                await self.run_schema_setup(pg_schema_path, sql_schema_path)
            else:
                self.logger.info("Skipping schema setup (--skip-schema specified)")

            # Step 2: Reference Data Setup
            if not skip_reference:
                self.logger.info("\n" + "=" * 50)
                self.logger.info("STEP 2: REFERENCE DATA SETUP")
                self.logger.info("=" * 50)
                await self.run_reference_data_setup()
            else:
                self.logger.info(
                    "Skipping reference data setup (--skip-reference specified)"
                )

            # Step 3: Sample Data Generation
            if not skip_sample_data:
                self.logger.info("\n" + "=" * 50)
                self.logger.info("STEP 3: SAMPLE DATA GENERATION")
                self.logger.info("=" * 50)
                await self.run_sample_data_generation(total_claims, batch_size)
            else:
                self.logger.info(
                    "Skipping sample data generation (--skip-sample-data specified)"
                )

            # Summary
            duration = datetime.now() - start_time
            self.logger.info("\n" + "=" * 80)
            self.logger.info("DATABASE SETUP COMPLETED SUCCESSFULLY!")
            self.logger.info(f"Total Duration: {duration}")
            self.logger.info("=" * 80)

            # Next steps
            self.logger.info("\nNEXT STEPS:")
            self.logger.info("1. Verify your config.yaml database connections")
            self.logger.info("2. Test the claims processing pipeline:")
            self.logger.info("   python -m src.processing.main")
            self.logger.info("3. Start the web UI to monitor processing:")
            self.logger.info("   uvicorn src.web.app:app --reload")
            self.logger.info("4. Check processing results:")
            self.logger.info("   http://localhost:8000/failed_claims")

        except Exception as e:
            self.logger.error(f"Setup failed: {e}")
            raise


async def main():
    """Main entry point"""

    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Complete database setup and data generation for claims processing system",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Database and schema options
    parser.add_argument(
        "--skip-database", action="store_true", help="Skip database creation (assumes databases exist)"
    )
    parser.add_argument(
        "--skip-schema", action="store_true", help="Skip database schema creation"
    )
    parser.add_argument(
        "--skip-reference", action="store_true", help="Skip reference data setup"
    )
    parser.add_argument(
        "--skip-sample-data", action="store_true", help="Skip sample data generation"
    )

    # Data generation options
    parser.add_argument(
        "--claims",
        type=int,
        default=100000,
        help="Number of claims to generate (default: 100000)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for database operations (default: 1000)",
    )

    # Schema file paths
    parser.add_argument(
        "--pg-schema",
        default="sql/create_postgres_schema.sql",
        help="Path to PostgreSQL schema file",
    )
    parser.add_argument(
        "--sql-schema",
        default="sql/create_sqlserver_schema.sql",
        help="Path to SQL Server schema file",
    )

    # Logging options
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    # Configure logging with logs directory
    log_level = logging.DEBUG if args.debug else logging.INFO
    setup_log_filename = logs_dir / f'setup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(setup_log_filename),
        ],
    )

    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        logger.info("Loading configuration...")
        config = load_config()

        # Validate schema file paths (only if we're not skipping schema setup)
        if not args.skip_schema:
            if not os.path.exists(args.pg_schema):
                logger.error(f"PostgreSQL schema file not found: {args.pg_schema}")
                return 1
            if not os.path.exists(args.sql_schema):
                logger.error(f"SQL Server schema file not found: {args.sql_schema}")
                return 1

        # Create orchestrator and run setup
        orchestrator = DatabaseSetupOrchestrator(config)
        await orchestrator.run_full_setup(
            pg_schema_path=args.pg_schema,
            sql_schema_path=args.sql_schema,
            skip_database=args.skip_database,
            skip_schema=args.skip_schema,
            skip_reference=args.skip_reference,
            skip_sample_data=args.skip_sample_data,
            total_claims=args.claims,
            batch_size=args.batch_size,
        )

        return 0

    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Setup failed with error: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)