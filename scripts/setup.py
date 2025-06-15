#!/usr/bin/env python3
"""
Fixed Database Setup Script - Handles SQL Server CREATE DATABASE Transaction Issue

This version properly handles SQL Server's requirement that CREATE DATABASE
must be executed outside of transactions.
"""

import argparse
import asyncio
import logging
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
# Change working directory to project root to find config.yaml
project_root = Path(__file__).parent.parent
os.chdir(project_root)

from src.config.config import load_config
from src.db.postgres import PostgresDatabase
from src.db.sql_server import SQLServerDatabase

# Import pyodbc to create direct connections for database creation
import pyodbc


class SchemaObject:
    """Represents a database schema object (table, index, view, etc.)"""
    def __init__(self, name: str, obj_type: str, definition: str = "", dependencies: List[str] = None):
        self.name = name
        self.obj_type = obj_type  # 'table', 'index', 'view', 'procedure', 'constraint'
        self.definition = definition
        self.dependencies = dependencies or []


class DatabaseSetupOrchestrator:
    """Orchestrates the complete database setup and data generation process with schema verification"""

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
            min_pool_size=1,  # Use minimal pool for setup
            max_pool_size=2,  # Use minimal pool for setup
            threshold_ms=self.config.postgres.threshold_ms,
            retries=self.config.postgres.retries,
            retry_delay=self.config.postgres.retry_delay,
            retry_max_delay=self.config.postgres.retry_max_delay,
            retry_jitter=self.config.postgres.retry_jitter,
        )
        
        pg_db = PostgresDatabase(temp_config)
        try:
            self.logger.info("Connecting to PostgreSQL server...")
            await pg_db.connect(prepare_queries=False)
            self.logger.info("Connected to PostgreSQL successfully")
            
            # Check if target database exists
            self.logger.info(f"Checking if database '{self.config.postgres.database}' exists...")
            result = await pg_db.fetch(
                "SELECT 1 FROM pg_database WHERE datname = $1",
                self.config.postgres.database
            )
            
            if not result:
                self.logger.info(f"Database '{self.config.postgres.database}' does not exist, creating it...")
                
                # Use direct connection to create database
                try:
                    conn = await pg_db.pool.acquire()
                    try:
                        create_db_query = f'CREATE DATABASE "{self.config.postgres.database}"'
                        self.logger.info(f"Executing: {create_db_query}")
                        await conn.execute(create_db_query)
                        self.logger.info("PostgreSQL database created successfully")
                    finally:
                        await pg_db.pool.release(conn)
                except Exception as create_error:
                    self.logger.error(f"Failed to create database: {str(create_error)}")
                    # Check if it was created by another process
                    result = await pg_db.fetch(
                        "SELECT 1 FROM pg_database WHERE datname = $1",
                        self.config.postgres.database
                    )
                    if not result:
                        raise create_error
                    else:
                        self.logger.info("Database exists now (created by another process)")
            else:
                self.logger.info("PostgreSQL database already exists")
                
        except Exception as e:
            self.logger.error(f"Error ensuring PostgreSQL database exists: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            raise
        finally:
            try:
                await pg_db.close()
            except:
                pass

    async def _ensure_sqlserver_database(self):
        """Ensure SQL Server database exists, create if it doesn't"""
        self.logger.info("Checking SQL Server database...")
        
        try:
            # Create direct pyodbc connection for database creation
            # SQL Server requires CREATE DATABASE to be executed outside of transactions
            self.logger.info("Creating direct SQL Server connection for database operations...")
            
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config.sqlserver.host},{self.config.sqlserver.port};"
                f"DATABASE=master;"  # Connect to master database
                f"UID={self.config.sqlserver.user};"
                f"PWD={self.config.sqlserver.password};"
                f"APP=ClaimsProcessor_Setup;"
                f"Connection Timeout=30;"
                f"TrustServerCertificate=yes;"
            )
            
            conn = pyodbc.connect(connection_string, autocommit=True)  # IMPORTANT: autocommit=True
            self.logger.info("Connected to SQL Server successfully")
            
            # Check if target database exists
            self.logger.info(f"Checking if database '{self.config.sqlserver.database}' exists...")
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM sys.databases WHERE name = ?", self.config.sqlserver.database)
            result = cursor.fetchone()
            
            if not result:
                self.logger.info(f"Database '{self.config.sqlserver.database}' does not exist, creating it...")
                
                create_db_query = f"CREATE DATABASE [{self.config.sqlserver.database}]"
                self.logger.info(f"Executing: {create_db_query}")
                
                # Execute without transaction (autocommit is enabled)
                cursor.execute(create_db_query)
                self.logger.info("SQL Server database created successfully")
            else:
                self.logger.info("SQL Server database already exists")
            
            cursor.close()
            conn.close()
                
        except Exception as e:
            self.logger.error(f"Error ensuring SQL Server database exists: {str(e)}")
            self.logger.error(f"Error type: {type(e).__name__}")
            
            # Log more details about the connection
            self.logger.error(f"Connection details:")
            self.logger.error(f"  Host: {self.config.sqlserver.host}")
            self.logger.error(f"  Port: {self.config.sqlserver.port}")
            self.logger.error(f"  User: {self.config.sqlserver.user}")
            self.logger.error(f"  Database: master")
            
            raise

    async def _get_postgres_schema_objects(self, db: PostgresDatabase) -> Dict[str, SchemaObject]:
        """Get existing schema objects from PostgreSQL database"""
        self.logger.info("Retrieving existing PostgreSQL schema objects...")
        
        objects = {}
        
        try:
            # Get tables - simplified query
            tables = await db.fetch(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = $1",
                'public'
            )
            
            for table in tables:
                table_name = table['table_name']
                table_type = 'view' if table['table_type'] == 'VIEW' else 'table'
                objects[table_name] = SchemaObject(table_name, table_type)
            
            self.logger.info(f"Found {len(objects)} tables/views")
        except Exception as e:
            self.logger.warning(f"Failed to get tables: {e}")
        
        try:
            # Get indexes - simplified query
            indexes = await db.fetch(
                "SELECT indexname, tablename FROM pg_indexes WHERE schemaname = $1",
                'public'
            )
            
            index_count = 0
            for index in indexes:
                index_name = index['indexname']
                if not index_name.endswith('_pkey'):  # Skip primary key indexes
                    objects[index_name] = SchemaObject(
                        index_name, 'index', 
                        dependencies=[index['tablename']]
                    )
                    index_count += 1
            
            self.logger.info(f"Found {index_count} indexes")
        except Exception as e:
            self.logger.warning(f"Failed to get indexes: {e}")
        
        try:
            # Get constraints - simplified query
            constraints = await db.fetch(
                """SELECT constraint_name, table_name, constraint_type
                   FROM information_schema.table_constraints 
                   WHERE table_schema = $1 
                   AND constraint_type IN ('FOREIGN KEY', 'CHECK', 'UNIQUE')""",
                'public'
            )
            
            constraint_count = 0
            for constraint in constraints:
                constraint_name = constraint['constraint_name']
                objects[constraint_name] = SchemaObject(
                    constraint_name, 'constraint',
                    dependencies=[constraint['table_name']]
                )
                constraint_count += 1
            
            self.logger.info(f"Found {constraint_count} constraints")
        except Exception as e:
            self.logger.warning(f"Failed to get constraints: {e}")
        
        try:
            # Get procedures/functions - simplified query
            procedures = await db.fetch(
                "SELECT routine_name FROM information_schema.routines WHERE routine_schema = $1",
                'public'
            )
            
            proc_count = 0
            for proc in procedures:
                proc_name = proc['routine_name']
                objects[proc_name] = SchemaObject(proc_name, 'procedure')
                proc_count += 1
            
            self.logger.info(f"Found {proc_count} procedures/functions")
        except Exception as e:
            self.logger.warning(f"Failed to get procedures: {e}")
        
        self.logger.info(f"Found {len(objects)} total existing schema objects in PostgreSQL")
        return objects

    async def _get_sqlserver_schema_objects(self, db: SQLServerDatabase) -> Dict[str, SchemaObject]:
        """Get existing schema objects from SQL Server database"""
        self.logger.info("Retrieving existing SQL Server schema objects...")
        
        objects = {}
        
        try:
            # Get tables and views - simplified query
            tables = await db.fetch(
                """SELECT name, type_desc 
                   FROM sys.objects 
                   WHERE type IN ('U', 'V') AND schema_id = SCHEMA_ID('dbo')"""
            )
            
            for table in tables:
                table_name = table['name']
                table_type = 'view' if table['type_desc'] == 'VIEW' else 'table'
                objects[table_name] = SchemaObject(table_name, table_type)
            
            self.logger.info(f"Found {len([o for o in objects.values() if o.obj_type in ['table', 'view']])} tables/views")
        except Exception as e:
            self.logger.warning(f"Failed to get tables: {e}")
        
        try:
            # Get indexes - simplified query
            indexes = await db.fetch(
                """SELECT i.name as index_name, t.name as table_name
                   FROM sys.indexes i
                   INNER JOIN sys.tables t ON i.object_id = t.object_id
                   WHERE i.name IS NOT NULL AND t.schema_id = SCHEMA_ID('dbo')"""
            )
            
            index_count = 0
            for index in indexes:
                index_name = index['index_name']
                if not index_name.startswith('PK_'):  # Skip primary key indexes
                    objects[index_name] = SchemaObject(
                        index_name, 'index',
                        dependencies=[index['table_name']]
                    )
                    index_count += 1
            
            self.logger.info(f"Found {index_count} indexes")
        except Exception as e:
            self.logger.warning(f"Failed to get indexes: {e}")
        
        try:
            # Get procedures - simplified query
            procedures = await db.fetch(
                "SELECT name FROM sys.procedures WHERE schema_id = SCHEMA_ID('dbo')"
            )
            
            proc_count = 0
            for proc in procedures:
                proc_name = proc['name']
                objects[proc_name] = SchemaObject(proc_name, 'procedure')
                proc_count += 1
            
            self.logger.info(f"Found {proc_count} procedures")
        except Exception as e:
            self.logger.warning(f"Failed to get procedures: {e}")
        
        self.logger.info(f"Found {len(objects)} total existing schema objects in SQL Server")
        return objects

    def _parse_schema_file_objects(self, schema_content: str, is_sqlserver: bool = False) -> Set[str]:
        """Parse schema file to extract expected object names"""
        expected_objects = set()
        
        # Table patterns
        table_patterns = [
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(\w+)',
            r'CREATE\s+TABLE\s+(?:\[?(\w+)\]?)',
        ]
        
        # Index patterns
        index_patterns = [
            r'CREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+)?(?:COLUMNSTORE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)',
            r'CREATE\s+(?:UNIQUE\s+)?(?:CLUSTERED\s+)?(?:COLUMNSTORE\s+)?INDEX\s+(?:\[?(\w+)\]?)',
        ]
        
        # View patterns
        view_patterns = [
            r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(\w+)',
            r'CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:\[?\w+\]?\.)?(?:\[?(\w+)\]?)',
        ]
        
        # Procedure patterns
        procedure_patterns = [
            r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+(?:\w+\.)?(\w+)',
            r'CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:\w+\.)?(\w+)',
        ]
        
        all_patterns = table_patterns + index_patterns + view_patterns + procedure_patterns
        
        for pattern in all_patterns:
            matches = re.finditer(pattern, schema_content, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                obj_name = match.group(1)
                if obj_name and not obj_name.lower() in ['exists', 'not', 'if']:
                    expected_objects.add(obj_name.lower())
        
        return expected_objects

    async def _drop_dependent_objects(self, db, objects_to_drop: List[SchemaObject], is_sqlserver: bool = False):
        """Drop objects in dependency order (indexes before tables, etc.)"""
        
        # Sort objects by type for proper dependency order
        drop_order = ['index', 'constraint', 'view', 'procedure', 'table']
        
        for obj_type in drop_order:
            type_objects = [obj for obj in objects_to_drop if obj.obj_type == obj_type]
            
            for obj in type_objects:
                try:
                    if obj.obj_type == 'table':
                        if is_sqlserver:
                            drop_sql = f"DROP TABLE [{obj.name}]"
                        else:
                            drop_sql = f"DROP TABLE IF EXISTS {obj.name} CASCADE"
                    elif obj.obj_type == 'view':
                        if is_sqlserver:
                            drop_sql = f"DROP VIEW [{obj.name}]"
                        else:
                            drop_sql = f"DROP VIEW IF EXISTS {obj.name} CASCADE"
                    elif obj.obj_type == 'index':
                        if is_sqlserver:
                            drop_sql = f"DROP INDEX [{obj.name}] ON [{obj.dependencies[0] if obj.dependencies else 'unknown'}]"
                        else:
                            drop_sql = f"DROP INDEX IF EXISTS {obj.name}"
                    elif obj.obj_type == 'procedure':
                        if is_sqlserver:
                            drop_sql = f"DROP PROCEDURE [{obj.name}]"
                        else:
                            drop_sql = f"DROP FUNCTION IF EXISTS {obj.name}() CASCADE"
                    elif obj.obj_type == 'constraint':
                        if is_sqlserver:
                            table_name = obj.dependencies[0] if obj.dependencies else 'unknown'
                            drop_sql = f"ALTER TABLE [{table_name}] DROP CONSTRAINT [{obj.name}]"
                        else:
                            table_name = obj.dependencies[0] if obj.dependencies else 'unknown'
                            drop_sql = f"ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS {obj.name}"
                    else:
                        continue
                    
                    self.logger.info(f"Dropping {obj.obj_type}: {obj.name}")
                    await db.execute(drop_sql)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to drop {obj.obj_type} {obj.name}: {e}")

    async def run_schema_setup_with_verification(
        self, 
        pg_schema_path: str, 
        sql_schema_path: str, 
        force_recreate: bool = False
    ):
        """Setup schemas with intelligent verification and recreation"""
        self.logger.info("Starting database schema setup with verification...")

        # Setup PostgreSQL schema with verification
        await self._setup_postgres_schema_with_verification(pg_schema_path, force_recreate)

        # Setup SQL Server schema with verification
        await self._setup_sqlserver_schema_with_verification(sql_schema_path, force_recreate)

        self.logger.info("Database schema setup with verification completed successfully!")

    async def _setup_postgres_schema_with_verification(self, schema_path: str, force_recreate: bool = False):
        """Setup PostgreSQL schema with verification"""
        self.logger.info("Setting up PostgreSQL schema with verification...")

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"PostgreSQL schema file not found: {schema_path}")

        # Read schema file
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        pg_db = PostgresDatabase(self.config.postgres)
        try:
            await pg_db.connect(prepare_queries=False)
            self.logger.info("Connected to PostgreSQL database")

            # Determine server version to check feature support
            server_version = 0
            try:
                version_rows = await pg_db.fetch("SHOW server_version_num")
                if version_rows:
                    server_version = int(version_rows[0]["server_version_num"])
            except Exception as e:
                self.logger.warning(
                    f"Could not determine PostgreSQL version: {e}"
                )
            supports_toast_compress = server_version >= 150000

            # Try to get existing objects, but handle errors gracefully
            existing_objects = {}
            try:
                existing_objects = await self._get_postgres_schema_objects(pg_db)
            except Exception as e:
                self.logger.warning(f"Could not retrieve existing schema objects: {e}")
                self.logger.info("Proceeding with fresh schema setup...")
            
            # Parse expected objects from schema file
            expected_objects = self._parse_schema_file_objects(schema_sql, is_sqlserver=False)
            
            self.logger.info(f"Expected objects: {len(expected_objects)}")
            self.logger.info(f"Existing objects: {len(existing_objects)}")
            
            # Determine what needs to be recreated
            objects_to_drop = []
            
            if force_recreate and existing_objects:
                self.logger.info("Force recreate enabled - dropping all existing objects")
                objects_to_drop = list(existing_objects.values())
            elif existing_objects:
                # Check for objects that exist but shouldn't, or need updates
                for obj_name, obj in existing_objects.items():
                    if obj_name.lower() not in expected_objects:
                        self.logger.info(f"Found unexpected object: {obj_name}")
                        objects_to_drop.append(obj)
            
            # Drop objects that need to be recreated
            if objects_to_drop:
                self.logger.info(f"Dropping {len(objects_to_drop)} objects for recreation...")
                await self._drop_dependent_objects(pg_db, objects_to_drop, is_sqlserver=False)
            
            # Execute schema statements
            statements = self._split_sql_statements(schema_sql)
            successful_statements = 0
            
            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        # Skip database creation and connection statements
                        if any(
                            skip_cmd in statement.upper()
                            for skip_cmd in ["CREATE DATABASE", "\\C", "USE "]
                        ):
                            continue

                        if (
                            not supports_toast_compress
                            and "toast.compress" in statement.lower()
                        ):
                            self.logger.info(
                                "Skipping toast compression statement due to unsupported PostgreSQL version"
                            )
                            continue

                        await pg_db.execute(statement)
                        successful_statements += 1

                    except Exception as e:
                        # Log error but continue with other statements
                        self.logger.warning(f"Error executing PostgreSQL statement {i+1}: {e}")
                        self.logger.debug(f"Failed statement: {statement[:100]}...")

            self.logger.info(f"PostgreSQL schema setup completed - {successful_statements} statements executed")

        except Exception as e:
            self.logger.error(f"Error setting up PostgreSQL schema: {e}")
            raise
        finally:
            await pg_db.close()

    async def _setup_sqlserver_schema_with_verification(self, schema_path: str, force_recreate: bool = False):
        """Setup SQL Server schema with verification"""
        self.logger.info("Setting up SQL Server schema with verification...")

        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"SQL Server schema file not found: {schema_path}")

        # Read schema file
        with open(schema_path, "r", encoding="utf-8") as f:
            schema_sql = f.read()

        sql_db = SQLServerDatabase(self.config.sqlserver)
        try:
            await sql_db.connect()
            self.logger.info("Connected to SQL Server database")

            # Try to get existing objects, but handle errors gracefully
            existing_objects = {}
            try:
                existing_objects = await self._get_sqlserver_schema_objects(sql_db)
            except Exception as e:
                self.logger.warning(f"Could not retrieve existing schema objects: {e}")
                self.logger.info("Proceeding with fresh schema setup...")
            
            # Parse expected objects from schema file
            expected_objects = self._parse_schema_file_objects(schema_sql, is_sqlserver=True)
            
            self.logger.info(f"Expected objects: {len(expected_objects)}")
            self.logger.info(f"Existing objects: {len(existing_objects)}")
            
            # Determine what needs to be recreated
            objects_to_drop = []
            
            if force_recreate and existing_objects:
                self.logger.info("Force recreate enabled - dropping all existing objects")
                objects_to_drop = list(existing_objects.values())
            elif existing_objects:
                # Check for objects that exist but shouldn't, or need updates
                for obj_name, obj in existing_objects.items():
                    if obj_name.lower() not in expected_objects:
                        self.logger.info(f"Found unexpected object: {obj_name}")
                        objects_to_drop.append(obj)
            
            # Drop objects that need to be recreated
            if objects_to_drop:
                self.logger.info(f"Dropping {len(objects_to_drop)} objects for recreation...")
                await self._drop_dependent_objects(sql_db, objects_to_drop, is_sqlserver=True)
            
            # Execute schema statements
            statements = self._split_sql_statements(schema_sql, sql_server=True)
            successful_statements = 0

            for i, statement in enumerate(statements):
                if statement.strip():
                    try:
                        # Skip database creation and USE statements
                        if any(
                            skip_cmd in statement.upper()
                            for skip_cmd in ["CREATE DATABASE", "USE "]
                        ):
                            continue

                        await sql_db.execute(statement)
                        successful_statements += 1

                    except Exception as e:
                        # Log error but continue with other statements
                        self.logger.warning(f"Error executing SQL Server statement {i+1}: {e}")
                        self.logger.debug(f"Failed statement: {statement[:100]}...")

            self.logger.info(f"SQL Server schema setup completed - {successful_statements} statements executed")

        except Exception as e:
            self.logger.error(f"Error setting up SQL Server schema: {e}")
            raise
        finally:
            await sql_db.close()

    def _split_sql_statements(self, sql_content: str, sql_server: bool = False) -> list:
        """Split SQL content into individual statements.

        For PostgreSQL this method is aware of dollar quoted blocks so that
        functions or procedures containing semicolons are not split into
        multiple statements.  The previous implementation simply split on
        semicolons which resulted in statements like the ``archive_old_claims``
        procedure being truncated and causing errors such as ``unterminated
        dollar-quoted string`` when executed.
        """

        if sql_server:
            # SQL Server uses GO as batch separator. Split on lines that
            # contain only GO, ignoring case and surrounding whitespace.
            raw_statements = re.split(
                r"^\s*GO\s*$", sql_content, flags=re.IGNORECASE | re.MULTILINE
            )
        else:
            raw_statements = []
            current = []
            in_dollar = False
            dollar_tag = ""
            in_single = False

            for line in sql_content.splitlines():
                i = 0
                while i < len(line):
                    ch = line[i]

                    # Handle line comments when not within a quoted block
                    if not in_dollar and not in_single and line[i:].startswith("--"):
                        break

                    if ch == "'" and not in_dollar:
                        in_single = not in_single
                        current.append(ch)
                        i += 1
                        continue

                    if ch == "$" and not in_single:
                        match = re.match(r"\$[^$]*\$", line[i:])
                        if match:
                            tag = match.group()
                            current.append(tag)
                            i += len(tag)
                            if in_dollar and tag == dollar_tag:
                                in_dollar = False
                                dollar_tag = ""
                            else:
                                in_dollar = True
                                dollar_tag = tag
                            continue

                    if ch == ";" and not in_single and not in_dollar:
                        current.append(ch)
                        raw_statements.append("".join(current).strip())
                        current = []
                        i += 1
                        continue

                    current.append(ch)
                    i += 1

                current.append("\n")

            if "".join(current).strip():
                raw_statements.append("".join(current).strip())

        # Clean up statements and remove empty/comment-only ones
        cleaned_statements = []
        for stmt in raw_statements:
            cleaned = stmt.strip()
            if cleaned and not cleaned.startswith("--"):
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
        force_schema: bool = False,
        total_claims: int = 100000,
        batch_size: int = 1000,
    ):
        """Run the complete database setup process with intelligent schema management"""

        start_time = datetime.now()
        self.logger.info("=" * 80)
        self.logger.info("STARTING ENHANCED DATABASE SETUP WITH SCHEMA VERIFICATION")
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

            # Step 1: Schema Setup with Verification
            if not skip_schema:
                self.logger.info("\n" + "=" * 50)
                self.logger.info("STEP 1: DATABASE SCHEMA SETUP WITH VERIFICATION")
                self.logger.info("=" * 50)
                if force_schema:
                    self.logger.info("Force schema recreation enabled")
                await self.run_schema_setup_with_verification(pg_schema_path, sql_schema_path, force_schema)
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
            self.logger.info("ENHANCED DATABASE SETUP COMPLETED SUCCESSFULLY!")
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
        description="Enhanced database setup with intelligent schema verification for claims processing system",
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
    parser.add_argument(
        "--force-schema", action="store_true", help="Force recreation of all schema objects"
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
            force_schema=args.force_schema,
            total_claims=args.claims,
            batch_size=args.batch_size,
        )

        return 0

    except KeyboardInterrupt:
        logger.info("Setup interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Setup failed with error: {e}")
        logger.error("Full error traceback:", exc_info=True)
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)