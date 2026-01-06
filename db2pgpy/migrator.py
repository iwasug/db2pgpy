"""Migrator orchestration module."""

import time
from typing import Dict, Any
from db2pgpy.extractors.schema import SchemaExtractor
from db2pgpy.converters.schema import SchemaConverter
from db2pgpy.data_transfer import DataTransfer
from db2pgpy.validator import Validator
from db2pgpy.connectors.db2 import DB2Connector
from db2pgpy.connectors.postgres import PostgresConnector
from db2pgpy.progress import ProgressTracker
from db2pgpy.sequence_manager import SequenceManager


class Migrator:
    """Orchestrate DB2 to PostgreSQL migration."""
    
    def __init__(self, schema_extractor: SchemaExtractor, 
                 schema_converter: SchemaConverter,
                 data_transfer: DataTransfer,
                 validator: Validator,
                 db2_connector: DB2Connector,
                 pg_connector: PostgresConnector,
                 progress_tracker: ProgressTracker,
                 logger: Any,
                 create_sequences: bool = True,
                 drop_existing: bool = False,
                 skip_data_if_exists: bool = True):
        """Initialize Migrator.
        
        Args:
            schema_extractor: Schema extractor instance
            schema_converter: Schema converter instance
            data_transfer: Data transfer instance
            validator: Validator instance
            db2_connector: DB2 connector instance
            pg_connector: PostgreSQL connector instance
            progress_tracker: Progress tracker instance
            logger: Logger instance
            create_sequences: Enable auto-sequence creation for primary keys (default: True)
            drop_existing: Drop existing tables before migration (default: False)
            skip_data_if_exists: Skip data transfer for existing tables (default: True)
        """
        self.schema_extractor = schema_extractor
        self.schema_converter = schema_converter
        self.data_transfer = data_transfer
        self.validator = validator
        self.db2_connector = db2_connector
        self.pg_connector = pg_connector
        self.progress_tracker = progress_tracker
        self.logger = logger
        self.create_sequences = create_sequences
        self.drop_existing = drop_existing
        self.skip_data_if_exists = skip_data_if_exists
        self.sequence_manager = SequenceManager(db2_connector, pg_connector) if create_sequences else None
    
    def run_migration(self, config: Dict[str, Any], mode: str = 'full') -> Dict[str, Any]:
        """Run end-to-end migration.
        
        Args:
            config: Migration configuration with tables list
            mode: Migration mode ('full', 'schema_only', 'data_only')
            
        Returns:
            Migration summary with status and statistics
        """
        start_time = time.time()
        tables = config.get('tables', [])
        validate_enabled = config.get('validate', True)
        
        self.logger.info(f"Starting migration in {mode} mode for {len(tables)} tables")
        
        tables_migrated = 0
        tables_failed = 0
        continue_on_error = config.get('continue_on_error', False)
        
        for table_name in tables:
            try:
                self.logger.info(f"Processing table: {table_name}")
                
                # Phase 1: Extract schema from DB2
                self.logger.info(f"Phase 1: Extracting schema for {table_name}")
                schema_info = self.schema_extractor.extract_table_schema(table_name)
                pk_columns = self.schema_extractor.extract_primary_keys(table_name)
                fk_info = self.schema_extractor.extract_foreign_keys(table_name)
                indexes = self.schema_extractor.extract_indexes(table_name)
                
                # Phase 2: Convert to PostgreSQL DDL
                self.logger.info(f"Phase 2: Converting schema for {table_name}")
                create_table_ddl = self.schema_converter.generate_create_table_ddl(
                    table_name, schema_info
                )
                
                # Phase 3: Create schema in PostgreSQL
                # Check if table already exists
                table_exists = self.pg_connector.table_exists(table_name)
                
                if table_exists and self.drop_existing:
                    self.logger.warning(f"⚠ Dropping existing table {table_name}")
                    self.pg_connector.execute_ddl(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
                    table_exists = False
                
                if table_exists:
                    self.logger.warning(f"⚠ Table {table_name} already exists in PostgreSQL - skipping schema creation")
                    
                    # Check if we should skip data transfer for existing tables
                    if self.skip_data_if_exists and mode != 'schema_only':
                        self.logger.info(f"⚠ Skipping data transfer for existing table {table_name} (skip_data_if_exists=True)")
                        tables_migrated += 1
                        continue
                    elif mode == 'schema_only':
                        self.logger.info(f"Skipping {table_name} (schema-only mode)")
                        tables_migrated += 1
                        continue
                else:
                    self.logger.info(f"Phase 3: Creating table {table_name} in PostgreSQL")
                    self.pg_connector.execute_ddl(create_table_ddl)
                    
                    # Create primary keys if present
                    if pk_columns:
                        pk_ddl = self.schema_converter.generate_primary_key_ddl(table_name, pk_columns)
                        self.pg_connector.execute_ddl(pk_ddl)
                        
                        # Create sequences for primary keys
                        if self.create_sequences and self.sequence_manager:
                            self.logger.info(f"Phase 3b: Creating sequences for {table_name}")
                            self.sequence_manager.create_sequences_for_table(table_name, pk_columns)
                
                # Phase 4: Transfer data (if not schema_only mode)
                if mode != 'schema_only':
                    self.logger.info(f"Phase 4: Transferring data for {table_name}")
                    self.data_transfer.transfer_table(table_name, self.progress_tracker)
                
                # Phase 5: Validate (if enabled and not schema_only)
                if mode != 'schema_only' and validate_enabled:
                    self.logger.info(f"Phase 5: Validating {table_name}")
                    self.validator.validate_row_counts(table_name)
                
                tables_migrated += 1
                
            except Exception as err:
                tables_failed += 1
                self.logger.error(f"✗ Failed to migrate table {table_name}: {str(err)}")
                
                if continue_on_error:
                    self.logger.warning(f"Continuing with next table (continue_on_error=True)")
                    # Rollback any pending transaction
                    if hasattr(self.pg_connector, 'conn') and self.pg_connector.conn:
                        try:
                            self.pg_connector.conn.rollback()
                        except Exception:
                            pass
                    continue
                else:
                    self.logger.error(f"Stopping migration (continue_on_error=False)")
                    raise
        
        total_time = time.time() - start_time
        
        status = 'completed' if tables_failed == 0 else 'completed_with_errors'
        self.logger.info(f"Migration {status}: {tables_migrated} tables succeeded, {tables_failed} tables failed in {total_time:.2f}s")
        
        return {
            'status': status,
            'tables_migrated': tables_migrated,
            'tables_failed': tables_failed,
            'total_time': total_time
        }
