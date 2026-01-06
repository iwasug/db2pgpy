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


class Migrator:
    """Orchestrate DB2 to PostgreSQL migration."""
    
    def __init__(self, schema_extractor: SchemaExtractor, 
                 schema_converter: SchemaConverter,
                 data_transfer: DataTransfer,
                 validator: Validator,
                 db2_connector: DB2Connector,
                 pg_connector: PostgresConnector,
                 progress_tracker: ProgressTracker,
                 logger: Any):
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
        """
        self.schema_extractor = schema_extractor
        self.schema_converter = schema_converter
        self.data_transfer = data_transfer
        self.validator = validator
        self.db2_connector = db2_connector
        self.pg_connector = pg_connector
        self.progress_tracker = progress_tracker
        self.logger = logger
    
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
        
        for table_name in tables:
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
            self.logger.info(f"Phase 3: Creating table {table_name} in PostgreSQL")
            self.pg_connector.execute_ddl(create_table_ddl)
            
            # Create primary keys if present
            if pk_columns:
                pk_ddl = self.schema_converter.generate_primary_key_ddl(table_name, pk_columns)
                self.pg_connector.execute_ddl(pk_ddl)
            
            # Phase 4: Transfer data (if not schema_only mode)
            if mode != 'schema_only':
                self.logger.info(f"Phase 4: Transferring data for {table_name}")
                self.data_transfer.transfer_table(table_name, self.progress_tracker)
            
            # Phase 5: Validate (if enabled and not schema_only)
            if mode != 'schema_only' and validate_enabled:
                self.logger.info(f"Phase 5: Validating {table_name}")
                self.validator.validate_row_counts(table_name)
            
            tables_migrated += 1
        
        total_time = time.time() - start_time
        
        self.logger.info(f"Migration completed: {tables_migrated} tables in {total_time:.2f}s")
        
        return {
            'status': 'completed',
            'tables_migrated': tables_migrated,
            'total_time': total_time
        }
