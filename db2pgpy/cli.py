"""Command-line interface for DB2 to PostgreSQL migration."""
import click
import sys
from pathlib import Path
from .config import load_config, validate_config
from .logger import setup_logger
from .connectors.db2 import DB2Connector
from .connectors.postgres import PostgresConnector
from .extractors.schema import SchemaExtractor
from .converters.schema import SchemaConverter
from .converters.types import TypeConverter
from .data_transfer import DataTransfer
from .validator import Validator
from .migrator import Migrator
from .progress import ProgressTracker
from . import __version__


@click.group()
@click.version_option(version=__version__)
def cli():
    """DB2 to PostgreSQL migration tool."""
    pass


@cli.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
def validate(config):
    """Validate configuration and test database connections."""
    logger = setup_logger("validate", level="INFO")
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {config}")
        config_data = load_config(config)
        
        # Validate configuration
        logger.info("Validating configuration...")
        errors = validate_config(config_data)
        
        if errors:
            logger.error("Configuration validation failed:")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        logger.info("✓ Configuration is valid")
        
        # Test database connections
        logger.info("Testing database connections...")
        
        try:
            # Test DB2 connection
            db2_config = config_data['db2']
            db2_conn = DB2Connector(config=db2_config)
            db2_conn.connect()
            logger.info("✓ DB2 connection successful")
            db2_conn.disconnect()
        except Exception as e:
            logger.error(f"✗ DB2 connection failed: {e}")
            sys.exit(1)
        
        try:
            # Test PostgreSQL connection
            pg_key = 'postgres' if 'postgres' in config_data else 'postgresql'
            pg_config = config_data[pg_key]
            pg_conn = PostgresConnector(config=pg_config)
            pg_conn.connect()
            logger.info("✓ PostgreSQL connection successful")
            pg_conn.disconnect()
        except Exception as e:
            logger.error(f"✗ PostgreSQL connection failed: {e}")
            sys.exit(1)
        
        logger.info("✓ Validation completed successfully")
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
@click.option(
    "--schema-only",
    is_flag=True,
    help="Migrate schema only (no data)",
)
@click.option(
    "--data-only",
    is_flag=True,
    help="Migrate data only (assumes schema exists)",
)
@click.option(
    "--tables",
    "-t",
    multiple=True,
    help="Specific tables to migrate (can be specified multiple times)",
)
@click.option(
    "--batch-size",
    default=1000,
    type=int,
    help="Batch size for data migration (default: 1000)",
)
@click.option(
    "--parallel",
    "-p",
    default=1,
    type=int,
    help="Number of parallel workers (default: 1)",
)
@click.option(
    "--no-sequences",
    is_flag=True,
    help="Disable automatic sequence creation for primary keys",
)
@click.option(
    "--drop-existing",
    is_flag=True,
    help="Drop existing tables before migration (USE WITH CAUTION!)",
)
@click.option(
    "--skip-existing",
    is_flag=True,
    help="Skip tables that already exist (default behavior)",
)
@click.option(
    "--force-data",
    is_flag=True,
    help="Force data transfer even for existing tables (overwrite/append data)",
)
def migrate(config, schema_only, data_only, tables, batch_size, parallel, no_sequences, drop_existing, skip_existing, force_data):
    """Run full migration from DB2 to PostgreSQL."""
    logger = setup_logger("migrate", level="INFO")
    
    try:
        # Load configuration
        logger.info(f"Loading configuration from {config}")
        config_data = load_config(config)
        
        # Validate configuration
        errors = validate_config(config_data)
        if errors:
            logger.error("Configuration validation failed")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        logger.info("✓ Configuration validated successfully")
        
        # Determine migration mode
        if schema_only and data_only:
            logger.error("Cannot specify both --schema-only and --data-only")
            sys.exit(1)
        
        if schema_only:
            mode = 'schema_only'
            logger.info("Migration mode: Schema only")
        elif data_only:
            mode = 'data_only'
            logger.info("Migration mode: Data only")
        else:
            mode = 'full'
            logger.info("Migration mode: Full (schema + data)")
        
        # Setup database connections
        db2_config = config_data['db2']
        pg_key = 'postgres' if 'postgres' in config_data else 'postgresql'
        pg_config = config_data[pg_key]
        
        logger.info("Connecting to databases...")
        db2_connector = DB2Connector(config=db2_config)
        db2_connector.connect()
        logger.info("✓ Connected to DB2")
        
        pg_connector = PostgresConnector(config=pg_config)
        pg_connector.connect()
        logger.info("✓ Connected to PostgreSQL")
        
        # Setup migration components
        db2_schema = db2_config.get('schema', db2_config.get('database'))
        pg_schema = pg_config.get('schema', 'public')
        schema_extractor = SchemaExtractor(db2_connector)
        type_converter = TypeConverter()
        schema_converter = SchemaConverter(type_converter)
        data_transfer = DataTransfer(db2_connector, pg_connector, batch_size, schema=db2_schema)
        validator = Validator(db2_connector, pg_connector, db2_schema=db2_schema, pg_schema=pg_schema)
        progress_tracker = ProgressTracker('.db2pgpy_state.json')
        
        # Create migrator
        migrator = Migrator(
            schema_extractor=schema_extractor,
            schema_converter=schema_converter,
            data_transfer=data_transfer,
            validator=validator,
            db2_connector=db2_connector,
            pg_connector=pg_connector,
            progress_tracker=progress_tracker,
            logger=logger,
            create_sequences=not no_sequences,
            drop_existing=drop_existing,
            skip_data_if_exists=not force_data  # If force_data=True, don't skip
        )
        
        # Prepare migration configuration
        migration_config = {
            'tables': list(tables) if tables else config_data.get('migration', {}).get('tables', []),
            'validate': config_data.get('validation', {}).get('enabled', True)
        }
        
        # If no tables specified, discover all tables from DB2 schema
        if not migration_config['tables']:
            logger.info("No tables specified, discovering tables from DB2 schema...")
            schema = db2_config.get('schema', db2_config.get('database'))
            discovered_tables = db2_connector.get_tables(schema)
            
            # Apply exclusions if configured
            exclude_tables = config_data.get('migration', {}).get('exclude_tables', [])
            if exclude_tables:
                discovered_tables = [t for t in discovered_tables if t not in exclude_tables]
                logger.info(f"Excluding {len(exclude_tables)} tables from migration")
            
            migration_config['tables'] = discovered_tables
            logger.info(f"Discovered {len(discovered_tables)} tables in schema '{schema}'")
        
        if not migration_config['tables']:
            logger.error("No tables found for migration")
            logger.error("Check that the DB2 schema contains tables")
            sys.exit(1)
        
        logger.info(f"Migrating {len(migration_config['tables'])} tables")
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Parallel workers: {parallel}")
        
        # Run migration
        logger.info("Starting migration...")
        result = migrator.run_migration(migration_config, mode=mode)
        
        # Display results
        logger.info("=" * 60)
        logger.info("MIGRATION COMPLETED")
        logger.info("=" * 60)
        logger.info(f"Status: {result['status']}")
        logger.info(f"Tables migrated: {result['tables_migrated']}")
        logger.info(f"Total time: {result['total_time']:.2f} seconds")
        logger.info("=" * 60)
        
        # Cleanup
        db2_connector.disconnect()
        pg_connector.disconnect()
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


@cli.command()
@click.option(
    "--config",
    "-c",
    required=True,
    type=click.Path(exists=True),
    help="Path to configuration YAML file",
)
@click.option(
    "--state-file",
    default=".db2pgpy_state.json",
    help="Path to state file (default: .db2pgpy_state.json)",
)
def resume(config, state_file):
    """Resume migration from last saved state."""
    logger = setup_logger("resume", level="INFO")
    
    try:
        # Check if state file exists
        state_path = Path(state_file)
        if not state_path.exists():
            logger.error(f"State file not found: {state_file}")
            logger.error("No migration to resume. Use 'migrate' command to start new migration.")
            sys.exit(1)
        
        logger.info(f"Loading state from {state_file}")
        
        # Load progress tracker
        progress_tracker = ProgressTracker(state_file)
        summary = progress_tracker.get_summary()
        
        logger.info("=" * 60)
        logger.info("MIGRATION PROGRESS")
        logger.info("=" * 60)
        logger.info(f"Current phase: {summary['current_phase']}")
        logger.info(f"Completed phases: {', '.join(summary['completed_phases']) if summary['completed_phases'] else 'None'}")
        logger.info(f"Tables: {summary['completed_tables']}/{summary['total_tables']} completed")
        logger.info(f"Rows: {summary['migrated_rows']}/{summary['total_rows']} migrated")
        logger.info(f"Overall progress: {summary['overall_percentage']:.2f}%")
        logger.info(f"Last updated: {summary['last_updated']}")
        logger.info("=" * 60)
        
        # Load configuration
        logger.info(f"Loading configuration from {config}")
        config_data = load_config(config)
        
        # Validate configuration
        errors = validate_config(config_data)
        if errors:
            logger.error("Configuration validation failed")
            for error in errors:
                logger.error(f"  - {error}")
            sys.exit(1)
        
        # Setup database connections
        db2_config = config_data['db2']
        pg_key = 'postgres' if 'postgres' in config_data else 'postgresql'
        pg_config = config_data[pg_key]
        
        logger.info("Connecting to databases...")
        db2_connector = DB2Connector(config=db2_config)
        db2_connector.connect()
        logger.info("✓ Connected to DB2")
        
        pg_connector = PostgresConnector(config=pg_config)
        pg_connector.connect()
        logger.info("✓ Connected to PostgreSQL")
        
        # Setup migration components
        schema_extractor = SchemaExtractor(db2_connector)
        type_converter = TypeConverter()
        schema_converter = SchemaConverter(type_converter)
        data_transfer = DataTransfer(db2_connector, pg_connector, 
                                     config_data.get('migration', {}).get('batch_size', 1000))
        validator = Validator(db2_connector, pg_connector)
        
        # Create migrator
        migrator = Migrator(
            schema_extractor=schema_extractor,
            schema_converter=schema_converter,
            data_transfer=data_transfer,
            validator=validator,
            db2_connector=db2_connector,
            pg_connector=pg_connector,
            progress_tracker=progress_tracker,
            logger=logger
        )
        
        # Determine which tables still need migration
        all_tables = config_data.get('migration', {}).get('tables', [])
        table_progress = progress_tracker.get_all_table_progress()
        
        # Filter to incomplete tables
        incomplete_tables = [
            table for table in all_tables
            if table not in table_progress or 
            table_progress[table]['rows_migrated'] < table_progress[table]['total_rows']
        ]
        
        if not incomplete_tables:
            logger.info("✓ All tables have been migrated")
            logger.info("Nothing to resume")
        else:
            logger.info(f"Resuming migration for {len(incomplete_tables)} remaining tables")
            
            migration_config = {
                'tables': incomplete_tables,
                'validate': config_data.get('validation', {}).get('enabled', True)
            }
            
            # Run migration
            result = migrator.run_migration(migration_config, mode='full')
            
            # Display results
            logger.info("=" * 60)
            logger.info("RESUMED MIGRATION COMPLETED")
            logger.info("=" * 60)
            logger.info(f"Status: {result['status']}")
            logger.info(f"Tables migrated: {result['tables_migrated']}")
            logger.info(f"Total time: {result['total_time']:.2f} seconds")
            logger.info("=" * 60)
        
        # Cleanup
        db2_connector.disconnect()
        pg_connector.disconnect()
        
    except Exception as e:
        logger.error(f"Resume failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
