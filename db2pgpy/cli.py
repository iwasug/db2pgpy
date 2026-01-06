"""Command-line interface for DB2 to PostgreSQL migration."""
import click
import sys
from pathlib import Path
from .config import load_config, validate_config
from .logger import setup_logger
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
        
        # TODO: Test database connections
        logger.info("Database connection tests not yet implemented")
        
        logger.info("Validation completed successfully")
        
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
def migrate(config, schema_only, data_only, tables, batch_size, parallel):
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
        
        logger.info("Configuration validated successfully")
        
        # Migration options
        if schema_only and data_only:
            logger.error("Cannot specify both --schema-only and --data-only")
            sys.exit(1)
        
        if schema_only:
            logger.info("Migration mode: Schema only")
        elif data_only:
            logger.info("Migration mode: Data only")
        else:
            logger.info("Migration mode: Full (schema + data)")
        
        if tables:
            logger.info(f"Migrating specific tables: {', '.join(tables)}")
        
        logger.info(f"Batch size: {batch_size}")
        logger.info(f"Parallel workers: {parallel}")
        
        # TODO: Implement actual migration logic
        logger.warning("Migration implementation not yet complete")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
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
        
        # Load configuration
        logger.info(f"Loading configuration from {config}")
        config_data = load_config(config)
        
        # TODO: Implement resume logic
        logger.warning("Resume implementation not yet complete")
        
    except Exception as e:
        logger.error(f"Resume failed: {e}")
        sys.exit(1)


def main():
    """Entry point for the CLI."""
    cli()


if __name__ == "__main__":
    main()
