# DB2 to PostgreSQL Migration Tool (db2pgpy)

A Python-based tool for migrating IBM DB2 databases to PostgreSQL with schema conversion, data transfer, and validation capabilities.

## Features

- **Schema Migration**: Automatically convert DB2 table structures to PostgreSQL
- **Type Conversion**: Intelligent mapping of DB2 data types to PostgreSQL equivalents
- **Data Transfer**: Efficient batch-based data migration with progress tracking
- **Resume Capability**: Resume interrupted migrations from the last checkpoint
- **Validation**: Post-migration validation of row counts and data integrity
- **PL/SQL Conversion**: Best-effort conversion of DB2 procedures to PL/pgSQL
- **Additional Objects**: Support for views, sequences, and stored procedures
- **Parallel Processing**: Optional parallel table migration for faster execution
- **Comprehensive Logging**: Detailed logs for troubleshooting and audit trails

## Requirements

- Python 3.8 or higher
- IBM DB2 Client Libraries (for `ibm_db` package)
- PostgreSQL 10 or higher

## Installation

### 1. Install IBM DB2 Client Libraries

Download and install the IBM Data Server Driver from IBM's website.

### 2. Install the Tool

```bash
# Clone the repository
git clone https://github.com/yourusername/db2pgpy.git
cd db2pgpy

# Install dependencies
pip install -r requirements.txt

# Install the package
pip install -e .
```

## Quick Start

### 1. Create a Configuration File

Create a YAML configuration file (e.g., `config.yaml`):

```yaml
db2:
  host: db2server.example.com
  port: 50000
  database: MYDB
  user: db2user
  password: db2password
  schema: MYSCHEMA

postgresql:
  host: pgserver.example.com
  port: 5432
  database: mydb
  user: pguser
  password: pgpassword
  schema: public

migration:
  tables:
    - CUSTOMERS
    - ORDERS
    - PRODUCTS
  batch_size: 1000
  parallel_workers: 1

validation:
  enabled: true
  row_count: true
  data_sampling: true
  sample_size: 100

logging:
  level: INFO
  file: migration.log
  console: true
```

### 2. Validate Configuration

Test your configuration and database connections:

```bash
db2pgpy validate --config config.yaml
```

### 3. Run Migration

Perform the complete migration:

```bash
# Full migration (schema + data)
db2pgpy migrate --config config.yaml

# Schema only
db2pgpy migrate --config config.yaml --schema-only

# Data only (assumes schema exists)
db2pgpy migrate --config config.yaml --data-only

# Specific tables only
db2pgpy migrate --config config.yaml --tables CUSTOMERS --tables ORDERS
```

### 4. Resume Interrupted Migration

If a migration is interrupted, you can resume from the last checkpoint:

```bash
db2pgpy resume --config config.yaml
```

## Configuration Options

### Database Configuration

#### DB2
- `host`: DB2 server hostname
- `port`: DB2 server port (default: 50000)
- `database`: Database name
- `user`: Database user
- `password`: Database password
- `schema`: Schema name (optional)
- `ssl`: Enable SSL connection (optional, default: false)
- `timeout`: Connection timeout in seconds (optional, default: 30)

#### PostgreSQL
- `host`: PostgreSQL server hostname
- `port`: PostgreSQL server port (default: 5432)
- `database`: Database name
- `user`: Database user
- `password`: Database password
- `schema`: Schema name (default: public)
- `ssl`: Enable SSL connection (optional, default: false)
- `timeout`: Connection timeout in seconds (optional, default: 30)

### Migration Options

- `mode`: Migration mode - `full`, `schema-only`, or `data-only` (default: full)
- `tables`: List of tables to migrate (required)
- `exclude_tables`: List of tables to exclude (optional)
- `batch_size`: Number of rows per batch for data transfer (default: 1000)
- `parallel_workers`: Number of parallel workers (default: 1)
- `continue_on_error`: Continue migration if errors occur (default: false)

### Validation Options

- `enabled`: Enable validation (default: true)
- `row_count`: Validate row counts match (default: true)
- `data_sampling`: Validate sample data (default: true)
- `sample_size`: Number of rows to sample (default: 100)

### Resume Configuration

- `enabled`: Enable resume capability (default: true)
- `checkpoint_file`: Path to checkpoint file (default: .db2pgpy_checkpoint.json)

### Logging Configuration

- `level`: Log level - DEBUG, INFO, WARNING, ERROR (default: INFO)
- `file`: Log file path (default: db2pgpy.log)
- `console`: Enable console output (default: true)
- `format`: Log message format

## Command-Line Interface

### Commands

#### validate
Validate configuration and test database connections.

```bash
db2pgpy validate --config config.yaml
```

#### migrate
Run the migration process.

```bash
db2pgpy migrate --config config.yaml [OPTIONS]
```

Options:
- `--schema-only`: Migrate schema only (no data)
- `--data-only`: Migrate data only (assumes schema exists)
- `--tables`, `-t`: Specific tables to migrate (can specify multiple times)
- `--batch-size`: Batch size for data migration (default: 1000)
- `--parallel`, `-p`: Number of parallel workers (default: 1)

#### resume
Resume an interrupted migration.

```bash
db2pgpy resume --config config.yaml [OPTIONS]
```

Options:
- `--state-file`: Path to state file (default: .db2pgpy_state.json)

## Type Mapping

See [TYPE_MAPPING.md](docs/TYPE_MAPPING.md) for complete DB2 to PostgreSQL type conversion reference.

## Migration Guide

See [MIGRATION_GUIDE.md](docs/MIGRATION_GUIDE.md) for step-by-step migration instructions and troubleshooting.

## Architecture

The tool consists of several components:

- **Connectors**: Database connection handlers for DB2 and PostgreSQL
- **Extractors**: Schema information extraction from DB2 system catalogs
- **Converters**: Type and schema conversion from DB2 to PostgreSQL
- **Data Transfer**: Efficient batch-based data migration
- **Validator**: Post-migration validation
- **Progress Tracker**: Resume capability with checkpoint management
- **Migrator**: Orchestration of the migration process

## Development

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=db2pgpy --cov-report=html

# Run specific test file
pytest tests/unit/test_migrator.py
```

### Code Formatting

```bash
# Format code with black
black db2pgpy tests

# Check code style with flake8
flake8 db2pgpy tests
```

## Limitations

- PL/SQL conversion is best-effort; complex procedures may require manual review
- Triggers are not automatically migrated
- DB2-specific features (e.g., materialized query tables) may not have direct equivalents
- Large tables (millions of rows) may require careful batch size tuning

## Troubleshooting

### Connection Errors

If you encounter connection errors:

1. Verify database credentials
2. Check network connectivity and firewall rules
3. Ensure DB2 client libraries are properly installed
4. Verify PostgreSQL allows remote connections (check `pg_hba.conf`)

### Type Conversion Issues

If type conversions fail:

1. Review the TYPE_MAPPING.md documentation
2. Check logs for specific conversion errors
3. Consider manual schema adjustments for complex types

### Data Transfer Issues

If data transfer fails or is slow:

1. Adjust `batch_size` in configuration (larger for faster, smaller for stability)
2. Check network bandwidth between servers
3. Monitor database resource usage (CPU, memory, disk I/O)
4. Review logs for specific error messages

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- GitHub Issues: https://github.com/yourusername/db2pgpy/issues
- Documentation: [MIGRATION_GUIDE.md](docs/MIGRATION_GUIDE.md)

## Acknowledgments

- IBM DB2 documentation
- PostgreSQL documentation
- Python community

## Version History

### 0.1.0 (Initial Release)
- Schema migration with type conversion
- Data transfer with batch processing
- Resume capability
- Validation features
- PL/SQL conversion (best-effort)
- Views, sequences, and procedures extraction
