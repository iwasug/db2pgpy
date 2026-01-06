# DB2 to PostgreSQL Migration Tool (db2pgpy)

A production-ready Python tool for migrating IBM DB2 databases (including IBM Maximo) to PostgreSQL with advanced schema conversion, automatic sequence generation, and intelligent data transfer capabilities.

## ✨ Key Features

- **🔄 Auto-Discovery**: Automatically discovers all tables from DB2 schema - no manual configuration needed
- **🔡 Case-Sensitive Names**: Preserves uppercase table and column names (DB2-compatible)
- **⚡ Auto-Increment Sequences**: Automatically creates PostgreSQL sequences for primary keys
- **🛡️ Smart Table Handling**: Intelligently handles existing tables with skip/drop/recreate options
- **📊 Schema Migration**: Automatically converts DB2 table structures to PostgreSQL
- **🔄 Type Conversion**: Intelligent mapping of DB2 data types to PostgreSQL equivalents
- **💾 Batch Data Transfer**: Efficient batch-based data migration with configurable batch sizes
- **✅ Validation**: Post-migration validation of row counts and data integrity
- **⏸️ Resume Capability**: Resume interrupted migrations from the last checkpoint
- **📝 Progress Tracking**: Real-time progress tracking with checkpoint management
- **🔍 Comprehensive Logging**: Detailed logs for troubleshooting and audit trails

## Requirements

- Python 3.8 or higher
- IBM DB2 Client Libraries (for `ibm_db` package)
- PostgreSQL 10 or higher

## Installation

### 1. Install IBM DB2 Client Libraries

Download and install the IBM Data Server Driver from IBM's website.

### 2. Create Virtual Environment (Recommended)

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

### 3. Install the Tool

```bash
# Clone the repository
git clone https://github.com/yourusername/db2pgpy.git
cd db2pgpy

# Install dependencies
pip install -r requirements.txt

# Install the package in development mode
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
# Full migration (all tables - auto-discovered)
db2pgpy migrate --config config.yaml

# Full migration with auto-increment sequences (default)
db2pgpy migrate --config config.yaml

# Schema only
db2pgpy migrate --config config.yaml --schema-only

# Data only (assumes schema exists)
db2pgpy migrate --config config.yaml --data-only

# Specific tables only
db2pgpy migrate --config config.yaml --tables CUSTOMERS --tables ORDERS

# Drop existing tables before migration (USE WITH CAUTION!)
db2pgpy migrate --config config.yaml --drop-existing

# Disable auto-sequence creation
db2pgpy migrate --config config.yaml --no-sequences
```

**Note**: If no tables are specified, the tool will automatically discover and migrate ALL tables from the DB2 schema!

### 4. Resume Interrupted Migration

If a migration is interrupted, you can resume from the last checkpoint:

```bash
db2pgpy resume --config config.yaml
```

Or simply re-run the same migrate command - the tool will automatically skip tables that already exist!

```bash
# Re-run after interruption - will skip completed tables
db2pgpy migrate --config config.yaml
```

## 🚀 New Features

### Auto-Discovery of Tables

No need to manually specify all table names! The tool automatically discovers all tables from your DB2 schema:

```bash
# Discovers and migrates ALL tables
db2pgpy migrate --config config.yaml
```

The tool will:
- ✅ Connect to DB2 schema
- ✅ Discover all tables (e.g., 1188 tables from MAXIMO)
- ✅ Apply any exclusion filters from config
- ✅ Migrate all discovered tables

### Case-Sensitive Table and Column Names

All table and column names are preserved in **UPPERCASE** to maintain DB2 compatibility:

```sql
-- PostgreSQL tables will be created as:
CREATE TABLE "CUSTOMERS" (
    "CUSTOMER_ID" bigint NOT NULL,
    "NAME" character varying(100) NOT NULL,
    ...
);

-- Query with proper quoting:
SELECT * FROM "CUSTOMERS" WHERE "CUSTOMER_ID" = 1;
```

### Automatic Sequence Generation

Primary keys automatically get PostgreSQL sequences for auto-increment behavior:

```sql
-- Automatically creates:
CREATE SEQUENCE "customers_customer_id_seq" START WITH 1;
ALTER TABLE "CUSTOMERS" 
    ALTER COLUMN "CUSTOMER_ID" 
    SET DEFAULT nextval('customers_customer_id_seq');

-- Insert without specifying ID:
INSERT INTO "CUSTOMERS" ("NAME", "EMAIL") 
VALUES ('John Doe', 'john@example.com');
-- CUSTOMER_ID is automatically assigned: 1, 2, 3, ...
```

**Sequence Start Value Logic**:
1. Checks MAXSEQUENCE table (for Maximo databases)
2. Falls back to MAX(column_value) from the table
3. Sets start value = max_value + 1

**Disable auto-sequences** if you want to preserve original behavior:
```bash
db2pgpy migrate --config config.yaml --no-sequences
```

### Intelligent Handling of Existing Tables

The tool provides three modes for handling tables that already exist:

#### 1. Default: Skip Schema, Transfer Data
```bash
db2pgpy migrate --config config.yaml
```
- ✅ Detects existing tables
- ✅ Skips schema creation (no errors!)
- ✅ Still transfers data (append/update)
- ✅ Perfect for incremental migrations

#### 2. Drop and Recreate
```bash
db2pgpy migrate --config config.yaml --drop-existing
```
- ⚠️ **WARNING**: Drops existing tables with CASCADE
- ✅ Creates fresh schema
- ✅ Creates new sequences
- ✅ Transfers all data
- ⚠️ **USE WITH CAUTION** - All existing data will be lost!

#### 3. Schema-Only Skip
```bash
db2pgpy migrate --config config.yaml --schema-only
```
- ✅ Skips existing tables completely
- ✅ Only creates new tables
- ✅ Safe for adding new tables to existing database

### Use Cases

**Incremental Migration**:
```bash
# Day 1: Migrate first 500 tables
db2pgpy migrate --config config.yaml --tables TABLE1 --tables TABLE2 ...

# Day 2: Migrate remaining tables (skips already migrated)
db2pgpy migrate --config config.yaml
```

**Re-migrate Single Table**:
```bash
# Fix and re-migrate a problematic table
db2pgpy migrate --config config.yaml --tables PROBLEM_TABLE --drop-existing
```

**Add New Tables Only**:
```bash
# Only create new tables, skip existing ones
db2pgpy migrate --config config.yaml --schema-only
```

**Sync Data Only**:
```bash
# Schema is good, just sync the data
db2pgpy migrate --config config.yaml --data-only
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
- `tables`: List of tables to migrate (optional - auto-discovers if empty)
- `exclude_tables`: List of tables to exclude (optional)
- `batch_size`: Number of rows per batch for data transfer (default: 1000)
- `parallel_workers`: Number of parallel workers (default: 1)
- `continue_on_error`: Continue migration if errors occur (default: false)

**Important**: If `tables` is empty or not specified, the tool will automatically discover ALL tables from the DB2 schema!

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
- `--no-sequences`: Disable automatic sequence creation for primary keys
- `--drop-existing`: Drop existing tables before migration (⚠️ USE WITH CAUTION!)
- `--skip-existing`: Skip tables that already exist (default behavior)

**Examples**:
```bash
# Migrate all tables (auto-discovery)
db2pgpy migrate --config config.yaml

# Migrate with auto-increment sequences (default)
db2pgpy migrate --config config.yaml

# Schema-only migration
db2pgpy migrate --config config.yaml --schema-only

# Re-migrate with drop existing tables
db2pgpy migrate --config config.yaml --drop-existing

# Migrate without sequences
db2pgpy migrate --config config.yaml --no-sequences

# Migrate specific tables
db2pgpy migrate --config config.yaml --tables CUSTOMERS --tables ORDERS
```

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

## 📋 IBM Maximo Support

This tool has been specifically tested and optimized for **IBM Maximo** databases:

- ✅ Successfully tested with 1188 Maximo tables
- ✅ Handles Maximo naming conventions (uppercase tables/columns)
- ✅ Compatible with MAXSEQUENCE table for sequence management
- ✅ Preserves Maximo data types and constraints
- ✅ Handles complex foreign key relationships

**Example Maximo Migration**:
```bash
# Migrate entire Maximo database
db2pgpy migrate --config config.yaml

# Result: 1188 tables migrated with:
# - All uppercase names preserved
# - Auto-increment sequences created
# - Primary keys and foreign keys migrated
# - Data validated
```

## Limitations

- Triggers are not automatically migrated (manual migration required)
- DB2-specific features (e.g., materialized query tables) may not have direct equivalents
- Large tables (millions of rows) may require careful batch size tuning
- Complex stored procedures may need manual review after migration

## Troubleshooting

### Connection Errors

If you encounter connection errors:

1. Verify database credentials
2. Check network connectivity and firewall rules
3. Ensure DB2 client libraries are properly installed
4. Verify PostgreSQL allows remote connections (check `pg_hba.conf`)

**PostgreSQL Database with Dot in Name**:
If your PostgreSQL database name contains a dot (e.g., `CMMS.Dev`), the tool handles it correctly using proper connection parameters.

### "Table Already Exists" Error

If you see "relation already exists" errors:

```bash
# Solution 1: Let the tool skip existing tables (default)
db2pgpy migrate --config config.yaml

# Solution 2: Drop and recreate (if safe to do so)
db2pgpy migrate --config config.yaml --drop-existing

# Solution 3: Schema-only for new tables
db2pgpy migrate --config config.yaml --schema-only
```

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

### 1.0.0 (Production Release)
- ✅ **Auto-Discovery**: Automatic table discovery from DB2 schema
- ✅ **Case-Sensitive Names**: Preserves uppercase table/column names
- ✅ **Auto-Increment Sequences**: Automatic PostgreSQL sequence generation
- ✅ **Smart Table Handling**: Skip/drop/recreate existing tables
- ✅ **Batch Data Transfer**: Efficient data migration with configurable batches
- ✅ **Progress Tracking**: Resume capability with checkpoints
- ✅ **Validation**: Row count and data integrity validation
- ✅ **IBM Maximo Support**: Tested with 1188 Maximo tables
- ✅ **Comprehensive Logging**: Detailed migration logs

### 0.1.0 (Initial Release)
- Schema migration with type conversion
- Data transfer with batch processing
- Basic resume capability
- Validation features
