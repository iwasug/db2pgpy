# DB2 to PostgreSQL Migration Tool - Design Document

**Date**: 2026-01-06  
**Version**: 1.0  
**Purpose**: CLI application for one-time full database migration from DB2 to PostgreSQL

---

## Overview

A Python CLI tool that performs comprehensive one-time migration of DB2 databases to PostgreSQL, including full schema (tables, indexes, constraints, views, stored procedures, triggers, sequences) and data transfer with automatic type conversion, batch processing, resume capability, and post-migration validation.

---

## Architecture

### Modular Component Architecture

The tool follows a modular design with clear separation of concerns, making it testable, maintainable, and extensible.

### Project Structure

```
db2pgpy/
├── db2pgpy/
│   ├── __init__.py
│   ├── cli.py                 # CLI entry point using Click
│   ├── config.py              # YAML config loader & validator
│   ├── connectors/
│   │   ├── __init__.py
│   │   ├── db2.py             # DB2 connection & queries
│   │   └── postgres.py        # PostgreSQL connection & queries
│   ├── extractors/
│   │   ├── __init__.py
│   │   ├── schema.py          # Extract table schemas
│   │   ├── views.py           # Extract views
│   │   ├── procedures.py      # Extract stored procedures
│   │   ├── triggers.py        # Extract triggers
│   │   └── sequences.py       # Extract sequences
│   ├── converters/
│   │   ├── __init__.py
│   │   ├── types.py           # Type mapping DB2→PostgreSQL
│   │   ├── schema.py          # Schema DDL conversion
│   │   └── plsql.py           # SQL PL→PL/pgSQL conversion
│   ├── migrator.py            # Orchestrates migration phases
│   ├── data_transfer.py       # Batch data migration
│   ├── validator.py           # Post-migration validation
│   ├── progress.py            # Progress tracking & resume
│   └── logger.py              # Logging utilities
├── docs/
│   └── plans/
├── tests/
│   ├── unit/
│   └── integration/
├── config.example.yaml
├── requirements.txt
├── setup.py
└── README.md
```

---

## Core Components

### CLI (cli.py)

Entry point using Click framework providing intuitive commands.

**Commands**:
- `db2pgpy migrate --config config.yaml [--mode MODE]` - Execute migration
- `db2pgpy resume --config config.yaml` - Resume interrupted migration
- `db2pgpy validate --config config.yaml` - Run validation only
- `db2pgpy validate-config --config config.yaml` - Test configuration and connectivity

**Flags**:
- `--config` - Path to YAML configuration file (required)
- `--mode` - Migration mode: full, schema_only, data_only (overrides config)
- `--help` - Display command help
- `--version` - Display tool and dependency versions

### Config Manager (config.py)

Loads and validates YAML configuration files.

**Responsibilities**:
- Parse YAML configuration
- Validate required fields using jsonschema
- Provide typed configuration objects to other components
- Support for optional .env file for sensitive credentials

### Connectors (connectors/)

Database connection wrappers with error handling and connection pooling.

**DB2 Connector (db2.py)**:
- Wraps ibm_db or ibm_db_dbi
- Connection retry logic with exponential backoff
- Query execution with error handling
- System catalog queries (SYSCAT tables)

**PostgreSQL Connector (postgres.py)**:
- Wraps psycopg2
- Connection pooling for performance
- Transaction management
- Bulk insert operations (COPY or batch INSERT)

### Extractors (extractors/)

Query DB2 system catalogs to extract database object metadata.

**Schema Extractor (schema.py)**:
- Extract table definitions from SYSCAT.TABLES, SYSCAT.COLUMNS
- Extract indexes from SYSCAT.INDEXES
- Extract constraints (PK, FK, UNIQUE, CHECK) from SYSCAT.TABCONST, SYSCAT.KEYCOLUSE

**Views Extractor (views.py)**:
- Extract view definitions from SYSCAT.VIEWS
- Preserve view dependencies for creation order

**Procedures Extractor (procedures.py)**:
- Extract stored procedure code from SYSCAT.PROCEDURES, SYSCAT.ROUTINES
- Capture parameters and return types

**Triggers Extractor (triggers.py)**:
- Extract trigger definitions from SYSCAT.TRIGGERS
- Preserve trigger timing and events

**Sequences Extractor (sequences.py)**:
- Extract sequence definitions from SYSCAT.SEQUENCES
- Capture current values, increments, min/max values

### Converters (converters/)

Transform DB2 objects to PostgreSQL equivalents.

**Type Mapper (types.py)**:
- Comprehensive DB2 to PostgreSQL type mappings
- Automatic best-effort conversions with logging
- Warning generation for approximate mappings

**Type Mappings**:
- Numeric: SMALLINT→SMALLINT, INTEGER→INTEGER, BIGINT→BIGINT, DECIMAL(p,s)→NUMERIC(p,s), REAL→REAL, DOUBLE→DOUBLE PRECISION, DECFLOAT→NUMERIC
- String: CHAR(n)→CHAR(n), VARCHAR(n)→VARCHAR(n), CLOB→TEXT, GRAPHIC(n)→VARCHAR(n*2) [warning], VARGRAPHIC→VARCHAR [warning], DBCLOB→TEXT
- Date/Time: DATE→DATE, TIME→TIME, TIMESTAMP→TIMESTAMP, TIMESTAMP WITH TIME ZONE→TIMESTAMPTZ
- Binary: BLOB→BYTEA, BINARY→BYTEA, VARBINARY→BYTEA
- Special: XML→XML, BOOLEAN→BOOLEAN, ROWID→skipped [warning]

**Schema Converter (schema.py)**:
- Generate PostgreSQL CREATE TABLE statements
- Convert index definitions
- Convert constraint syntax
- Handle dependency ordering

**PL/SQL Converter (plsql.py)**:
- Best-effort conversion from DB2 SQL PL to PL/pgSQL
- Syntax translation for control structures
- System variable mapping
- Fallback to file extraction for complex procedures

**Conversion Strategy**:
- Convert control structures: IF/WHILE/FOR to PostgreSQL equivalents
- Replace DB2 system variables with PostgreSQL equivalents
- Save unconvertible procedures to `.db2sql` files for manual review
- Log warnings with file locations

### Migrator (migrator.py)

Orchestrates the entire migration process through five sequential phases.

**Phase 1: Extract**
- Query DB2 system catalogs for all object metadata
- Store in Python data structures for processing

**Phase 2: Convert**
- Transform DB2 metadata to PostgreSQL definitions
- Apply type mappings
- Convert DDL syntax
- Attempt procedural code conversion

**Phase 3: Create Schema**
- Execute PostgreSQL DDL in dependency order
- Create: tables → indexes → sequences → views → procedures → triggers
- Log each successful creation
- Handle creation failures appropriately

**Phase 4: Transfer Data**
- Batch process data for each table
- Transform data values as needed
- Update progress after each table

**Phase 5: Validate**
- Compare row counts
- Sample and verify data
- Compare schema structures
- Generate validation report

### Data Transfer (data_transfer.py)

Handles bulk data migration with batch processing.

**Approach**:
- Use `fetchmany()` for chunked iteration from DB2
- Configurable batch size (default 1000 rows)
- Each batch wrapped in PostgreSQL transaction
- Progress updates after each batch
- Metrics: rows/second, elapsed time, ETA

**Data Transformation**:
- Handle date/time format differences
- Manage character encoding
- Process NULL values appropriately
- Handle special characters in strings

### Validator (validator.py)

Post-migration validation with multiple verification levels.

**Row Count Validation**:
- Query COUNT(*) on both databases for all tables
- Report mismatches with table names and counts
- Mark validation as FAILED on any mismatch

**Schema Structure Validation**:
- Compare column counts and names
- Verify mapped data types match expectations
- Check primary keys, unique constraints, foreign keys
- Warn about index differences

**Data Sampling Validation**:
- Select random sample rows (configurable size, default 100)
- Compare values between DB2 and PostgreSQL
- Handle type differences (precision, padding)
- Report data mismatches with details

**Validation Report**:
- Tables validated (passed/failed counts)
- Total row counts compared
- Data mismatches found
- Schema differences
- Objects skipped
- Saved to `validation_report.txt` with timestamp

### Progress Tracker (progress.py)

Manages migration state for resume capability.

**State File** (`.db2pgpy_progress.json`):
- Migration mode
- Completed tables list
- Current table and last batch position
- Failed objects with error messages
- Timestamps and metrics

**Resume Logic**:
- Skip completed tables on resume
- Continue from last saved position
- Validate state consistency
- Delete state file on successful completion

### Logger (logger.py)

Multi-level logging with dual output.

**Log Levels**:
- ERROR: Critical failures
- WARNING: Non-blocking issues, approximate conversions
- INFO: Progress updates, completed phases
- DEBUG: SQL statements, detailed object information

**Output Destinations**:
- Console: Colored formatting with progress bars (tqdm, colorama)
- Log File: Timestamped entries, full details, SQL statements

---

## Migration Modes

### Full Migration (default)

Executes all five phases: Extract → Convert → Create Schema → Transfer Data → Validate

**Use Cases**:
- Complete database migration
- First-time migration from DB2 to PostgreSQL

### Schema-Only Mode

Executes phases 1-3 only: Extract → Convert → Create Schema

**Use Cases**:
- Setting up target database structure first
- Testing schema conversion before data transfer
- Separating schema and data migration phases

**Behavior**:
- Skips data transfer and data validation
- Performs schema structure validation only
- Reports objects created (counts)
- Faster completion (minutes vs hours)

### Data-Only Mode

Executes phases 4-5 only: Transfer Data → Validate

**Use Cases**:
- Re-running data migration after schema fixes
- Incremental data loads
- Separating schema and data migration phases

**Pre-Validation**:
- Verify all source tables exist in target PostgreSQL
- Confirm column compatibility (names and types)
- Error if schema mismatch or missing tables

**Behavior**:
- Skips extraction and conversion
- Performs batch data transfer
- Validates row counts and data sampling

---

## Configuration

### YAML Configuration File

```yaml
db2:
  host: localhost
  port: 50000
  database: SAMPLE
  user: db2admin
  password: secret123
  schema: DB2INST1  # Optional: specific schema to migrate

postgresql:
  host: localhost
  port: 5432
  database: migrated_db
  user: postgres
  password: pgpass123
  schema: public  # Target schema

migration:
  mode: full  # Options: full, schema_only, data_only
  batch_size: 1000
  continue_on_error: false
  save_failed_objects: true  # Save unconverted procedures/triggers to files
  failed_objects_dir: ./failed_conversions
  
resume:
  enabled: true
  state_file: .db2pgpy_progress.json
  
logging:
  level: INFO  # DEBUG, INFO, WARNING, ERROR
  console: true
  file: migration.log
  show_sql: false  # Log SQL statements (useful for debugging)

validation:
  enabled: true
  sample_size: 100  # Rows to sample for data validation per table
  skip_large_tables: false  # Skip validation for tables >1M rows
```

### Configuration Validation

- Required fields checking
- Type validation for all options
- Connection string validation
- Range validation for numeric values (batch_size, sample_size)

---

## Error Handling

### Connection Errors

**Strategy**:
- Retry logic with exponential backoff (3 attempts)
- Fail fast after retries exhausted
- Validate credentials on startup
- Clear error messages with troubleshooting hints

### Schema Conversion Errors

**Complex Objects** (procedures, triggers):
- Non-blocking - log warning
- Save original DB2 code to file
- Continue migration with placeholder comment in PostgreSQL

**Tables**:
- Blocking - migration stops
- Clear error message indicating which object failed
- Include reason and suggested fix

### Data Transfer Errors

**Batch-Level Handling**:
- Log error with table name, batch number, row range
- Mark table as failed in progress tracker
- Option to skip failed table if `continue_on_error: true`
- Log sample problematic rows for debugging

### Type Conversion Warnings

**Approximate Mappings**:
- Log warning for conversions that may lose precision
- Examples: GRAPHIC→VARCHAR, DECFLOAT with high precision
- Continue migration but flag for user review

---

## User Experience

### Startup

1. Display banner with tool name and version
2. Load and validate configuration
3. Test database connections with retry logic
4. Show migration plan summary:
   - Source: DB2 schema details
   - Target: PostgreSQL schema details
   - Estimated object counts (tables, views, procedures)
   - Batch size and mode settings

### During Migration

**Phase Indicators**: Current phase (e.g., "Phase 2/5: Converting schema objects")

**Per-Table Progress**: "Migrating table EMPLOYEES (5/127)"

**Progress Bars**: Real-time with tqdm showing:
- Current table rows transferred
- Transfer speed (rows/second)
- Elapsed time
- Estimated time remaining

**Color-Coded Messages**:
- Green: Success messages
- Yellow: Warnings (type conversions, skipped objects)
- Red: Errors (failures)

### Completion

**Summary Report**:
- Total duration
- Tables migrated successfully (count)
- Total rows transferred
- Warnings summary (count with examples)
- Errors summary (count with details)
- Validation results (passed/failed)

**Next Steps Guidance**:
- Review validation report location
- Check failed conversions directory
- Manual steps for unconverted procedures/triggers
- Recommendations for post-migration testing

---

## Dependencies

### Core Libraries

```
ibm_db>=3.1.0              # DB2 connectivity (requires IBM Data Server Driver)
psycopg2-binary>=2.9.0     # PostgreSQL connectivity
PyYAML>=6.0                # YAML parsing
click>=8.0.0               # CLI framework
tqdm>=4.65.0               # Progress bars
colorama>=0.4.6            # Colored output
jsonschema>=4.17.0         # Config validation
python-dotenv>=1.0.0       # .env file support
```

### Development Dependencies

```
pytest>=7.0.0              # Testing framework
pytest-mock>=3.10.0        # Mocking
black>=23.0.0              # Code formatting
flake8>=6.0.0              # Linting
```

### External Requirements

**IBM Data Server Driver**: Must be installed separately from IBM website. Not available via pip. Required for ibm_db to function.

**Platform-Specific Notes**:
- Linux: Requires `LD_LIBRARY_PATH` configuration
- Windows: Requires driver DLLs in PATH
- macOS: May need dylib configuration

---

## Installation & Deployment

### Installation

**Development Mode**:
```bash
pip install -e .
```

**Production Mode**:
```bash
pip install .
```

### Package Structure

**setup.py Configuration**:
- Entry point: `db2pgpy` command maps to `db2pgpy.cli:main`
- Python version requirement: >=3.8
- Semantic versioning (1.0.0 for initial release)

### Distribution

**Wheel Package**:
```bash
python setup.py bdist_wheel
```

**Future**: Upload to PyPI for `pip install db2pgpy`

---

## Testing Strategy

### Unit Tests

**Coverage**: 80%+ for core logic

**Components to Test**:
- Type converter with various DB2 types
- Config loader with valid/invalid YAML
- Progress tracker state save/load
- Schema converter DDL generation
- Error handling paths

**Approach**: Mock database connections to avoid requiring actual databases

### Integration Tests

**Optional**: Requires test database instances

**Setup**:
- Docker containers for PostgreSQL
- IBM DB2 Express-C or mock for DB2
- Small sample databases with varied schemas

**Tests**:
- Full migration flow end-to-end
- Schema creation verification
- Data transfer accuracy
- Validation correctness

### Test Data

**Fixtures**:
- Simple tables (basic types, PKs)
- Complex constraints (FKs, UNIQUEs, CHECKs)
- Various data types (all supported DB2 types)
- Edge cases (NULLs, special characters, large text, binary data)

---

## Documentation

### README.md

- Project overview and features
- Quick start guide
- Installation instructions (including IBM DB2 driver)
- Basic usage example
- Links to detailed documentation

### config.example.yaml

- Fully commented example configuration
- Shows all available options with explanations
- Copy-paste ready template

### MIGRATION_GUIDE.md

- Step-by-step migration walkthrough
- Pre-migration checklist
- Running schema-only first, then data-only approach
- Post-migration verification steps
- Troubleshooting common issues

### TYPE_MAPPING.md

- Complete DB2 to PostgreSQL type reference
- List of automatic conversions
- Known limitations
- Examples of edge cases requiring manual review

### DEVELOPMENT.md

- Setup development environment
- Running tests
- Contributing guidelines
- Architecture overview for developers

---

## Success Criteria

A successful migration meets the following criteria:

1. **Schema Migration**: All tables, indexes, constraints, views, sequences created in PostgreSQL
2. **Data Transfer**: All rows transferred with matching row counts
3. **Validation**: Row count validation passes, data sampling shows no mismatches
4. **Resume Capability**: Can resume from interruption without data loss
5. **Logging**: Clear logs showing progress, warnings, and errors
6. **Error Handling**: Graceful handling of failures with actionable error messages
7. **Performance**: Reasonable transfer speed (target: 10,000+ rows/second for simple tables)

---

## Known Limitations

1. **Procedural Code**: Complex DB2 procedures/triggers may require manual conversion
2. **DB2-Specific Functions**: Some DB2 built-in functions have no direct PostgreSQL equivalent
3. **Performance Features**: DB2 partitioning, compression settings not migrated
4. **Security**: Users, roles, permissions not migrated (PostgreSQL has different model)
5. **Temporal Tables**: DB2 temporal tables require manual conversion
6. **Complex Cursors**: Nested cursors may need manual review

---

## Future Enhancements

Potential features for future versions:

- Parallel table migration (multiple tables simultaneously)
- Incremental migration support (detect and migrate only changed data)
- Migration dry-run mode (simulate without making changes)
- Web UI for configuration and monitoring
- Advanced type mapping customization via config
- Support for other source databases (Oracle, SQL Server)
- Cloud database support (AWS RDS, Azure Database)

---

## Implementation Checklist

- [ ] Setup project structure and packaging
- [ ] Implement configuration loader with validation
- [ ] Implement DB2 connector with system catalog queries
- [ ] Implement PostgreSQL connector with bulk operations
- [ ] Implement schema extractor for tables, indexes, constraints
- [ ] Implement extractors for views, procedures, triggers, sequences
- [ ] Implement type mapper with comprehensive mappings
- [ ] Implement schema converter for DDL generation
- [ ] Implement PL/SQL converter with fallback strategy
- [ ] Implement data transfer with batch processing
- [ ] Implement progress tracker with resume capability
- [ ] Implement validator with multi-level verification
- [ ] Implement migrator orchestration logic
- [ ] Implement CLI commands with Click
- [ ] Implement logging with dual output
- [ ] Add migration mode support (full, schema_only, data_only)
- [ ] Write unit tests for core components
- [ ] Write integration tests (optional)
- [ ] Create documentation (README, guides, references)
- [ ] Create example configuration file
- [ ] Manual testing with sample DB2 database
- [ ] Performance optimization and tuning

---

**End of Design Document**
