# DB2 to PostgreSQL Migration Guide

This guide provides step-by-step instructions for migrating your IBM DB2 database to PostgreSQL using db2pgpy.

## Table of Contents

1. [Pre-Migration Checklist](#pre-migration-checklist)
2. [Installation](#installation)
3. [Migration Steps](#migration-steps)
4. [Post-Migration Tasks](#post-migration-tasks)
5. [Troubleshooting](#troubleshooting)
6. [Best Practices](#best-practices)

## Pre-Migration Checklist

Before starting your migration, ensure:

### Database Assessment

- [ ] Identify all tables, views, procedures, and sequences to migrate
- [ ] Document DB2-specific features and dependencies
- [ ] Check for non-standard data types or custom types
- [ ] Identify large tables that may require special handling
- [ ] Review stored procedures for DB2-specific syntax

### Environment Preparation

- [ ] Install Python 3.8 or higher
- [ ] Install IBM DB2 Client Libraries
- [ ] Install PostgreSQL server (10 or higher)
- [ ] Verify network connectivity between all systems
- [ ] Ensure sufficient disk space for migration (at least 2x source database size)
- [ ] Set up backup strategies for both source and target databases

### Access Requirements

- [ ] DB2 read access with permissions to:
  - SYSCAT views
  - All tables/views/sequences to migrate
  - Stored procedure definitions
- [ ] PostgreSQL write access with permissions to:
  - Create schemas
  - Create tables, views, sequences
  - Insert data
  - Create indexes and constraints

## Installation

### Step 1: Install IBM DB2 Client

```bash
# Download IBM Data Server Driver from IBM website
# Follow IBM's installation instructions for your platform

# On Linux, you may need to set library paths:
export LD_LIBRARY_PATH=/opt/ibm/db2/clidriver/lib:$LD_LIBRARY_PATH
```

### Step 2: Install db2pgpy

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install from source
git clone https://github.com/yourusername/db2pgpy.git
cd db2pgpy
pip install -r requirements.txt
pip install -e .

# Verify installation
db2pgpy --version
```

## Migration Steps

### Step 1: Create Configuration File

Create a YAML configuration file (`migration_config.yaml`):

```yaml
db2:
  host: db2-prod.example.com
  port: 50000
  database: PRODDB
  user: migration_user
  password: ${DB2_PASSWORD}  # Use environment variable for security
  schema: MYSCHEMA

postgresql:
  host: pg-prod.example.com
  port: 5432
  database: proddb
  user: migration_user
  password: ${PG_PASSWORD}
  schema: public

migration:
  # List all tables to migrate
  tables:
    - CUSTOMERS
    - ORDERS
    - ORDER_ITEMS
    - PRODUCTS
    - INVENTORY
  
  # Adjust batch size based on table size and network
  batch_size: 1000  # Increase for faster migration, decrease if issues occur
  
  # Parallel workers (use with caution)
  parallel_workers: 1  # Start with 1, increase after testing
  
  # Continue on error (not recommended for production)
  continue_on_error: false

validation:
  enabled: true
  row_count: true
  data_sampling: true
  sample_size: 100

resume:
  enabled: true
  checkpoint_file: .db2pgpy_checkpoint.json

logging:
  level: INFO
  file: migration.log
  console: true
```

### Step 2: Test Configuration

Before running the migration, validate your configuration:

```bash
# Set passwords as environment variables (more secure)
export DB2_PASSWORD='your_db2_password'
export PG_PASSWORD='your_pg_password'

# Validate configuration and connections
db2pgpy validate --config migration_config.yaml
```

Expected output:
```
[INFO] Loading configuration from migration_config.yaml
[INFO] Validating configuration...
[INFO] ✓ Configuration is valid
[INFO] Testing database connections...
[INFO] ✓ DB2 connection successful
[INFO] ✓ PostgreSQL connection successful
[INFO] ✓ Validation completed successfully
```

### Step 3: Run Test Migration

Perform a test migration with a small table first:

```bash
# Migrate a single small table
db2pgpy migrate --config migration_config.yaml --tables TEST_TABLE

# Review logs
tail -f migration.log
```

### Step 4: Schema-Only Migration

Migrate the schema first to catch any structural issues:

```bash
db2pgpy migrate --config migration_config.yaml --schema-only
```

This will:
- Extract table structures from DB2
- Convert data types to PostgreSQL equivalents
- Create tables in PostgreSQL
- Create primary keys
- Log any conversion warnings

Review the logs for:
- Type conversion warnings
- Unsupported features
- Schema creation errors

### Step 5: Manual Schema Adjustments

After schema migration, review and adjust:

```sql
-- Connect to PostgreSQL
psql -h pg-prod.example.com -U migration_user -d proddb

-- Check created tables
\dt

-- Review table structure
\d+ customers

-- Add any manual adjustments needed
-- Example: Adding indexes that couldn't be auto-converted
CREATE INDEX idx_customer_email ON customers(email);

-- Example: Adjusting column types if needed
ALTER TABLE orders ALTER COLUMN amount TYPE NUMERIC(12,2);
```

### Step 6: Data Migration

After schema is verified, migrate the data:

```bash
# Full data migration
db2pgpy migrate --config migration_config.yaml --data-only

# Monitor progress
tail -f migration.log

# In another terminal, check progress
cat .db2pgpy_checkpoint.json
```

### Step 7: Validation

The tool automatically validates during migration, but you can also manually verify:

```sql
-- In DB2
SELECT COUNT(*) FROM MYSCHEMA.CUSTOMERS;

-- In PostgreSQL
SELECT COUNT(*) FROM public.customers;

-- Check for data discrepancies
SELECT * FROM public.customers ORDER BY id LIMIT 10;
```

### Step 8: Handle Stored Procedures

The tool provides best-effort PL/SQL conversion. Review converted procedures:

```bash
# Check the failed_conversions directory
ls -l failed_conversions/

# Review each file
cat failed_conversions/GET_CUSTOMER_conversion.sql
```

Manually convert complex procedures:
1. Review the attempted conversion
2. Adjust PostgreSQL-specific syntax
3. Test in a development environment
4. Deploy to production

### Step 9: Migrate Additional Objects

Migrate views, sequences, and other objects as needed:

```python
# Use Python API for custom migrations
from db2pgpy.extractors.views import ViewExtractor
from db2pgpy.extractors.sequences import SequenceExtractor
from db2pgpy.connectors.db2 import DB2Connector

# Setup connector
db2_conn = DB2Connector(config=db2_config)
db2_conn.connect()

# Extract views
view_extractor = ViewExtractor(db2_conn)
views = view_extractor.extract_views(schema='MYSCHEMA')

# Review and create views manually in PostgreSQL
for view in views:
    print(f"View: {view['name']}")
    print(f"Definition: {view['definition']}")
```

## Post-Migration Tasks

### 1. Update Statistics

```sql
-- In PostgreSQL, analyze tables
ANALYZE customers;
ANALYZE orders;
-- Or analyze all tables
ANALYZE;
```

### 2. Create Indexes

```sql
-- Create indexes based on DB2 indexes
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_date ON orders(order_date);

-- Check index usage
SELECT * FROM pg_stat_user_indexes;
```

### 3. Set Up Foreign Keys

```sql
-- Add foreign key constraints
ALTER TABLE orders 
ADD CONSTRAINT fk_orders_customer 
FOREIGN KEY (customer_id) REFERENCES customers(id);
```

### 4. Configure Sequences

```sql
-- Reset sequence values if needed
SELECT setval('customers_id_seq', (SELECT MAX(id) FROM customers));
```

### 5. Update Application Connection Strings

Update your applications to use PostgreSQL:

```python
# Before (DB2)
import ibm_db
conn = ibm_db.connect("DATABASE=proddb;HOSTNAME=db2-server;...", "", "")

# After (PostgreSQL)
import psycopg2
conn = psycopg2.connect(
    host="pg-server",
    port=5432,
    database="proddb",
    user="appuser",
    password="password"
)
```

### 6. Performance Tuning

```sql
-- Tune PostgreSQL configuration
-- Edit postgresql.conf

shared_buffers = 256MB              # Adjust based on RAM
effective_cache_size = 1GB          # Adjust based on RAM
maintenance_work_mem = 64MB
checkpoint_completion_target = 0.9
wal_buffers = 16MB
default_statistics_target = 100
```

### 7. Set Up Monitoring

```sql
-- Enable query logging
ALTER DATABASE proddb SET log_statement = 'all';

-- Monitor slow queries
CREATE EXTENSION pg_stat_statements;
```

## Troubleshooting

### Connection Issues

**Problem**: Cannot connect to DB2

```
ERROR: Connection attempt failed: [IBM][CLI Driver] SQL30081N
```

**Solutions**:
1. Verify DB2 server is running: `db2 list applications`
2. Check firewall rules: `telnet db2-server 50000`
3. Verify credentials
4. Check DB2 client configuration

**Problem**: Cannot connect to PostgreSQL

```
ERROR: Connection failed: could not connect to server
```

**Solutions**:
1. Verify PostgreSQL is running: `systemctl status postgresql`
2. Check pg_hba.conf for access rules
3. Verify postgresql.conf listen_addresses setting
4. Check firewall rules

### Type Conversion Issues

**Problem**: Unsupported data type

```
WARNING: Type conversion not found for DB2 type: GRAPHIC
```

**Solutions**:
1. Check TYPE_MAPPING.md for alternatives
2. Manually create the column with appropriate type
3. Use VARCHAR as fallback for text types

**Problem**: Decimal precision loss

```
WARNING: Decimal(31,5) exceeds PostgreSQL maximum
```

**Solutions**:
1. PostgreSQL NUMERIC supports up to (1000, 1000)
2. Review if extreme precision is needed
3. Adjust application logic if needed

### Data Transfer Issues

**Problem**: Out of memory during migration

```
ERROR: Out of memory
```

**Solutions**:
1. Reduce batch_size in configuration (e.g., from 1000 to 100)
2. Increase available memory
3. Migrate large tables separately

**Problem**: Slow migration speed

**Solutions**:
1. Increase batch_size (e.g., from 1000 to 5000)
2. Temporarily disable indexes during data load
3. Increase PostgreSQL shared_buffers and maintenance_work_mem
4. Use parallel_workers (with caution)

**Problem**: Data truncation errors

```
ERROR: value too long for type character varying(50)
```

**Solutions**:
1. Check actual data lengths in DB2
2. Adjust PostgreSQL column length
3. Review data for unexpected values

### Resume Issues

**Problem**: Cannot resume migration

```
ERROR: State file corrupted
```

**Solutions**:
1. Delete checkpoint file and restart
2. Check disk space
3. Verify file permissions

## Best Practices

### Planning

1. **Start Small**: Test with non-critical tables first
2. **Document Everything**: Keep notes of manual changes
3. **Use Version Control**: Store configuration files in git
4. **Test Thoroughly**: Verify in dev/staging before production

### During Migration

1. **Monitor Progress**: Watch logs and checkpoint files
2. **Verify Data**: Spot-check migrated data regularly
3. **Backup**: Keep backups of both source and target
4. **Off-Peak Hours**: Run large migrations during low-traffic periods

### After Migration

1. **Parallel Running**: Run both databases in parallel initially
2. **Performance Testing**: Compare query performance
3. **Application Testing**: Test all application features
4. **Rollback Plan**: Have a clear rollback strategy

### Security

1. **Use Environment Variables**: Don't hardcode passwords
2. **Secure Connections**: Use SSL/TLS when possible
3. **Least Privilege**: Use dedicated migration users with minimal permissions
4. **Audit Logging**: Keep detailed logs of the migration process

### Performance

1. **Batch Sizing**: Start conservative, then optimize
2. **Network Bandwidth**: Ensure adequate bandwidth between servers
3. **Index Strategy**: Consider disabling indexes during load, rebuild after
4. **Vacuum and Analyze**: Run after migration completes

### Validation

1. **Row Counts**: Always verify row counts match
2. **Data Sampling**: Spot-check random rows
3. **Aggregates**: Compare SUM, AVG, MAX, MIN values
4. **Application Testing**: Test critical business workflows

## Migration Checklist

- [ ] Pre-migration assessment complete
- [ ] Test environment migration successful
- [ ] Configuration file created and validated
- [ ] Database connections tested
- [ ] Schema-only migration completed
- [ ] Schema manually reviewed and adjusted
- [ ] Data migration completed
- [ ] Validation passed
- [ ] Stored procedures converted and tested
- [ ] Views and sequences migrated
- [ ] Indexes created
- [ ] Foreign keys established
- [ ] Statistics updated
- [ ] Application connection strings updated
- [ ] Application testing completed
- [ ] Performance tuning applied
- [ ] Monitoring configured
- [ ] Documentation updated
- [ ] Rollback plan documented
- [ ] Go-live approved

## Getting Help

If you encounter issues:

1. Check the logs: `tail -f migration.log`
2. Review this guide's troubleshooting section
3. Consult the TYPE_MAPPING.md for type issues
4. Search existing GitHub issues
5. Create a new GitHub issue with:
   - Error messages
   - Configuration file (sanitized)
   - Log excerpts
   - DB2 and PostgreSQL versions
