# DB2 to PostgreSQL Type Mapping Reference

This document provides a comprehensive reference for data type conversions from IBM DB2 to PostgreSQL.

## Numeric Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| SMALLINT | SMALLINT | 2-byte integer, -32768 to 32767 |
| INTEGER / INT | INTEGER / INT | 4-byte integer, -2147483648 to 2147483647 |
| BIGINT | BIGINT | 8-byte integer, very large range |
| DECIMAL(p,s) | NUMERIC(p,s) | Exact numeric, precision p, scale s |
| NUMERIC(p,s) | NUMERIC(p,s) | Identical in both databases |
| REAL | REAL | 4-byte floating point |
| DOUBLE / FLOAT | DOUBLE PRECISION | 8-byte floating point |
| DECFLOAT(16) | DOUBLE PRECISION | 16-digit decimal floating point → 8-byte float |
| DECFLOAT(34) | NUMERIC(34,16) | 34-digit decimal floating point → high precision numeric |

### Notes:
- DB2 DECIMAL supports up to (31, s), PostgreSQL NUMERIC supports up to (1000, 1000)
- For exact decimal arithmetic, NUMERIC is preferred over floating point types
- DECFLOAT conversions may lose precision; review critical financial calculations

## String Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| CHAR(n) | CHAR(n) | Fixed-length character string |
| VARCHAR(n) | VARCHAR(n) | Variable-length character string |
| LONG VARCHAR | TEXT | Very long variable-length string |
| CLOB(n) | TEXT | Character large object |
| GRAPHIC(n) | CHAR(n) | Fixed-length double-byte string → CHAR |
| VARGRAPHIC(n) | VARCHAR(n) | Variable-length double-byte string → VARCHAR |
| DBCLOB(n) | TEXT | Double-byte character large object |

### Notes:
- PostgreSQL has no size limit on TEXT type
- CHAR is blank-padded in both databases
- VARCHAR max length: DB2 = 32704 bytes, PostgreSQL = 1GB
- For internationalization, use PostgreSQL's Unicode support with appropriate collation

## Date and Time Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| DATE | DATE | Calendar date (year, month, day) |
| TIME | TIME | Time of day without timezone |
| TIME WITH TIME ZONE | TIME WITH TIME ZONE | Time of day with timezone |
| TIMESTAMP | TIMESTAMP | Date and time without timezone |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP WITH TIME ZONE | Date and time with timezone (recommended) |

### Notes:
- PostgreSQL TIMESTAMP range: 4713 BC to 294276 AD
- DB2 TIMESTAMP range: 0001-01-01 to 9999-12-31
- Use TIMESTAMP WITH TIME ZONE for applications spanning timezones
- PostgreSQL stores timestamps in UTC internally

## Binary Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| BINARY(n) | BYTEA | Fixed-length binary data |
| VARBINARY(n) | BYTEA | Variable-length binary data |
| BLOB(n) | BYTEA | Binary large object |

### Notes:
- PostgreSQL BYTEA has no practical size limit
- Binary data is hex-encoded by default in PostgreSQL
- For large binary objects, consider using Large Objects (lo) or external storage

## Boolean Type

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| BOOLEAN | BOOLEAN | True/False value |

### Notes:
- DB2 BOOLEAN added in version 11.1
- PostgreSQL accepts 't', 'true', 'y', 'yes', '1' for TRUE
- PostgreSQL accepts 'f', 'false', 'n', 'no', '0' for FALSE

## XML Type

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| XML | XML | XML document |

### Notes:
- Both databases support XML data type
- PostgreSQL has built-in XML functions: xpath(), xml_is_well_formed(), etc.
- Verify XML validation rules are compatible

## JSON Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| JSON (text) | JSON or JSONB | JSON data |

### Notes:
- DB2 stores JSON as CLOB; PostgreSQL has native JSON type
- JSONB is recommended for PostgreSQL (binary, indexed, faster)
- Convert DB2 JSON CLOBs to PostgreSQL JSONB for better performance

## Array Types

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| ARRAY (limited) | ARRAY | Array of values |

### Notes:
- DB2 array support is limited compared to PostgreSQL
- PostgreSQL supports multi-dimensional arrays
- Example: INTEGER[] for array of integers

## Special DB2 Types

### ROWID
| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| ROWID | Omit or use SERIAL | DB2's internal row identifier |

**Recommendation**: Use PostgreSQL SERIAL or BIGSERIAL for auto-incrementing IDs instead.

### DATALINK
| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| DATALINK | VARCHAR | External file reference |

**Recommendation**: Store file paths as VARCHAR and manage file storage separately.

### User-Defined Types (UDT)

| DB2 Type | PostgreSQL Type | Notes |
|----------|----------------|-------|
| DISTINCT TYPE | DOMAIN or CREATE TYPE | User-defined type |
| STRUCTURED TYPE | CREATE TYPE | Complex object type |

**Recommendation**: Recreate UDTs in PostgreSQL using CREATE DOMAIN or CREATE TYPE.

Example:
```sql
-- DB2
CREATE DISTINCT TYPE SSN AS CHAR(11);

-- PostgreSQL
CREATE DOMAIN SSN AS CHAR(11) 
CHECK (VALUE ~ '^\d{3}-\d{2}-\d{4}$');
```

## Type Conversion Examples

### Numeric Conversions

```sql
-- DB2
CREATE TABLE products (
    id INTEGER,
    price DECIMAL(10,2),
    weight DOUBLE,
    quantity SMALLINT
);

-- PostgreSQL (converted)
CREATE TABLE products (
    id INTEGER,
    price NUMERIC(10,2),
    weight DOUBLE PRECISION,
    quantity SMALLINT
);
```

### String Conversions

```sql
-- DB2
CREATE TABLE customers (
    id INTEGER,
    code CHAR(10),
    name VARCHAR(100),
    description CLOB,
    notes GRAPHIC(50)
);

-- PostgreSQL (converted)
CREATE TABLE customers (
    id INTEGER,
    code CHAR(10),
    name VARCHAR(100),
    description TEXT,
    notes CHAR(50)  -- Review: GRAPHIC converted to CHAR
);
```

### Date/Time Conversions

```sql
-- DB2
CREATE TABLE events (
    id INTEGER,
    event_date DATE,
    event_time TIME,
    created_at TIMESTAMP
);

-- PostgreSQL (converted)
CREATE TABLE events (
    id INTEGER,
    event_date DATE,
    event_time TIME,
    created_at TIMESTAMP
);
```

### Binary Conversions

```sql
-- DB2
CREATE TABLE files (
    id INTEGER,
    file_data BLOB(1M),
    checksum BINARY(32)
);

-- PostgreSQL (converted)
CREATE TABLE files (
    id INTEGER,
    file_data BYTEA,
    checksum BYTEA
);
```

## Special Considerations

### Precision and Scale

**DB2 DECIMAL(31,5)** may need adjustment if it exceeds typical requirements:

```sql
-- If precision > 28 is rarely used
DECIMAL(31,5) → NUMERIC(28,5)

-- If full precision is required
DECIMAL(31,5) → NUMERIC(31,5)  -- PostgreSQL supports this
```

### Character Sets and Collation

DB2 and PostgreSQL handle character sets differently:

```sql
-- DB2
CREATE TABLE test (
    name VARCHAR(100) CCSID UNICODE
);

-- PostgreSQL
CREATE TABLE test (
    name VARCHAR(100) COLLATE "en_US.UTF-8"
);
```

**Recommendation**: Use UTF-8 encoding for PostgreSQL database to support international characters.

### Default Values

Ensure default values are compatible:

```sql
-- DB2
CREATE TABLE orders (
    id INTEGER,
    order_date DATE DEFAULT CURRENT DATE,
    status VARCHAR(20) DEFAULT 'PENDING'
);

-- PostgreSQL (converted)
CREATE TABLE orders (
    id INTEGER,
    order_date DATE DEFAULT CURRENT_DATE,
    status VARCHAR(20) DEFAULT 'PENDING'
);
```

### Identity Columns

```sql
-- DB2
CREATE TABLE products (
    id INTEGER GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(100)
);

-- PostgreSQL (use SERIAL or IDENTITY)
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100)
);

-- Or using SQL standard IDENTITY (PostgreSQL 10+)
CREATE TABLE products (
    id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name VARCHAR(100)
);
```

## Type Conversion Checklist

- [ ] Review all DECIMAL/NUMERIC precision and scale requirements
- [ ] Verify GRAPHIC/VARGRAPHIC conversions for internationalization needs
- [ ] Check CLOB/BLOB size requirements
- [ ] Confirm TIMESTAMP timezone handling
- [ ] Verify floating-point precision requirements
- [ ] Review and recreate user-defined types
- [ ] Check default value compatibility
- [ ] Verify identity column behavior
- [ ] Test character set and collation requirements
- [ ] Review XML and JSON data handling

## Common Conversion Issues and Solutions

### Issue 1: GRAPHIC Type Loss

**Problem**: GRAPHIC(n) stores double-byte characters but converts to CHAR(n)

**Solution**: 
```sql
-- Verify character data fits in CHAR(n)
-- Or use VARCHAR(n) with appropriate length
-- Ensure database encoding is UTF-8
```

### Issue 2: Decimal Precision

**Problem**: DECIMAL(31,10) may exceed typical display requirements

**Solution**:
```sql
-- Review actual data range
SELECT MAX(column_name), MIN(column_name) FROM table_name;

-- Adjust precision if possible
DECIMAL(31,10) → NUMERIC(18,10)
```

### Issue 3: TIMESTAMP Timezone

**Problem**: Ambiguous times during DST transitions

**Solution**:
```sql
-- Use TIMESTAMP WITH TIME ZONE
-- Store all times in UTC
-- Convert to local time in application
```

### Issue 4: BLOB Size

**Problem**: Very large BLOBs may cause performance issues

**Solution**:
```sql
-- Consider file system storage with path in database
-- Use PostgreSQL Large Objects (lo)
-- Or use BYTEA for smaller objects
```

## Performance Considerations

### Indexing

After type conversion, recreate indexes appropriately:

```sql
-- B-tree index (default, good for most types)
CREATE INDEX idx_name ON table_name(column_name);

-- GiST index for text search
CREATE INDEX idx_description ON products USING GiST(to_tsvector('english', description));

-- GIN index for JSONB
CREATE INDEX idx_data ON events USING GIN(data);

-- Hash index for equality comparisons
CREATE INDEX idx_code ON products USING HASH(code);
```

### Statistics

Update statistics after migration:

```sql
-- Analyze specific table
ANALYZE table_name;

-- Analyze all tables
ANALYZE;

-- Set statistics target for specific columns
ALTER TABLE table_name ALTER COLUMN column_name SET STATISTICS 1000;
```

## Testing Type Conversions

### Validation Queries

```sql
-- Check for data truncation
SELECT column_name, LENGTH(column_name) as len
FROM table_name
ORDER BY len DESC
LIMIT 10;

-- Verify numeric ranges
SELECT 
    MIN(numeric_column) as min_val,
    MAX(numeric_column) as max_val,
    AVG(numeric_column) as avg_val
FROM table_name;

-- Check date ranges
SELECT 
    MIN(date_column) as earliest,
    MAX(date_column) as latest
FROM table_name;

-- Verify NULL handling
SELECT 
    COUNT(*) as total_rows,
    COUNT(column_name) as non_null_rows,
    COUNT(*) - COUNT(column_name) as null_rows
FROM table_name;
```

## Additional Resources

- [PostgreSQL Data Types Documentation](https://www.postgresql.org/docs/current/datatype.html)
- [IBM DB2 Data Types Documentation](https://www.ibm.com/docs/en/db2)
- [PostgreSQL Type Conversion Functions](https://www.postgresql.org/docs/current/functions-formatting.html)

## Version Compatibility

| Component | Minimum Version | Recommended Version |
|-----------|----------------|---------------------|
| DB2 | 9.7 | 11.5 or higher |
| PostgreSQL | 10 | 14 or higher |
| Python | 3.8 | 3.10 or higher |

## Summary

This mapping guide covers the most common type conversions. For edge cases or DB2-specific features:

1. Test conversions in a development environment
2. Review actual data ranges and requirements
3. Consult DB2 and PostgreSQL documentation
4. Consider manual schema adjustments for complex types
5. Validate data integrity after conversion

For questions or issues with specific type conversions, refer to the troubleshooting section in MIGRATION_GUIDE.md or open a GitHub issue.
