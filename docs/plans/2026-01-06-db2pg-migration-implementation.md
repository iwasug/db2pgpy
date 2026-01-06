# DB2 to PostgreSQL Migration Tool - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a production-ready CLI tool that migrates DB2 databases to PostgreSQL with full schema, data transfer, and validation.

**Architecture:** Modular component design with clear separation: connectors wrap database access, extractors query metadata, converters transform DB2→PostgreSQL, migrator orchestrates phases, validator ensures correctness.

**Tech Stack:** Python 3.8+, Click (CLI), ibm_db (DB2), psycopg2 (PostgreSQL), PyYAML (config), tqdm (progress), pytest (testing)

---

## Task 1: Project Foundation

**Files:**
- Create: `setup.py`
- Create: `requirements.txt`
- Create: `db2pgpy/__init__.py`
- Create: `tests/__init__.py`
- Create: `tests/unit/__init__.py`

**Step 1: Write setup.py**

```python
from setuptools import setup, find_packages

setup(
    name="db2pgpy",
    version="1.0.0",
    description="CLI tool for migrating DB2 databases to PostgreSQL",
    author="Your Name",
    packages=find_packages(),
    install_requires=[
        "ibm_db>=3.1.0",
        "psycopg2-binary>=2.9.0",
        "PyYAML>=6.0",
        "click>=8.0.0",
        "tqdm>=4.65.0",
        "colorama>=0.4.6",
        "jsonschema>=4.17.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-mock>=3.10.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "db2pgpy=db2pgpy.cli:main",
        ],
    },
    python_requires=">=3.8",
)
```

**Step 2: Write requirements.txt**

```
ibm_db>=3.1.0
psycopg2-binary>=2.9.0
PyYAML>=6.0
click>=8.0.0
tqdm>=4.65.0
colorama>=0.4.6
jsonschema>=4.17.0
python-dotenv>=1.0.0
pytest>=7.0.0
pytest-mock>=3.10.0
black>=23.0.0
flake8>=6.0.0
```

**Step 3: Create package __init__.py files**

Create empty `db2pgpy/__init__.py`:
```python
"""DB2 to PostgreSQL migration tool."""
__version__ = "1.0.0"
```

Create empty `tests/__init__.py` and `tests/unit/__init__.py`

**Step 4: Commit**

```bash
git add setup.py requirements.txt db2pgpy/__init__.py tests/
git commit -m "feat: add project foundation with setup.py and requirements"
```

---

## Task 2: Configuration Module

**Files:**
- Create: `db2pgpy/config.py`
- Create: `tests/unit/test_config.py`
- Create: `config.example.yaml`

**Step 1: Write failing test for config loading**

Create `tests/unit/test_config.py`:
```python
import pytest
from db2pgpy.config import Config


def test_load_valid_config(tmp_path):
    """Test loading a valid YAML config file."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
db2:
  host: localhost
  port: 50000
  database: SAMPLE
  user: db2admin
  password: secret
  schema: DB2INST1

postgresql:
  host: localhost
  port: 5432
  database: migrated
  user: postgres
  password: pgpass
  schema: public

migration:
  mode: full
  batch_size: 1000
  continue_on_error: false
""")
    
    config = Config.load(str(config_file))
    
    assert config.db2.host == "localhost"
    assert config.db2.port == 50000
    assert config.postgresql.database == "migrated"
    assert config.migration.batch_size == 1000


def test_load_missing_required_field(tmp_path):
    """Test loading config with missing required field."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
db2:
  host: localhost
postgresql:
  host: localhost
""")
    
    with pytest.raises(ValueError, match="Missing required field"):
        Config.load(str(config_file))


def test_load_nonexistent_file():
    """Test loading a non-existent config file."""
    with pytest.raises(FileNotFoundError):
        Config.load("/nonexistent/config.yaml")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_config.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'db2pgpy.config'"

**Step 3: Implement Config class**

Create `db2pgpy/config.py`:
```python
"""Configuration management for DB2 to PostgreSQL migration."""
import os
from dataclasses import dataclass
from typing import Optional
import yaml


@dataclass
class DB2Config:
    """DB2 connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: Optional[str] = None


@dataclass
class PostgreSQLConfig:
    """PostgreSQL connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "public"


@dataclass
class MigrationConfig:
    """Migration settings."""
    mode: str = "full"
    batch_size: int = 1000
    continue_on_error: bool = False
    save_failed_objects: bool = True
    failed_objects_dir: str = "./failed_conversions"


@dataclass
class ResumeConfig:
    """Resume settings."""
    enabled: bool = True
    state_file: str = ".db2pgpy_progress.json"


@dataclass
class LoggingConfig:
    """Logging settings."""
    level: str = "INFO"
    console: bool = True
    file: str = "migration.log"
    show_sql: bool = False


@dataclass
class ValidationConfig:
    """Validation settings."""
    enabled: bool = True
    sample_size: int = 100
    skip_large_tables: bool = False


@dataclass
class Config:
    """Complete configuration."""
    db2: DB2Config
    postgresql: PostgreSQLConfig
    migration: MigrationConfig
    resume: ResumeConfig
    logging: LoggingConfig
    validation: ValidationConfig

    @classmethod
    def load(cls, config_path: str) -> "Config":
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Config object
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If required fields are missing
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Config file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        # Validate required fields
        required_fields = {
            'db2': ['host', 'port', 'database', 'user', 'password'],
            'postgresql': ['host', 'port', 'database', 'user', 'password']
        }
        
        for section, fields in required_fields.items():
            if section not in data:
                raise ValueError(f"Missing required field: {section}")
            for field in fields:
                if field not in data[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")
        
        # Build config objects with defaults
        db2_config = DB2Config(
            host=data['db2']['host'],
            port=data['db2']['port'],
            database=data['db2']['database'],
            user=data['db2']['user'],
            password=data['db2']['password'],
            schema=data['db2'].get('schema')
        )
        
        postgresql_config = PostgreSQLConfig(
            host=data['postgresql']['host'],
            port=data['postgresql']['port'],
            database=data['postgresql']['database'],
            user=data['postgresql']['user'],
            password=data['postgresql']['password'],
            schema=data['postgresql'].get('schema', 'public')
        )
        
        migration_data = data.get('migration', {})
        migration_config = MigrationConfig(
            mode=migration_data.get('mode', 'full'),
            batch_size=migration_data.get('batch_size', 1000),
            continue_on_error=migration_data.get('continue_on_error', False),
            save_failed_objects=migration_data.get('save_failed_objects', True),
            failed_objects_dir=migration_data.get('failed_objects_dir', './failed_conversions')
        )
        
        resume_data = data.get('resume', {})
        resume_config = ResumeConfig(
            enabled=resume_data.get('enabled', True),
            state_file=resume_data.get('state_file', '.db2pgpy_progress.json')
        )
        
        logging_data = data.get('logging', {})
        logging_config = LoggingConfig(
            level=logging_data.get('level', 'INFO'),
            console=logging_data.get('console', True),
            file=logging_data.get('file', 'migration.log'),
            show_sql=logging_data.get('show_sql', False)
        )
        
        validation_data = data.get('validation', {})
        validation_config = ValidationConfig(
            enabled=validation_data.get('enabled', True),
            sample_size=validation_data.get('sample_size', 100),
            skip_large_tables=validation_data.get('skip_large_tables', False)
        )
        
        return cls(
            db2=db2_config,
            postgresql=postgresql_config,
            migration=migration_config,
            resume=resume_config,
            logging=logging_config,
            validation=validation_config
        )
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_config.py -v`
Expected: 3 tests PASS

**Step 5: Create example config file**

Create `config.example.yaml`:
```yaml
# DB2 Source Database Configuration
db2:
  host: localhost
  port: 50000
  database: SAMPLE
  user: db2admin
  password: your_password_here
  schema: DB2INST1  # Optional: specific schema to migrate

# PostgreSQL Target Database Configuration
postgresql:
  host: localhost
  port: 5432
  database: migrated_db
  user: postgres
  password: your_password_here
  schema: public  # Target schema

# Migration Settings
migration:
  mode: full  # Options: full, schema_only, data_only
  batch_size: 1000  # Rows per batch for data transfer
  continue_on_error: false  # Continue if table migration fails
  save_failed_objects: true  # Save unconverted procedures/triggers
  failed_objects_dir: ./failed_conversions

# Resume Configuration
resume:
  enabled: true
  state_file: .db2pgpy_progress.json

# Logging Configuration
logging:
  level: INFO  # DEBUG, INFO, WARNING, ERROR
  console: true
  file: migration.log
  show_sql: false  # Log SQL statements for debugging

# Validation Configuration
validation:
  enabled: true
  sample_size: 100  # Rows to sample per table
  skip_large_tables: false  # Skip validation for tables >1M rows
```

**Step 6: Commit**

```bash
git add db2pgpy/config.py tests/unit/test_config.py config.example.yaml
git commit -m "feat: add configuration module with YAML loading and validation"
```

---

## Task 3: Logger Module

**Files:**
- Create: `db2pgpy/logger.py`
- Create: `tests/unit/test_logger.py`

**Step 1: Write failing test for logger**

Create `tests/unit/test_logger.py`:
```python
import logging
import pytest
from db2pgpy.logger import setup_logger


def test_setup_logger_console_only(caplog):
    """Test logger with console output only."""
    logger = setup_logger("test", level="INFO", console=True, log_file=None)
    
    with caplog.at_level(logging.INFO):
        logger.info("Test message")
    
    assert "Test message" in caplog.text
    assert logger.level == logging.INFO


def test_setup_logger_with_file(tmp_path):
    """Test logger with file output."""
    log_file = tmp_path / "test.log"
    logger = setup_logger("test", level="DEBUG", console=False, log_file=str(log_file))
    
    logger.debug("Debug message")
    logger.info("Info message")
    
    assert log_file.exists()
    content = log_file.read_text()
    assert "Debug message" in content
    assert "Info message" in content


def test_logger_color_formatting():
    """Test that colored formatter works."""
    logger = setup_logger("test", level="INFO", console=True)
    
    # Should not raise exception
    logger.info("Info")
    logger.warning("Warning")
    logger.error("Error")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_logger.py -v`
Expected: FAIL with "ModuleNotFoundError: No module named 'db2pgpy.logger'"

**Step 3: Implement logger module**

Create `db2pgpy/logger.py`:
```python
"""Logging utilities with colored console output."""
import logging
import sys
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored output."""
    
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.GREEN,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        """Format log record with color."""
        color = self.COLORS.get(record.levelname, '')
        record.levelname = f"{color}{record.levelname}{Style.RESET_ALL}"
        return super().format(record)


def setup_logger(
    name: str,
    level: str = "INFO",
    console: bool = True,
    log_file: str = None,
    show_sql: bool = False
) -> logging.Logger:
    """Setup logger with console and/or file output.
    
    Args:
        name: Logger name
        level: Log level (DEBUG, INFO, WARNING, ERROR)
        console: Enable console output
        log_file: Path to log file (None to disable)
        show_sql: Show SQL statements in logs
        
    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper()))
    logger.handlers = []  # Clear existing handlers
    
    # Console handler with colors
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_formatter = ColoredFormatter(
            '%(levelname)s: %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)  # File gets all logs
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_logger.py -v`
Expected: 3 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/logger.py tests/unit/test_logger.py
git commit -m "feat: add logger module with colored console output"
```

---

## Task 4: Type Converter Module

**Files:**
- Create: `db2pgpy/converters/__init__.py`
- Create: `db2pgpy/converters/types.py`
- Create: `tests/unit/test_type_converter.py`

**Step 1: Write failing tests for type conversion**

Create `tests/unit/test_type_converter.py`:
```python
import pytest
from db2pgpy.converters.types import TypeMapper


def test_convert_numeric_types():
    """Test conversion of DB2 numeric types to PostgreSQL."""
    mapper = TypeMapper()
    
    assert mapper.convert("SMALLINT") == "SMALLINT"
    assert mapper.convert("INTEGER") == "INTEGER"
    assert mapper.convert("BIGINT") == "BIGINT"
    assert mapper.convert("DECIMAL(10,2)") == "NUMERIC(10,2)"
    assert mapper.convert("REAL") == "REAL"
    assert mapper.convert("DOUBLE") == "DOUBLE PRECISION"


def test_convert_string_types():
    """Test conversion of DB2 string types to PostgreSQL."""
    mapper = TypeMapper()
    
    assert mapper.convert("CHAR(50)") == "CHAR(50)"
    assert mapper.convert("VARCHAR(255)") == "VARCHAR(255)"
    assert mapper.convert("CLOB") == "TEXT"


def test_convert_datetime_types():
    """Test conversion of DB2 date/time types to PostgreSQL."""
    mapper = TypeMapper()
    
    assert mapper.convert("DATE") == "DATE"
    assert mapper.convert("TIME") == "TIME"
    assert mapper.convert("TIMESTAMP") == "TIMESTAMP"


def test_convert_binary_types():
    """Test conversion of DB2 binary types to PostgreSQL."""
    mapper = TypeMapper()
    
    assert mapper.convert("BLOB") == "BYTEA"
    assert mapper.convert("BINARY(100)") == "BYTEA"


def test_convert_with_warning():
    """Test conversion that generates warning."""
    mapper = TypeMapper()
    
    pg_type, warning = mapper.convert_with_warning("GRAPHIC(50)")
    assert pg_type == "VARCHAR(100)"
    assert warning is not None
    assert "GRAPHIC" in warning


def test_unsupported_type():
    """Test handling of unsupported type."""
    mapper = TypeMapper()
    
    with pytest.raises(ValueError, match="Unsupported DB2 type"):
        mapper.convert("UNKNOWN_TYPE")
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_type_converter.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement TypeMapper class**

Create `db2pgpy/converters/__init__.py` (empty file)

Create `db2pgpy/converters/types.py`:
```python
"""DB2 to PostgreSQL type mapping."""
import re
from typing import Tuple, Optional


class TypeMapper:
    """Maps DB2 data types to PostgreSQL equivalents."""
    
    # Direct mappings (no transformation needed)
    DIRECT_MAPPINGS = {
        'SMALLINT': 'SMALLINT',
        'INTEGER': 'INTEGER',
        'INT': 'INTEGER',
        'BIGINT': 'BIGINT',
        'REAL': 'REAL',
        'DOUBLE': 'DOUBLE PRECISION',
        'FLOAT': 'DOUBLE PRECISION',
        'DATE': 'DATE',
        'TIME': 'TIME',
        'TIMESTAMP': 'TIMESTAMP',
        'BOOLEAN': 'BOOLEAN',
        'BLOB': 'BYTEA',
        'CLOB': 'TEXT',
        'DBCLOB': 'TEXT',
        'XML': 'XML',
    }
    
    # Pattern-based mappings
    PATTERN_MAPPINGS = [
        (r'^CHAR\((\d+)\)$', r'CHAR(\1)'),
        (r'^VARCHAR\((\d+)\)$', r'VARCHAR(\1)'),
        (r'^DECIMAL\((\d+),(\d+)\)$', r'NUMERIC(\1,\2)'),
        (r'^NUMERIC\((\d+),(\d+)\)$', r'NUMERIC(\1,\2)'),
        (r'^BINARY\((\d+)\)$', r'BYTEA'),
        (r'^VARBINARY\((\d+)\)$', r'BYTEA'),
    ]
    
    # Mappings that need warnings
    WARNING_MAPPINGS = {
        'GRAPHIC': lambda size: (f'VARCHAR({int(size)*2})', 
                                 'GRAPHIC converted to VARCHAR - review for Unicode compatibility'),
        'VARGRAPHIC': lambda size: (f'VARCHAR({int(size)*2})', 
                                    'VARGRAPHIC converted to VARCHAR - review for Unicode compatibility'),
        'DECFLOAT': lambda: ('NUMERIC', 
                            'DECFLOAT converted to NUMERIC - may lose precision'),
    }

    def convert(self, db2_type: str) -> str:
        """Convert DB2 type to PostgreSQL type.
        
        Args:
            db2_type: DB2 data type string
            
        Returns:
            PostgreSQL data type string
            
        Raises:
            ValueError: If type is unsupported
        """
        result, _ = self.convert_with_warning(db2_type)
        return result

    def convert_with_warning(self, db2_type: str) -> Tuple[str, Optional[str]]:
        """Convert DB2 type to PostgreSQL type with optional warning.
        
        Args:
            db2_type: DB2 data type string
            
        Returns:
            Tuple of (postgresql_type, warning_message)
            
        Raises:
            ValueError: If type is unsupported
        """
        db2_type = db2_type.strip().upper()
        
        # Check direct mappings
        if db2_type in self.DIRECT_MAPPINGS:
            return self.DIRECT_MAPPINGS[db2_type], None
        
        # Check pattern mappings
        for pattern, replacement in self.PATTERN_MAPPINGS:
            match = re.match(pattern, db2_type)
            if match:
                pg_type = re.sub(pattern, replacement, db2_type)
                return pg_type, None
        
        # Check warning mappings
        for db2_base, converter in self.WARNING_MAPPINGS.items():
            if db2_type.startswith(db2_base):
                # Extract size if present
                match = re.search(r'\((\d+)\)', db2_type)
                if match:
                    pg_type, warning = converter(match.group(1))
                else:
                    pg_type, warning = converter()
                return pg_type, warning
        
        # Unsupported type
        raise ValueError(f"Unsupported DB2 type: {db2_type}")
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_type_converter.py -v`
Expected: 6 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/converters/ tests/unit/test_type_converter.py
git commit -m "feat: add type mapper for DB2 to PostgreSQL conversion"
```

---

## Task 5: Progress Tracker Module

**Files:**
- Create: `db2pgpy/progress.py`
- Create: `tests/unit/test_progress.py`

**Step 1: Write failing tests for progress tracker**

Create `tests/unit/test_progress.py`:
```python
import json
import pytest
from db2pgpy.progress import ProgressTracker


def test_initialize_new_tracker(tmp_path):
    """Test creating a new progress tracker."""
    state_file = tmp_path / "progress.json"
    tracker = ProgressTracker(str(state_file), mode="full")
    
    assert tracker.mode == "full"
    assert len(tracker.completed_tables) == 0
    assert tracker.current_table is None


def test_mark_table_completed(tmp_path):
    """Test marking a table as completed."""
    state_file = tmp_path / "progress.json"
    tracker = ProgressTracker(str(state_file), mode="full")
    
    tracker.mark_table_completed("EMPLOYEES")
    tracker.save()
    
    assert "EMPLOYEES" in tracker.completed_tables
    assert state_file.exists()


def test_resume_from_state(tmp_path):
    """Test resuming from saved state."""
    state_file = tmp_path / "progress.json"
    
    # Create initial tracker and save state
    tracker1 = ProgressTracker(str(state_file), mode="full")
    tracker1.mark_table_completed("TABLE1")
    tracker1.mark_table_completed("TABLE2")
    tracker1.save()
    
    # Resume from state
    tracker2 = ProgressTracker.resume(str(state_file))
    
    assert tracker2.mode == "full"
    assert "TABLE1" in tracker2.completed_tables
    assert "TABLE2" in tracker2.completed_tables


def test_is_table_completed(tmp_path):
    """Test checking if table is completed."""
    state_file = tmp_path / "progress.json"
    tracker = ProgressTracker(str(state_file), mode="full")
    
    tracker.mark_table_completed("DONE_TABLE")
    
    assert tracker.is_table_completed("DONE_TABLE")
    assert not tracker.is_table_completed("PENDING_TABLE")


def test_delete_state_file(tmp_path):
    """Test deleting state file on completion."""
    state_file = tmp_path / "progress.json"
    tracker = ProgressTracker(str(state_file), mode="full")
    tracker.save()
    
    assert state_file.exists()
    
    tracker.delete()
    
    assert not state_file.exists()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_progress.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement ProgressTracker class**

Create `db2pgpy/progress.py`:
```python
"""Progress tracking and resume capability."""
import json
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict, Any


@dataclass
class ProgressTracker:
    """Tracks migration progress for resume capability."""
    
    state_file: str
    mode: str
    completed_tables: List[str] = field(default_factory=list)
    current_table: Optional[str] = None
    failed_objects: Dict[str, str] = field(default_factory=dict)
    start_time: Optional[str] = None
    last_update: Optional[str] = None
    
    def __post_init__(self):
        """Initialize start time."""
        if self.start_time is None:
            self.start_time = datetime.now().isoformat()
        self.last_update = datetime.now().isoformat()
    
    def mark_table_completed(self, table_name: str):
        """Mark a table as completed.
        
        Args:
            table_name: Name of completed table
        """
        if table_name not in self.completed_tables:
            self.completed_tables.append(table_name)
        self.last_update = datetime.now().isoformat()
    
    def mark_object_failed(self, object_name: str, error: str):
        """Mark an object as failed with error message.
        
        Args:
            object_name: Name of failed object
            error: Error message
        """
        self.failed_objects[object_name] = error
        self.last_update = datetime.now().isoformat()
    
    def is_table_completed(self, table_name: str) -> bool:
        """Check if a table is completed.
        
        Args:
            table_name: Name of table to check
            
        Returns:
            True if table is completed
        """
        return table_name in self.completed_tables
    
    def save(self):
        """Save progress state to file."""
        state = asdict(self)
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
    
    @classmethod
    def resume(cls, state_file: str) -> "ProgressTracker":
        """Resume from saved state file.
        
        Args:
            state_file: Path to state file
            
        Returns:
            ProgressTracker restored from state
            
        Raises:
            FileNotFoundError: If state file doesn't exist
        """
        if not os.path.exists(state_file):
            raise FileNotFoundError(f"State file not found: {state_file}")
        
        with open(state_file, 'r') as f:
            state = json.load(f)
        
        return cls(**state)
    
    def delete(self):
        """Delete state file (call on successful completion)."""
        if os.path.exists(self.state_file):
            os.remove(self.state_file)
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_progress.py -v`
Expected: 5 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/progress.py tests/unit/test_progress.py
git commit -m "feat: add progress tracker with resume capability"
```

---

## Task 6: Database Connectors - PostgreSQL

**Files:**
- Create: `db2pgpy/connectors/__init__.py`
- Create: `db2pgpy/connectors/postgres.py`
- Create: `tests/unit/test_postgres_connector.py`

**Step 1: Write failing tests for PostgreSQL connector**

Create `tests/unit/test_postgres_connector.py`:
```python
import pytest
from unittest.mock import Mock, patch, MagicMock
from db2pgpy.connectors.postgres import PostgreSQLConnector
from db2pgpy.config import PostgreSQLConfig


@pytest.fixture
def pg_config():
    """Create test PostgreSQL config."""
    return PostgreSQLConfig(
        host="localhost",
        port=5432,
        database="testdb",
        user="testuser",
        password="testpass",
        schema="public"
    )


def test_connect_success(pg_config):
    """Test successful connection to PostgreSQL."""
    with patch('db2pgpy.connectors.postgres.psycopg2.connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(pg_config)
        connector.connect()
        
        mock_connect.assert_called_once()
        assert connector.connection == mock_conn


def test_connect_with_retry(pg_config):
    """Test connection with retry on failure."""
    with patch('db2pgpy.connectors.postgres.psycopg2.connect') as mock_connect:
        # Fail twice, succeed on third attempt
        mock_connect.side_effect = [
            Exception("Connection failed"),
            Exception("Connection failed"),
            Mock()
        ]
        
        connector = PostgreSQLConnector(pg_config)
        connector.connect()
        
        assert mock_connect.call_count == 3


def test_execute_query(pg_config):
    """Test executing a query."""
    with patch('db2pgpy.connectors.postgres.psycopg2.connect') as mock_connect:
        mock_cursor = Mock()
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(pg_config)
        connector.connect()
        connector.execute("CREATE TABLE test (id INT)")
        
        mock_cursor.execute.assert_called_once_with("CREATE TABLE test (id INT)")
        mock_conn.commit.assert_called_once()


def test_disconnect(pg_config):
    """Test disconnecting from database."""
    with patch('db2pgpy.connectors.postgres.psycopg2.connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        connector = PostgreSQLConnector(pg_config)
        connector.connect()
        connector.disconnect()
        
        mock_conn.close.assert_called_once()
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_postgres_connector.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement PostgreSQL connector**

Create `db2pgpy/connectors/__init__.py` (empty file)

Create `db2pgpy/connectors/postgres.py`:
```python
"""PostgreSQL database connector."""
import time
import psycopg2
from typing import Any, List, Optional
from db2pgpy.config import PostgreSQLConfig


class PostgreSQLConnector:
    """PostgreSQL database connection wrapper."""
    
    def __init__(self, config: PostgreSQLConfig):
        """Initialize connector with configuration.
        
        Args:
            config: PostgreSQL configuration
        """
        self.config = config
        self.connection: Optional[Any] = None
        self.schema = config.schema
    
    def connect(self, max_retries: int = 3, retry_delay: float = 2.0):
        """Connect to PostgreSQL database with retry logic.
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds
            
        Raises:
            Exception: If connection fails after all retries
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                self.connection = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.user,
                    password=self.config.password
                )
                # Set schema
                cursor = self.connection.cursor()
                cursor.execute(f"SET search_path TO {self.schema}")
                cursor.close()
                return
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
        
        raise Exception(f"Failed to connect to PostgreSQL after {max_retries} attempts: {last_error}")
    
    def execute(self, query: str, params: tuple = None):
        """Execute a query.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Raises:
            Exception: If not connected
        """
        if not self.connection:
            raise Exception("Not connected to database")
        
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
        finally:
            cursor.close()
    
    def fetchall(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute query and fetch all results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of result tuples
        """
        if not self.connection:
            raise Exception("Not connected to database")
        
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall()
        finally:
            cursor.close()
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_postgres_connector.py -v`
Expected: 4 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/connectors/ tests/unit/test_postgres_connector.py
git commit -m "feat: add PostgreSQL connector with retry logic"
```

---

## Task 7: Database Connectors - DB2

**Files:**
- Create: `db2pgpy/connectors/db2.py`
- Create: `tests/unit/test_db2_connector.py`

**Step 1: Write failing tests for DB2 connector**

Create `tests/unit/test_db2_connector.py`:
```python
import pytest
from unittest.mock import Mock, patch, MagicMock
from db2pgpy.connectors.db2 import DB2Connector
from db2pgpy.config import DB2Config


@pytest.fixture
def db2_config():
    """Create test DB2 config."""
    return DB2Config(
        host="localhost",
        port=50000,
        database="SAMPLE",
        user="db2admin",
        password="testpass",
        schema="DB2INST1"
    )


def test_connect_success(db2_config):
    """Test successful connection to DB2."""
    with patch('db2pgpy.connectors.db2.ibm_db.connect') as mock_connect:
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        connector = DB2Connector(db2_config)
        connector.connect()
        
        mock_connect.assert_called_once()
        assert connector.connection == mock_conn


def test_build_connection_string(db2_config):
    """Test building DB2 connection string."""
    connector = DB2Connector(db2_config)
    conn_str = connector._build_connection_string()
    
    assert "DATABASE=SAMPLE" in conn_str
    assert "HOSTNAME=localhost" in conn_str
    assert "PORT=50000" in conn_str
    assert "UID=db2admin" in conn_str
    assert "PWD=testpass" in conn_str


def test_fetch_tables(db2_config):
    """Test fetching table list from DB2."""
    with patch('db2pgpy.connectors.db2.ibm_db.connect') as mock_connect:
        with patch('db2pgpy.connectors.db2.ibm_db.exec_immediate') as mock_exec:
            with patch('db2pgpy.connectors.db2.ibm_db.fetch_tuple') as mock_fetch:
                mock_conn = Mock()
                mock_connect.return_value = mock_conn
                mock_stmt = Mock()
                mock_exec.return_value = mock_stmt
                
                # Simulate fetching two tables then None
                mock_fetch.side_effect = [
                    ('EMPLOYEES',),
                    ('DEPARTMENTS',),
                    False
                ]
                
                connector = DB2Connector(db2_config)
                connector.connect()
                tables = connector.fetch_tables()
                
                assert tables == ['EMPLOYEES', 'DEPARTMENTS']


def test_disconnect(db2_config):
    """Test disconnecting from DB2."""
    with patch('db2pgpy.connectors.db2.ibm_db.connect') as mock_connect:
        with patch('db2pgpy.connectors.db2.ibm_db.close') as mock_close:
            mock_conn = Mock()
            mock_connect.return_value = mock_conn
            
            connector = DB2Connector(db2_config)
            connector.connect()
            connector.disconnect()
            
            mock_close.assert_called_once_with(mock_conn)
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_db2_connector.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement DB2 connector**

Create `db2pgpy/connectors/db2.py`:
```python
"""DB2 database connector."""
import time
try:
    import ibm_db
except ImportError:
    ibm_db = None

from typing import Any, List, Optional
from db2pgpy.config import DB2Config


class DB2Connector:
    """DB2 database connection wrapper."""
    
    def __init__(self, config: DB2Config):
        """Initialize connector with configuration.
        
        Args:
            config: DB2 configuration
        """
        if ibm_db is None:
            raise ImportError(
                "ibm_db is not installed. "
                "Please install IBM Data Server Driver and ibm_db package."
            )
        
        self.config = config
        self.connection: Optional[Any] = None
        self.schema = config.schema
    
    def _build_connection_string(self) -> str:
        """Build DB2 connection string.
        
        Returns:
            Connection string
        """
        parts = [
            f"DATABASE={self.config.database}",
            f"HOSTNAME={self.config.host}",
            f"PORT={self.config.port}",
            f"PROTOCOL=TCPIP",
            f"UID={self.config.user}",
            f"PWD={self.config.password}"
        ]
        return ";".join(parts)
    
    def connect(self, max_retries: int = 3, retry_delay: float = 2.0):
        """Connect to DB2 database with retry logic.
        
        Args:
            max_retries: Maximum number of connection attempts
            retry_delay: Delay between retries in seconds
            
        Raises:
            Exception: If connection fails after all retries
        """
        conn_string = self._build_connection_string()
        last_error = None
        
        for attempt in range(max_retries):
            try:
                self.connection = ibm_db.connect(conn_string, "", "")
                return
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
        
        raise Exception(f"Failed to connect to DB2 after {max_retries} attempts: {last_error}")
    
    def execute(self, query: str) -> Any:
        """Execute a query and return statement.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Statement object
            
        Raises:
            Exception: If not connected
        """
        if not self.connection:
            raise Exception("Not connected to database")
        
        return ibm_db.exec_immediate(self.connection, query)
    
    def fetch_tables(self) -> List[str]:
        """Fetch list of tables in schema.
        
        Returns:
            List of table names
        """
        if self.schema:
            query = f"""
                SELECT TABNAME 
                FROM SYSCAT.TABLES 
                WHERE TABSCHEMA = '{self.schema}' 
                  AND TYPE = 'T'
                ORDER BY TABNAME
            """
        else:
            query = """
                SELECT TABNAME 
                FROM SYSCAT.TABLES 
                WHERE TYPE = 'T'
                ORDER BY TABNAME
            """
        
        stmt = self.execute(query)
        tables = []
        
        row = ibm_db.fetch_tuple(stmt)
        while row:
            tables.append(row[0])
            row = ibm_db.fetch_tuple(stmt)
        
        return tables
    
    def disconnect(self):
        """Close database connection."""
        if self.connection:
            ibm_db.close(self.connection)
            self.connection = None
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_db2_connector.py -v`
Expected: 4 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/connectors/db2.py tests/unit/test_db2_connector.py
git commit -m "feat: add DB2 connector with system catalog queries"
```

---

## Task 8: CLI Foundation

**Files:**
- Create: `db2pgpy/cli.py`
- Create: `tests/unit/test_cli.py`

**Step 1: Write failing tests for CLI**

Create `tests/unit/test_cli.py`:
```python
import pytest
from click.testing import CliRunner
from db2pgpy.cli import cli


def test_cli_help():
    """Test CLI help command."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    
    assert result.exit_code == 0
    assert 'migrate' in result.output
    assert 'validate-config' in result.output


def test_cli_version():
    """Test CLI version command."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--version'])
    
    assert result.exit_code == 0
    assert '1.0.0' in result.output


def test_migrate_missing_config():
    """Test migrate command without config file."""
    runner = CliRunner()
    result = runner.invoke(cli, ['migrate'])
    
    assert result.exit_code != 0
    assert 'config' in result.output.lower()


def test_validate_config_command(tmp_path):
    """Test validate-config command."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
db2:
  host: localhost
  port: 50000
  database: SAMPLE
  user: db2admin
  password: secret

postgresql:
  host: localhost
  port: 5432
  database: migrated
  user: postgres
  password: pgpass
""")
    
    runner = CliRunner()
    result = runner.invoke(cli, ['validate-config', '--config', str(config_file)])
    
    # Should try to connect (will fail without real DB, but that's OK for unit test)
    assert 'Validating configuration' in result.output
```

**Step 2: Run test to verify it fails**

Run: `pytest tests/unit/test_cli.py -v`
Expected: FAIL with "ModuleNotFoundError"

**Step 3: Implement CLI foundation**

Create `db2pgpy/cli.py`:
```python
"""Command-line interface for DB2 to PostgreSQL migration."""
import click
import sys
from db2pgpy import __version__
from db2pgpy.config import Config
from db2pgpy.logger import setup_logger


@click.group()
@click.version_option(version=__version__)
def cli():
    """DB2 to PostgreSQL migration tool."""
    pass


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True),
              help='Path to YAML configuration file')
@click.option('--mode', type=click.Choice(['full', 'schema_only', 'data_only']),
              help='Migration mode (overrides config)')
def migrate(config, mode):
    """Execute database migration.
    
    Performs complete migration from DB2 to PostgreSQL including
    schema extraction, conversion, data transfer, and validation.
    """
    click.echo(click.style("DB2 to PostgreSQL Migration Tool", fg='green', bold=True))
    click.echo(f"Version: {__version__}\n")
    
    try:
        # Load configuration
        click.echo("Loading configuration...")
        cfg = Config.load(config)
        
        # Override mode if specified
        if mode:
            cfg.migration.mode = mode
        
        click.echo(f"Migration mode: {cfg.migration.mode}")
        click.echo(f"Source: DB2 {cfg.db2.host}:{cfg.db2.port}/{cfg.db2.database}")
        click.echo(f"Target: PostgreSQL {cfg.postgresql.host}:{cfg.postgresql.port}/{cfg.postgresql.database}")
        
        # TODO: Implement migration logic
        click.echo("\nMigration logic not yet implemented.")
        
    except Exception as e:
        click.echo(click.style(f"Error: {e}", fg='red'), err=True)
        sys.exit(1)


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True),
              help='Path to YAML configuration file')
def resume(config):
    """Resume interrupted migration.
    
    Continues migration from last saved checkpoint.
    """
    click.echo("Resume command not yet implemented.")


@cli.command()
@click.option('--config', required=True, type=click.Path(exists=True),
              help='Path to YAML configuration file')
def validate(config):
    """Run validation only.
    
    Validates existing migration without performing data transfer.
    """
    click.echo("Validate command not yet implemented.")


@cli.command('validate-config')
@click.option('--config', required=True, type=click.Path(exists=True),
              help='Path to YAML configuration file')
def validate_config(config):
    """Validate configuration and test connections.
    
    Tests database connectivity without performing migration.
    """
    click.echo("Validating configuration...")
    
    try:
        cfg = Config.load(config)
        click.echo(click.style("✓ Configuration loaded successfully", fg='green'))
        
        # TODO: Test database connections
        click.echo("\nDatabase connection testing not yet implemented.")
        
    except FileNotFoundError as e:
        click.echo(click.style(f"✗ {e}", fg='red'), err=True)
        sys.exit(1)
    except ValueError as e:
        click.echo(click.style(f"✗ Invalid configuration: {e}", fg='red'), err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(click.style(f"✗ Error: {e}", fg='red'), err=True)
        sys.exit(1)


def main():
    """Entry point for CLI."""
    cli()


if __name__ == '__main__':
    main()
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/unit/test_cli.py -v`
Expected: 4 tests PASS

**Step 5: Commit**

```bash
git add db2pgpy/cli.py tests/unit/test_cli.py
git commit -m "feat: add CLI foundation with Click commands"
```

---

## Remaining Tasks Summary

The implementation plan continues with these remaining tasks:

**Task 9**: Schema Extractor - Extract table metadata from DB2 SYSCAT
**Task 10**: Schema Converter - Convert DB2 DDL to PostgreSQL DDL
**Task 11**: Data Transfer Module - Batch data migration
**Task 12**: Validator Module - Post-migration validation
**Task 13**: Migrator Orchestration - Coordinate all phases
**Task 14**: Views/Procedures/Triggers Extractors - Additional object types
**Task 15**: PL/SQL Converter - Best-effort procedural code conversion
**Task 16**: CLI Integration - Wire up all components
**Task 17**: Integration Tests - End-to-end testing
**Task 18**: Documentation - README and guides
**Task 19**: Example Configurations - Sample YAML files
**Task 20**: Final Testing & Polish - Performance tuning

Each remaining task follows the same TDD pattern:
1. Write failing test
2. Run test to verify failure
3. Implement minimal code
4. Run test to verify success
5. Commit

---

## Implementation Approach

**Recommended**: Use @superpowers:subagent-driven-development for parallel execution
- Fresh subagent per task
- Code review between tasks
- Fast iteration in current session

**Alternative**: Use @superpowers:executing-plans in separate session for batch execution

---

## Success Criteria

Migration complete when:
- All tests passing (unit + integration)
- CLI commands functional
- Can migrate sample DB2 database
- Validation report shows success
- Documentation complete
- Code formatted and linted
