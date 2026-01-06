"""Configuration management for DB2 to PostgreSQL migration."""
import os
from dataclasses import dataclass, field
from typing import Optional, List
import yaml


@dataclass
class DB2Config:
    """DB2 database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: Optional[str] = None
    ssl: bool = False
    timeout: int = 30


@dataclass
class PostgreSQLConfig:
    """PostgreSQL database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str = "public"
    ssl: bool = False
    timeout: int = 30


@dataclass
class MigrationConfig:
    """Migration process configuration."""
    mode: str = "full"  # full, schema-only, data-only
    batch_size: int = 1000
    continue_on_error: bool = False
    parallel_workers: int = 1
    tables: Optional[List[str]] = None
    exclude_tables: Optional[List[str]] = None


@dataclass
class ResumeConfig:
    """Resume configuration for interrupted migrations."""
    enabled: bool = True
    checkpoint_file: str = ".db2pgpy_checkpoint.json"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    file: Optional[str] = "db2pgpy.log"
    console: bool = True
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class ValidationConfig:
    """Validation configuration."""
    enabled: bool = True
    row_count: bool = True
    data_sampling: bool = True
    sample_size: int = 100


@dataclass
class Config:
    """Main configuration object."""
    db2: DB2Config
    postgresql: PostgreSQLConfig
    migration: MigrationConfig = field(default_factory=MigrationConfig)
    resume: ResumeConfig = field(default_factory=ResumeConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    validation: ValidationConfig = field(default_factory=ValidationConfig)

    @classmethod
    def load(cls, config_path: str) -> "Config":
        """
        Load configuration from a YAML file.
        
        Args:
            config_path: Path to the YAML configuration file
            
        Returns:
            Config object with validated configuration
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If required fields are missing or invalid
        """
        # Check if file exists
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Load YAML file
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
        
        if not data:
            raise ValueError("Configuration file is empty")
        
        # Validate and load DB2 config
        db2_data = data.get('db2', {})
        cls._validate_required_fields(db2_data, 
                                       ['host', 'port', 'database', 'user', 'password'],
                                       'db2')
        db2_config = DB2Config(
            host=db2_data['host'],
            port=int(db2_data['port']),
            database=db2_data['database'],
            user=db2_data['user'],
            password=db2_data['password'],
            schema=db2_data.get('schema'),
            ssl=db2_data.get('ssl', False),
            timeout=db2_data.get('timeout', 30)
        )
        
        # Validate and load PostgreSQL config
        pg_data = data.get('postgresql', {})
        cls._validate_required_fields(pg_data,
                                       ['host', 'port', 'database', 'user', 'password'],
                                       'postgresql')
        postgresql_config = PostgreSQLConfig(
            host=pg_data['host'],
            port=int(pg_data['port']),
            database=pg_data['database'],
            user=pg_data['user'],
            password=pg_data['password'],
            schema=pg_data.get('schema', 'public'),
            ssl=pg_data.get('ssl', False),
            timeout=pg_data.get('timeout', 30)
        )
        
        # Load migration config (all optional with defaults)
        migration_data = data.get('migration', {})
        migration_config = MigrationConfig(
            mode=migration_data.get('mode', 'full'),
            batch_size=migration_data.get('batch_size', 1000),
            continue_on_error=migration_data.get('continue_on_error', False),
            parallel_workers=migration_data.get('parallel_workers', 1),
            tables=migration_data.get('tables'),
            exclude_tables=migration_data.get('exclude_tables')
        )
        
        # Load resume config (all optional with defaults)
        resume_data = data.get('resume', {})
        resume_config = ResumeConfig(
            enabled=resume_data.get('enabled', True),
            checkpoint_file=resume_data.get('checkpoint_file', '.db2pgpy_checkpoint.json')
        )
        
        # Load logging config (all optional with defaults)
        logging_data = data.get('logging', {})
        logging_config = LoggingConfig(
            level=logging_data.get('level', 'INFO'),
            file=logging_data.get('file', 'db2pgpy.log'),
            console=logging_data.get('console', True),
            format=logging_data.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        )
        
        # Load validation config (all optional with defaults)
        validation_data = data.get('validation', {})
        validation_config = ValidationConfig(
            enabled=validation_data.get('enabled', True),
            row_count=validation_data.get('row_count', True),
            data_sampling=validation_data.get('data_sampling', True),
            sample_size=validation_data.get('sample_size', 100)
        )
        
        return cls(
            db2=db2_config,
            postgresql=postgresql_config,
            migration=migration_config,
            resume=resume_config,
            logging=logging_config,
            validation=validation_config
        )
    
    @staticmethod
    def _validate_required_fields(data: dict, required_fields: List[str], section: str):
        """
        Validate that all required fields are present in the data.
        
        Args:
            data: Dictionary of configuration data
            required_fields: List of required field names
            section: Configuration section name for error messages
            
        Raises:
            ValueError: If any required field is missing
        """
        for field_name in required_fields:
            if field_name not in data or data[field_name] is None:
                raise ValueError(f"Missing required field: {section}.{field_name}")


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file as dictionary.
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Dictionary with configuration data
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def validate_config(config_data: dict) -> List[str]:
    """
    Validate configuration data.
    
    Args:
        config_data: Configuration dictionary
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    # Check DB2 configuration
    if 'db2' not in config_data:
        errors.append("Missing 'db2' section")
    else:
        for field in ['host', 'port', 'database', 'user', 'password']:
            if field not in config_data['db2']:
                errors.append(f"Missing required field: db2.{field}")
    
    # Check PostgreSQL configuration
    if 'postgres' not in config_data and 'postgresql' not in config_data:
        errors.append("Missing 'postgres' or 'postgresql' section")
    else:
        pg_section = 'postgres' if 'postgres' in config_data else 'postgresql'
        for field in ['host', 'port', 'database', 'user', 'password']:
            if field not in config_data[pg_section]:
                errors.append(f"Missing required field: {pg_section}.{field}")
    
    return errors
