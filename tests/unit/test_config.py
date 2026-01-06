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
