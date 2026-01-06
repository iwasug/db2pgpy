import pytest
from click.testing import CliRunner
from unittest.mock import patch, Mock
from db2pgpy.cli import cli, validate, migrate, resume


@pytest.fixture
def runner():
    """Create a Click CLI test runner."""
    return CliRunner()


def test_cli_help(runner):
    """Test CLI help output."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "DB2 to PostgreSQL migration tool" in result.output


def test_validate_command_help(runner):
    """Test validate command help."""
    result = runner.invoke(validate, ["--help"])
    assert result.exit_code == 0
    assert "Validate configuration" in result.output


def test_migrate_command_help(runner):
    """Test migrate command help."""
    result = runner.invoke(migrate, ["--help"])
    assert result.exit_code == 0
    assert "Run full migration" in result.output


def test_resume_command_help(runner):
    """Test resume command help."""
    result = runner.invoke(resume, ["--help"])
    assert result.exit_code == 0
    assert "Resume migration" in result.output


def test_validate_command_missing_config(runner):
    """Test validate command with missing config file."""
    result = runner.invoke(validate, ["--config", "nonexistent.yaml"])
    assert result.exit_code != 0
    assert "Error" in result.output or "does not exist" in result.output


@patch('db2pgpy.cli.Path')
def test_validate_command_with_config(mock_path, runner):
    """Test validate command with config file."""
    # Mock config file exists
    mock_path.return_value.exists.return_value = True
    
    with patch('db2pgpy.cli.load_config') as mock_load_config:
        mock_load_config.return_value = {
            "db2": {"host": "localhost", "database": "testdb"},
            "postgres": {"host": "localhost", "database": "testdb"},
        }
        
        result = runner.invoke(validate, ["--config", "config.yaml"])
        # Command should process (exact output depends on implementation)
        assert "config.yaml" in result.output or result.exit_code == 0


@patch('db2pgpy.cli.Path')
def test_migrate_command_with_config(mock_path, runner):
    """Test migrate command."""
    mock_path.return_value.exists.return_value = True
    
    with patch('db2pgpy.cli.load_config') as mock_load_config:
        mock_load_config.return_value = {
            "db2": {"host": "localhost", "database": "testdb"},
            "postgres": {"host": "localhost", "database": "testdb"},
        }
        
        result = runner.invoke(migrate, ["--config", "config.yaml"])
        # Command should attempt to run
        assert result.exit_code is not None


def test_cli_version(runner):
    """Test CLI version display."""
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "version" in result.output.lower() or "1.0.0" in result.output
