import logging
import pytest
from db2pgpy.logger import setup_logger


def test_setup_logger_console_only():
    """Test logger with console output only."""
    logger = setup_logger("test_console", level="INFO", console=True, log_file=None)
    
    # Verify logger is configured correctly
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 1
    assert isinstance(logger.handlers[0], logging.StreamHandler)
    
    # Test that logging doesn't raise an exception
    logger.info("Test message")  # Should not raise


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
