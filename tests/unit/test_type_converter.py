import pytest
from db2pgpy.converters.types import TypeConverter


@pytest.fixture
def converter():
    """Create a TypeConverter instance."""
    return TypeConverter()


def test_numeric_types(converter):
    """Test numeric type conversions."""
    assert converter.convert("SMALLINT") == "SMALLINT"
    assert converter.convert("INTEGER") == "INTEGER"
    assert converter.convert("BIGINT") == "BIGINT"
    assert converter.convert("DECIMAL(10,2)") == "NUMERIC(10,2)"
    assert converter.convert("NUMERIC(15,3)") == "NUMERIC(15,3)"
    assert converter.convert("REAL") == "REAL"
    assert converter.convert("DOUBLE") == "DOUBLE PRECISION"


def test_string_types(converter):
    """Test string type conversions."""
    assert converter.convert("CHAR(10)") == "CHAR(10)"
    assert converter.convert("VARCHAR(255)") == "VARCHAR(255)"
    assert converter.convert("CLOB") == "TEXT"
    assert converter.convert("GRAPHIC(10)") == "CHAR(10)"
    assert converter.convert("VARGRAPHIC(255)") == "VARCHAR(255)"


def test_datetime_types(converter):
    """Test datetime type conversions."""
    assert converter.convert("DATE") == "DATE"
    assert converter.convert("TIME") == "TIME"
    assert converter.convert("TIMESTAMP") == "TIMESTAMP"


def test_binary_types(converter):
    """Test binary type conversions."""
    assert converter.convert("BLOB") == "BYTEA"
    assert converter.convert("BINARY(100)") == "BYTEA"
    assert converter.convert("VARBINARY(200)") == "BYTEA"


def test_special_types(converter):
    """Test special type conversions."""
    assert converter.convert("XML") == "XML"
    assert converter.convert("BOOLEAN") == "BOOLEAN"


def test_unknown_type(converter):
    """Test handling of unknown types."""
    result = converter.convert("UNKNOWNTYPE")
    assert result == "TEXT"  # Default fallback


def test_case_insensitive(converter):
    """Test that conversion is case-insensitive."""
    assert converter.convert("varchar(50)") == "VARCHAR(50)"
    assert converter.convert("INTEGER") == converter.convert("integer")
