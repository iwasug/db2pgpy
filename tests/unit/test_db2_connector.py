import pytest
from unittest.mock import Mock, patch, MagicMock
from db2pgpy.connectors.db2 import DB2Connector


@pytest.fixture
def db2_config():
    """DB2 connection configuration."""
    return {
        "host": "db2host",
        "port": 50000,
        "database": "testdb",
        "user": "testuser",
        "password": "testpass",
    }


@pytest.fixture
def connector(db2_config):
    """Create a DB2Connector instance."""
    return DB2Connector(db2_config)


def test_connection_string(connector):
    """Test connection string generation."""
    conn_str = connector._get_connection_string()
    assert "DATABASE=testdb" in conn_str
    assert "HOSTNAME=db2host" in conn_str
    assert "PORT=50000" in conn_str
    assert "UID=testuser" in conn_str
    assert "PWD=testpass" in conn_str


@patch('db2pgpy.connectors.db2.ibm_db')
def test_connect_success(mock_ibm_db, connector):
    """Test successful connection."""
    mock_conn = Mock()
    mock_ibm_db.connect.return_value = mock_conn
    
    connector.connect()
    
    assert connector.conn == mock_conn
    mock_ibm_db.connect.assert_called_once()


@patch('db2pgpy.connectors.db2.ibm_db')
def test_get_tables(mock_ibm_db, connector):
    """Test retrieving table list."""
    mock_conn = Mock()
    mock_stmt = Mock()
    
    # Mock table results
    mock_ibm_db.exec_immediate.return_value = mock_stmt
    mock_ibm_db.fetch_assoc.side_effect = [
        {"TABNAME": "TABLE1", "TABSCHEMA": "SCHEMA1"},
        {"TABNAME": "TABLE2", "TABSCHEMA": "SCHEMA1"},
        False,  # End of results
    ]
    
    connector.conn = mock_conn
    tables = connector.get_tables("SCHEMA1")
    
    assert len(tables) == 2
    assert tables[0] == "TABLE1"
    assert tables[1] == "TABLE2"


@patch('db2pgpy.connectors.db2.ibm_db')
def test_get_table_schema(mock_ibm_db, connector):
    """Test retrieving table schema."""
    mock_conn = Mock()
    mock_stmt = Mock()
    
    # Mock column results
    mock_ibm_db.exec_immediate.return_value = mock_stmt
    mock_ibm_db.fetch_assoc.side_effect = [
        {
            "COLNAME": "ID",
            "TYPENAME": "INTEGER",
            "LENGTH": 4,
            "SCALE": 0,
            "NULLS": "N",
        },
        {
            "COLNAME": "NAME",
            "TYPENAME": "VARCHAR",
            "LENGTH": 255,
            "SCALE": 0,
            "NULLS": "Y",
        },
        False,
    ]
    
    connector.conn = mock_conn
    schema = connector.get_table_schema("TABLE1", "SCHEMA1")
    
    assert len(schema) == 2
    assert schema[0]["name"] == "ID"
    assert schema[0]["type"] == "INTEGER"
    assert schema[0]["nullable"] is False
    assert schema[1]["name"] == "NAME"
    assert schema[1]["type"] == "VARCHAR(255)"
    assert schema[1]["nullable"] is True


@patch('db2pgpy.connectors.db2.ibm_db')
def test_get_table_row_count(mock_ibm_db, connector):
    """Test getting table row count."""
    mock_conn = Mock()
    mock_stmt = Mock()
    
    mock_ibm_db.exec_immediate.return_value = mock_stmt
    mock_ibm_db.fetch_assoc.return_value = {"1": 1000}
    
    connector.conn = mock_conn
    count = connector.get_table_row_count("TABLE1", "SCHEMA1")
    
    assert count == 1000


@patch('db2pgpy.connectors.db2.ibm_db')
def test_disconnect(mock_ibm_db, connector):
    """Test disconnection."""
    mock_conn = Mock()
    connector.conn = mock_conn
    
    connector.disconnect()
    
    mock_ibm_db.close.assert_called_once_with(mock_conn)
    assert connector.conn is None


def test_context_manager(db2_config):
    """Test using connector as context manager."""
    with patch('db2pgpy.connectors.db2.ibm_db') as mock_ibm_db:
        mock_conn = Mock()
        mock_ibm_db.connect.return_value = mock_conn
        
        with DB2Connector(db2_config) as connector:
            assert connector.conn == mock_conn
        
        mock_ibm_db.close.assert_called_once()
