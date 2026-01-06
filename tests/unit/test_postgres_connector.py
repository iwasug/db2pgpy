import pytest
from unittest.mock import Mock, patch, MagicMock
from db2pgpy.connectors.postgres import PostgresConnector


@pytest.fixture
def pg_config():
    """PostgreSQL connection configuration."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "testuser",
        "password": "testpass",
    }


@pytest.fixture
def connector(pg_config):
    """Create a PostgresConnector instance."""
    return PostgresConnector(pg_config)


def test_connection_string(connector):
    """Test connection string generation."""
    conn_str = connector._get_connection_string()
    assert "host=localhost" in conn_str
    assert "port=5432" in conn_str
    assert "database=testdb" in conn_str
    assert "user=testuser" in conn_str
    assert "password=testpass" in conn_str


@patch('db2pgpy.connectors.postgres.psycopg2')
def test_connect_success(mock_psycopg2, connector):
    """Test successful connection."""
    mock_conn = Mock()
    mock_psycopg2.connect.return_value = mock_conn
    
    connector.connect()
    
    assert connector.conn == mock_conn
    mock_psycopg2.connect.assert_called_once()


@patch('db2pgpy.connectors.postgres.psycopg2')
def test_connect_with_retry(mock_psycopg2, connector):
    """Test connection with retry on failure."""
    mock_conn = Mock()
    # First call fails, second succeeds
    mock_psycopg2.connect.side_effect = [Exception("Connection failed"), mock_conn]
    
    connector.connect()
    
    assert connector.conn == mock_conn
    assert mock_psycopg2.connect.call_count == 2


@patch('db2pgpy.connectors.postgres.psycopg2')
def test_execute_query(mock_psycopg2, connector):
    """Test query execution."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [(1, 'test')]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    connector.conn = mock_conn
    
    result = connector.execute_query("SELECT * FROM test")
    
    assert result == [(1, 'test')]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test", None)


@patch('db2pgpy.connectors.postgres.psycopg2')
def test_execute_query_with_params(mock_psycopg2, connector):
    """Test query execution with parameters."""
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = [(1, 'test')]
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    connector.conn = mock_conn
    
    result = connector.execute_query("SELECT * FROM test WHERE id = %s", (1,))
    
    assert result == [(1, 'test')]
    mock_cursor.execute.assert_called_once_with("SELECT * FROM test WHERE id = %s", (1,))


@patch('db2pgpy.connectors.postgres.psycopg2')
def test_disconnect(mock_psycopg2, connector):
    """Test disconnection."""
    mock_conn = Mock()
    connector.conn = mock_conn
    
    connector.disconnect()
    
    mock_conn.close.assert_called_once()
    assert connector.conn is None


def test_context_manager(pg_config):
    """Test using connector as context manager."""
    with patch('db2pgpy.connectors.postgres.psycopg2') as mock_psycopg2:
        mock_conn = Mock()
        mock_psycopg2.connect.return_value = mock_conn
        
        with PostgresConnector(pg_config) as connector:
            assert connector.conn == mock_conn
        
        mock_conn.close.assert_called_once()
