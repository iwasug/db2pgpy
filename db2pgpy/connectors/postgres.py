"""PostgreSQL database connector with retry logic."""
import time
import psycopg2
from typing import Dict, List, Tuple, Any, Optional
from ..logger import setup_logger


class PostgresConnector:
    """PostgreSQL database connector with connection pooling and retry logic."""
    
    def __init__(self, config: Dict[str, Any], max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize PostgreSQL connector.
        
        Args:
            config: Database connection configuration
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retry attempts (seconds)
        """
        self.config = config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.conn = None
        self.logger = setup_logger("postgres_connector")
    
    def _get_connection_params(self) -> Dict[str, Any]:
        """
        Build connection parameters from config.
        
        Returns:
            PostgreSQL connection parameters dictionary
        """
        params = {}
        if "host" in self.config:
            params["host"] = self.config["host"]
        if "port" in self.config:
            params["port"] = self.config["port"]
        if "database" in self.config:
            params["dbname"] = self.config["database"]  # psycopg2 uses 'dbname' not 'database'
        if "user" in self.config:
            params["user"] = self.config["user"]
        if "password" in self.config:
            params["password"] = self.config["password"]
        
        return params
    
    def connect(self):
        """Establish connection to PostgreSQL with retry logic."""
        conn_params = self._get_connection_params()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Connecting to PostgreSQL (attempt {attempt}/{self.max_retries})...")
                self.conn = psycopg2.connect(**conn_params)
                self.logger.info("Successfully connected to PostgreSQL")
                return
            except Exception as e:
                self.logger.error(f"Connection attempt {attempt} failed: {e}")
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    self.logger.error("Max retries reached. Connection failed.")
                    raise
    
    def disconnect(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.logger.info("Disconnected from PostgreSQL")
    
    def execute_query(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """
        Execute a SELECT query and return results.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            List of result tuples
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            self.conn.rollback()
            raise
    
    def execute_update(self, query: str, params: Optional[Tuple] = None) -> int:
        """
        Execute an INSERT/UPDATE/DELETE query.
        
        Args:
            query: SQL query to execute
            params: Optional query parameters
            
        Returns:
            Number of affected rows
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(query, params)
                self.conn.commit()
                return cursor.rowcount
        except Exception as e:
            self.conn.rollback()
            raise
    
    def execute_ddl(self, ddl: str) -> None:
        """
        Execute a DDL statement (CREATE, ALTER, DROP, etc.).
        
        Args:
            ddl: DDL statement to execute
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(ddl)
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise
    
    def execute_batch(self, query: str, data: List[Tuple]) -> int:
        """
        Execute batch insert/update operations.
        
        Args:
            query: SQL query template
            data: List of parameter tuples
            
        Returns:
            Total number of affected rows
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        total_rows = 0
        with self.conn.cursor() as cursor:
            for params in data:
                cursor.execute(query, params)
                total_rows += cursor.rowcount
            self.conn.commit()
        
        return total_rows
    
    def bulk_insert(self, table_name: str, data: List[Tuple]) -> int:
        """
        Bulk insert data into a table.
        
        Args:
            table_name: Name of the table
            data: List of row tuples
            
        Returns:
            Number of rows inserted
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        if not data:
            return 0
        
        try:
            # Build INSERT query with placeholders
            num_columns = len(data[0])
            placeholders = ', '.join(['%s'] * num_columns)
            insert_query = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            
            total_rows = 0
            with self.conn.cursor() as cursor:
                for row in data:
                    cursor.execute(insert_query, row)
                    total_rows += cursor.rowcount
                self.conn.commit()
            
            return total_rows
        except Exception as e:
            self.conn.rollback()
            raise
    
    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        """
        Check if a table exists in PostgreSQL.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: 'public')
            
        Returns:
            True if table exists
        """
        query = """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """
        result = self.execute_query(query, (schema, table_name))
        return result[0][0] if result else False
    
    def get_table_row_count(self, table_name: str, schema: str = "public") -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            Number of rows in table
        """
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        result = self.execute_query(query)
        return result[0][0] if result else 0
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
