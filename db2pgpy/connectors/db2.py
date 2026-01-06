"""DB2 database connector with system catalog queries."""
import time
from typing import Dict, List, Any, Optional
from ..logger import setup_logger

try:
    import ibm_db
except ImportError:
    ibm_db = None


class DB2Connector:
    """DB2 database connector with system catalog query support."""
    
    def __init__(self, config: Dict[str, Any], max_retries: int = 3, retry_delay: int = 5):
        """
        Initialize DB2 connector.
        
        Args:
            config: Database connection configuration
            max_retries: Maximum number of connection retry attempts
            retry_delay: Delay between retry attempts (seconds)
        """
        if ibm_db is None:
            raise ImportError(
                "ibm_db package is not installed. "
                "Please install IBM Data Server Driver and ibm_db package."
            )
        self.config = config
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.conn = None
        self.logger = setup_logger("db2_connector")
    
    def _get_connection_string(self) -> str:
        """
        Build connection string from config.
        
        Returns:
            DB2 connection string
        """
        parts = []
        if "database" in self.config:
            parts.append(f"DATABASE={self.config['database']}")
        if "host" in self.config:
            parts.append(f"HOSTNAME={self.config['host']}")
        if "port" in self.config:
            parts.append(f"PORT={self.config['port']}")
        if "user" in self.config:
            parts.append(f"UID={self.config['user']}")
        if "password" in self.config:
            parts.append(f"PWD={self.config['password']}")
        
        # Additional connection options
        parts.append("PROTOCOL=TCPIP")
        
        return ";".join(parts)
    
    def connect(self):
        """Establish connection to DB2 with retry logic."""
        conn_str = self._get_connection_string()
        
        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Connecting to DB2 (attempt {attempt}/{self.max_retries})...")
                self.conn = ibm_db.connect(conn_str, "", "")
                self.logger.info("Successfully connected to DB2")
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
            ibm_db.close(self.conn)
            self.conn = None
            self.logger.info("Disconnected from DB2")
    
    def get_tables(self, schema: str) -> List[str]:
        """
        Get list of tables in a schema.
        
        Args:
            schema: Schema name
            
        Returns:
            List of table names
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        query = f"""
            SELECT TABNAME 
            FROM SYSCAT.TABLES 
            WHERE TABSCHEMA = '{schema}' 
            AND TYPE = 'T'
            ORDER BY TABNAME
        """
        
        stmt = ibm_db.exec_immediate(self.conn, query)
        tables = []
        
        while True:
            row = ibm_db.fetch_assoc(stmt)
            if not row:
                break
            tables.append(row["TABNAME"])
        
        return tables
    
    def get_table_schema(self, table_name: str, schema: str) -> List[Dict[str, Any]]:
        """
        Get schema definition for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            List of column definitions
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        query = f"""
            SELECT 
                COLNAME, TYPENAME, LENGTH, SCALE, NULLS, DEFAULT, IDENTITY
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = '{schema}' AND TABNAME = '{table_name}'
            ORDER BY COLNO
        """
        
        stmt = ibm_db.exec_immediate(self.conn, query)
        columns = []
        
        while True:
            row = ibm_db.fetch_assoc(stmt)
            if not row:
                break
            
            # Build type string with length/scale
            type_str = row["TYPENAME"]
            if row["TYPENAME"] in ("VARCHAR", "CHAR", "GRAPHIC", "VARGRAPHIC"):
                type_str = f"{row['TYPENAME']}({row['LENGTH']})"
            elif row["TYPENAME"] in ("DECIMAL", "NUMERIC"):
                type_str = f"{row['TYPENAME']}({row['LENGTH']},{row['SCALE']})"
            
            columns.append({
                "name": row["COLNAME"],
                "type": type_str,
                "nullable": row["NULLS"] == "Y",
                "default": row.get("DEFAULT"),
                "is_identity": row.get("IDENTITY") == "Y",
            })
        
        return columns
    
    def get_table_row_count(self, table_name: str, schema: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            Number of rows in table
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
        stmt = ibm_db.exec_immediate(self.conn, query)
        row = ibm_db.fetch_assoc(stmt)
        
        # DB2 returns count as "1" key
        return row["1"] if row else 0
    
    def get_primary_keys(self, table_name: str, schema: str) -> List[str]:
        """
        Get primary key columns for a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            
        Returns:
            List of primary key column names
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        query = f"""
            SELECT COLNAME
            FROM SYSCAT.KEYCOLUSE
            WHERE TABSCHEMA = '{schema}' AND TABNAME = '{table_name}'
            AND CONSTNAME IN (
                SELECT CONSTNAME FROM SYSCAT.TABCONST
                WHERE TABSCHEMA = '{schema}' AND TABNAME = '{table_name}'
                AND TYPE = 'P'
            )
            ORDER BY COLSEQ
        """
        
        stmt = ibm_db.exec_immediate(self.conn, query)
        pk_columns = []
        
        while True:
            row = ibm_db.fetch_assoc(stmt)
            if not row:
                break
            pk_columns.append(row["COLNAME"])
        
        return pk_columns
    
    def fetch_table_data(self, table_name: str, schema: str, batch_size: int = 1000):
        """
        Fetch table data in batches.
        
        Args:
            table_name: Name of the table
            schema: Schema name
            batch_size: Number of rows per batch
            
        Yields:
            Batches of rows as list of tuples
        """
        if not self.conn:
            raise RuntimeError("Not connected to database")
        
        query = f'SELECT * FROM "{schema}"."{table_name}"'
        stmt = ibm_db.exec_immediate(self.conn, query)
        
        batch = []
        while True:
            row = ibm_db.fetch_tuple(stmt)
            if not row:
                if batch:
                    yield batch
                break
            
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
