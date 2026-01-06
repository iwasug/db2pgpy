"""Sequence manager for auto-increment primary keys."""

from typing import Dict, List, Any, Optional
from .connectors.db2 import DB2Connector
from .connectors.postgres import PostgresConnector
from .logger import setup_logger


class SequenceManager:
    """Manage PostgreSQL sequences for auto-increment primary keys."""
    
    def __init__(self, db2_connector: DB2Connector, pg_connector: PostgresConnector):
        """
        Initialize SequenceManager.
        
        Args:
            db2_connector: DB2 connector instance
            pg_connector: PostgreSQL connector instance
        """
        self.db2_connector = db2_connector
        self.pg_connector = pg_connector
        self.logger = setup_logger("sequence_manager")
    
    def get_maxsequence_info(self, table_name: str, schema: str = "MAXIMO") -> Optional[Dict[str, Any]]:
        """
        Get sequence information from MAXSEQUENCE table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (default: MAXIMO)
            
        Returns:
            Dictionary with sequence info or None if not found
        """
        query = f"""
            SELECT TBNAME, NAME, MAXRESERVED, SEQUENCENAME
            FROM {schema}.MAXSEQUENCE
            WHERE TBNAME = '{table_name}'
        """
        
        try:
            rows = self.db2_connector.execute_query(query)
            if rows:
                row = rows[0]
                return {
                    'table_name': row['TBNAME'],
                    'column_name': row['NAME'],
                    'current_value': row['MAXRESERVED'],
                    'sequence_name': row.get('SEQUENCENAME', f"{table_name}_seq")
                }
        except Exception as e:
            self.logger.debug(f"Could not get MAXSEQUENCE info for {table_name}: {e}")
        
        return None
    
    def get_max_value_from_table(self, table_name: str, column_name: str, schema: str = "MAXIMO") -> int:
        """
        Get maximum value from a table column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            schema: Schema name
            
        Returns:
            Maximum value or 0 if table is empty
        """
        query = f'SELECT MAX("{column_name}") as MAX_VAL FROM "{schema}"."{table_name}"'
        
        try:
            rows = self.db2_connector.execute_query(query)
            if rows and rows[0].get('MAX_VAL') is not None:
                return int(rows[0]['MAX_VAL'])
        except Exception as e:
            self.logger.debug(f"Could not get max value for {table_name}.{column_name}: {e}")
        
        return 0
    
    def create_sequence_for_column(
        self, 
        table_name: str, 
        column_name: str,
        start_value: Optional[int] = None,
        schema: str = "MAXIMO"
    ) -> str:
        """
        Create a PostgreSQL sequence for a column.
        
        Args:
            table_name: Name of the table
            column_name: Name of the column
            start_value: Starting value for sequence (if None, will query from DB2)
            schema: Schema name
            
        Returns:
            Name of the created sequence
        """
        sequence_name = f"{table_name}_{column_name}_seq".lower()
        
        # Get starting value
        if start_value is None:
            # Try to get from MAXSEQUENCE first
            seq_info = self.get_maxsequence_info(table_name, schema)
            if seq_info and seq_info['column_name'] == column_name:
                start_value = seq_info['current_value'] + 1
                self.logger.info(f"Using MAXSEQUENCE value {seq_info['current_value']} for {table_name}.{column_name}")
            else:
                # Get max value from table
                max_value = self.get_max_value_from_table(table_name, column_name, schema)
                start_value = max_value + 1 if max_value > 0 else 1
                self.logger.info(f"Using max table value {max_value} for {table_name}.{column_name}")
        
        # Create sequence
        create_seq_ddl = f"""
            CREATE SEQUENCE "{sequence_name}"
            START WITH {start_value}
            INCREMENT BY 1
            NO MINVALUE
            NO MAXVALUE
            CACHE 1;
        """
        
        try:
            self.pg_connector.execute_ddl(create_seq_ddl)
            self.logger.info(f"✓ Created sequence {sequence_name} starting at {start_value}")
        except Exception as e:
            self.logger.warning(f"Could not create sequence {sequence_name}: {e}")
            return None
        
        # Set default value for column
        alter_table_ddl = f"""
            ALTER TABLE "{table_name}" 
            ALTER COLUMN "{column_name}" 
            SET DEFAULT nextval('"{sequence_name}"');
        """
        
        try:
            self.pg_connector.execute_ddl(alter_table_ddl)
            self.logger.info(f"✓ Set default value for {table_name}.{column_name}")
        except Exception as e:
            self.logger.warning(f"Could not set default for {table_name}.{column_name}: {e}")
        
        # Set sequence ownership
        alter_seq_ddl = f"""
            ALTER SEQUENCE "{sequence_name}" 
            OWNED BY "{table_name}"."{column_name}";
        """
        
        try:
            self.pg_connector.execute_ddl(alter_seq_ddl)
            self.logger.info(f"✓ Set sequence ownership for {sequence_name}")
        except Exception as e:
            self.logger.warning(f"Could not set sequence ownership: {e}")
        
        return sequence_name
    
    def create_sequences_for_table(
        self, 
        table_name: str, 
        pk_columns: List[str],
        schema: str = "MAXIMO"
    ) -> List[str]:
        """
        Create sequences for all primary key columns in a table.
        
        Args:
            table_name: Name of the table
            pk_columns: List of primary key column names
            schema: Schema name
            
        Returns:
            List of created sequence names
        """
        if not pk_columns:
            self.logger.debug(f"No primary key columns for {table_name}")
            return []
        
        created_sequences = []
        
        for pk_column in pk_columns:
            self.logger.info(f"Creating sequence for {table_name}.{pk_column}")
            seq_name = self.create_sequence_for_column(table_name, pk_column, schema=schema)
            if seq_name:
                created_sequences.append(seq_name)
        
        return created_sequences
