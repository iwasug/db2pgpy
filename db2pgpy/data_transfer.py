"""Data transfer module for batch migration."""

import time
from typing import Dict, Any, List
from db2pgpy.connectors.db2 import DB2Connector
from db2pgpy.connectors.postgres import PostgresConnector
from db2pgpy.progress import ProgressTracker


class DataTransfer:
    """Transfer data from DB2 to PostgreSQL in batches."""
    
    def __init__(self, db2_connector: DB2Connector, pg_connector: PostgresConnector, 
                 batch_size: int = 1000):
        """Initialize DataTransfer.
        
        Args:
            db2_connector: DB2 database connector
            pg_connector: PostgreSQL database connector
            batch_size: Number of rows per batch
        """
        self.db2_connector = db2_connector
        self.pg_connector = pg_connector
        self.batch_size = batch_size
    
    def transfer_table(self, table_name: str, progress_tracker: ProgressTracker) -> Dict[str, Any]:
        """Transfer a single table from DB2 to PostgreSQL.
        
        Args:
            table_name: Name of the table to transfer
            progress_tracker: Progress tracker instance
            
        Returns:
            Dictionary with transfer statistics (rows_transferred, time_taken)
        """
        start_time = time.time()
        total_rows = 0
        
        # Fetch data in batches from DB2
        batches = self.db2_connector.fetch_table_data(table_name, self.batch_size)
        
        for batch in batches:
            if batch:
                # Insert batch into PostgreSQL
                self.pg_connector.bulk_insert(table_name, batch)
                total_rows += len(batch)
                
                # Update progress
                progress_tracker.update_progress(
                    table_name,
                    rows_transferred=total_rows
                )
        
        time_taken = time.time() - start_time
        
        return {
            'rows_transferred': total_rows,
            'time_taken': time_taken
        }
    
    def transfer_tables(self, table_list: List[str], 
                       progress_tracker: ProgressTracker) -> Dict[str, Dict[str, Any]]:
        """Transfer multiple tables from DB2 to PostgreSQL.
        
        Args:
            table_list: List of table names to transfer
            progress_tracker: Progress tracker instance
            
        Returns:
            Dictionary mapping table names to their transfer statistics
        """
        results = {}
        
        for table_name in table_list:
            results[table_name] = self.transfer_table(table_name, progress_tracker)
        
        return results
