"""DB2 sequence extractor module."""

from typing import List, Dict, Any
from db2pgpy.connectors.db2 import DB2Connector


class SequenceExtractor:
    """Extract sequence definitions from DB2."""
    
    def __init__(self, connector: DB2Connector):
        """Initialize SequenceExtractor.
        
        Args:
            connector: DB2 database connector instance
        """
        self.connector = connector
    
    def extract_sequences(self, schema: str = None) -> List[Dict[str, Any]]:
        """Extract sequence definitions from DB2.
        
        Args:
            schema: Optional schema name to filter sequences
            
        Returns:
            List of sequence definitions with name, start, increment, min/max values
        """
        # Query to get sequence definitions from SYSCAT.SEQUENCES
        if schema:
            schema_filter = f"WHERE SEQSCHEMA = '{schema}'"
        else:
            schema_filter = "WHERE SEQSCHEMA NOT LIKE 'SYS%'"
        
        query = f"""
            SELECT 
                SEQNAME,
                SEQSCHEMA,
                START,
                INCREMENT,
                MINVALUE,
                MAXVALUE,
                CYCLE,
                CACHE,
                ORDER as ORDERED,
                DATATYPEID,
                NEXTCACHEFIRSTVALUE,
                SEQTYPE
            FROM SYSCAT.SEQUENCES
            {schema_filter}
            ORDER BY SEQNAME
        """
        
        rows = self.connector.execute_query(query)
        
        sequences = []
        for row in rows:
            sequences.append({
                'name': row['SEQNAME'],
                'schema': row.get('SEQSCHEMA'),
                'start': row['START'],
                'increment': row['INCREMENT'],
                'min_value': row.get('MINVALUE'),
                'max_value': row.get('MAXVALUE'),
                'cycle': row.get('CYCLE') == 'Y',
                'cache': row.get('CACHE'),
                'ordered': row.get('ORDERED') == 'Y',
                'data_type_id': row.get('DATATYPEID'),
                'next_value': row.get('NEXTCACHEFIRSTVALUE'),
                'seq_type': row.get('SEQTYPE')
            })
        
        return sequences
