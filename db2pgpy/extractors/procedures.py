"""DB2 stored procedure extractor module."""

from typing import List, Dict, Any
from db2pgpy.connectors.db2 import DB2Connector


class ProcedureExtractor:
    """Extract stored procedure definitions from DB2."""
    
    def __init__(self, connector: DB2Connector):
        """Initialize ProcedureExtractor.
        
        Args:
            connector: DB2 database connector instance
        """
        self.connector = connector
    
    def extract_procedures(self, schema: str = None) -> List[Dict[str, Any]]:
        """Extract stored procedure definitions from DB2.
        
        Args:
            schema: Optional schema name to filter procedures
            
        Returns:
            List of procedure definitions with name, definition, and parameters
        """
        # Query to get procedure definitions from SYSCAT.ROUTINES
        if schema:
            schema_filter = f"WHERE ROUTINESCHEMA = '{schema}'"
        else:
            schema_filter = "WHERE ROUTINESCHEMA NOT LIKE 'SYS%'"
        
        query = f"""
            SELECT 
                ROUTINENAME,
                ROUTINESCHEMA,
                TEXT,
                LANGUAGE,
                SPECIFICNAME
            FROM SYSCAT.ROUTINES
            {schema_filter}
            AND ROUTINETYPE = 'P'
            ORDER BY ROUTINENAME
        """
        
        rows = self.connector.execute_query(query)
        
        procedures = []
        for row in rows:
            procedure_name = row['ROUTINENAME']
            specific_name = row['SPECIFICNAME']
            
            # Get parameter information for the procedure
            params = self._extract_procedure_params(specific_name, row.get('ROUTINESCHEMA'))
            
            procedures.append({
                'name': procedure_name,
                'schema': row.get('ROUTINESCHEMA'),
                'definition': row.get('TEXT'),
                'language': row.get('LANGUAGE'),
                'specific_name': specific_name,
                'parameters': params
            })
        
        return procedures
    
    def _extract_procedure_params(self, specific_name: str, schema: str = None) -> List[Dict[str, Any]]:
        """Extract parameter information for a procedure.
        
        Args:
            specific_name: Specific name of the procedure
            schema: Optional schema name
            
        Returns:
            List of parameter definitions
        """
        schema_filter = f"AND ROUTINESCHEMA = '{schema}'" if schema else ""
        
        query = f"""
            SELECT 
                PARMNAME,
                TYPENAME,
                ROWTYPE,
                ORDINAL,
                LENGTH
            FROM SYSCAT.ROUTINEPARMS
            WHERE SPECIFICNAME = '{specific_name}'
            {schema_filter}
            ORDER BY ORDINAL
        """
        
        rows = self.connector.execute_query(query)
        
        params = []
        for row in rows:
            # ROWTYPE: 'I' = IN, 'O' = OUT, 'B' = INOUT
            param_type = row['ROWTYPE']
            if param_type == 'I':
                direction = 'IN'
            elif param_type == 'O':
                direction = 'OUT'
            elif param_type == 'B':
                direction = 'INOUT'
            else:
                direction = 'IN'  # Default
            
            params.append({
                'name': row['PARMNAME'],
                'type': row['TYPENAME'],
                'direction': direction,
                'ordinal': row['ORDINAL'],
                'length': row.get('LENGTH')
            })
        
        return params
