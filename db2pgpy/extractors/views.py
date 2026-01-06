"""DB2 view extractor module."""

from typing import List, Dict, Any
from db2pgpy.connectors.db2 import DB2Connector


class ViewExtractor:
    """Extract view definitions from DB2."""
    
    def __init__(self, connector: DB2Connector):
        """Initialize ViewExtractor.
        
        Args:
            connector: DB2 database connector instance
        """
        self.connector = connector
    
    def extract_views(self, schema: str = None) -> List[Dict[str, Any]]:
        """Extract view definitions from DB2.
        
        Args:
            schema: Optional schema name to filter views
            
        Returns:
            List of view definitions with name, definition, and columns
        """
        # Query to get view definitions from SYSCAT.VIEWS
        if schema:
            schema_filter = f"WHERE VIEWSCHEMA = '{schema}'"
        else:
            schema_filter = ""
        
        query = f"""
            SELECT 
                VIEWNAME,
                VIEWSCHEMA,
                TEXT as DEFINITION
            FROM SYSCAT.VIEWS
            {schema_filter}
            ORDER BY VIEWNAME
        """
        
        rows = self.connector.execute_query(query)
        
        views = []
        for row in rows:
            view_name = row['VIEWNAME']
            
            # Get column information for the view
            columns = self._extract_view_columns(view_name, row.get('VIEWSCHEMA'))
            
            views.append({
                'name': view_name,
                'schema': row.get('VIEWSCHEMA'),
                'definition': row['DEFINITION'],
                'columns': columns
            })
        
        return views
    
    def _extract_view_columns(self, view_name: str, schema: str = None) -> List[Dict[str, str]]:
        """Extract column information for a view.
        
        Args:
            view_name: Name of the view
            schema: Optional schema name
            
        Returns:
            List of column definitions
        """
        schema_filter = f"AND TABSCHEMA = '{schema}'" if schema else ""
        
        query = f"""
            SELECT COLNAME, TYPENAME, NULLS
            FROM SYSCAT.COLUMNS
            WHERE TABNAME = '{view_name}'
            {schema_filter}
            ORDER BY COLNO
        """
        
        rows = self.connector.execute_query(query)
        
        columns = []
        for row in rows:
            columns.append({
                'name': row['COLNAME'],
                'type': row['TYPENAME'],
                'nullable': row['NULLS'] == 'Y'
            })
        
        return columns
