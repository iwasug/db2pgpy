"""Type conversion from DB2 to PostgreSQL."""
import re
from typing import Dict


class TypeConverter:
    """Convert DB2 data types to PostgreSQL equivalents."""
    
    # Type mapping dictionary
    TYPE_MAP: Dict[str, str] = {
        # Numeric types
        "SMALLINT": "SMALLINT",
        "INTEGER": "INTEGER",
        "INT": "INTEGER",
        "BIGINT": "BIGINT",
        "DECIMAL": "NUMERIC",
        "DEC": "NUMERIC",
        "NUMERIC": "NUMERIC",
        "REAL": "REAL",
        "DOUBLE": "DOUBLE PRECISION",
        "DOUBLE PRECISION": "DOUBLE PRECISION",
        "FLOAT": "DOUBLE PRECISION",
        "DECFLOAT": "NUMERIC",
        
        # String types
        "CHAR": "CHAR",
        "CHARACTER": "CHAR",
        "VARCHAR": "VARCHAR",
        "CHARACTER VARYING": "VARCHAR",
        "CLOB": "TEXT",
        "GRAPHIC": "CHAR",  # DB2 graphic types (Unicode) -> Char
        "VARGRAPHIC": "VARCHAR",
        "DBCLOB": "TEXT",
        "LONG VARCHAR": "TEXT",
        
        # Date/Time types
        "DATE": "DATE",
        "TIME": "TIME",
        "TIMESTAMP": "TIMESTAMP",
        
        # Binary types
        "BLOB": "BYTEA",
        "BINARY": "BYTEA",
        "VARBINARY": "BYTEA",
        "BINARY LARGE OBJECT": "BYTEA",
        
        # Special types
        "XML": "XML",
        "BOOLEAN": "BOOLEAN",
        "ROWID": "OID",
    }
    
    def convert(self, db2_type: str) -> str:
        """
        Convert a DB2 data type to PostgreSQL equivalent.
        
        Args:
            db2_type: DB2 data type string (e.g., "VARCHAR(255)", "DECIMAL(10,2)")
            
        Returns:
            PostgreSQL equivalent type
        """
        # Normalize the type string
        db2_type = db2_type.strip().upper()
        
        # Handle types with precision/scale (e.g., VARCHAR(255), DECIMAL(10,2))
        match = re.match(r'([A-Z\s]+)\s*\((.+)\)', db2_type)
        if match:
            base_type = match.group(1).strip()
            params = match.group(2).strip()
            
            # Map the base type
            pg_base_type = self.TYPE_MAP.get(base_type, "TEXT")
            
            # Special handling for DECIMAL -> NUMERIC
            if base_type in ("DECIMAL", "DEC"):
                return f"NUMERIC({params})"
            
            # Binary types don't preserve size in PostgreSQL
            if base_type in ("BINARY", "VARBINARY"):
                return "BYTEA"
            
            # For other types, preserve parameters
            return f"{pg_base_type}({params})"
        
        # Handle simple types without parameters
        pg_type = self.TYPE_MAP.get(db2_type, "TEXT")
        return pg_type
    
    def is_numeric(self, pg_type: str) -> bool:
        """
        Check if PostgreSQL type is numeric.
        
        Args:
            pg_type: PostgreSQL data type
            
        Returns:
            True if type is numeric
        """
        numeric_types = {
            "SMALLINT", "INTEGER", "BIGINT", "NUMERIC", "REAL", 
            "DOUBLE PRECISION", "SERIAL", "BIGSERIAL"
        }
        
        # Extract base type (remove parameters)
        base_type = re.match(r'([A-Z\s]+)', pg_type.upper())
        if base_type:
            return base_type.group(1).strip() in numeric_types
        
        return False
    
    def is_string(self, pg_type: str) -> bool:
        """
        Check if PostgreSQL type is string-based.
        
        Args:
            pg_type: PostgreSQL data type
            
        Returns:
            True if type is string-based
        """
        string_types = {"CHAR", "VARCHAR", "TEXT"}
        
        # Extract base type (remove parameters)
        base_type = re.match(r'([A-Z\s]+)', pg_type.upper())
        if base_type:
            return base_type.group(1).strip() in string_types
        
        return False
