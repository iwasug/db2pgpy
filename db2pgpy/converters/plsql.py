"""PL/SQL to PL/pgSQL converter module."""

import re
from typing import Tuple, List, Dict
from pathlib import Path


class PLSQLConverter:
    """Convert DB2 SQL PL procedures to PostgreSQL PL/pgSQL.
    
    This is a best-effort converter that handles common patterns.
    Complex procedures may require manual review and adjustment.
    """
    
    # Mapping of DB2 syntax to PostgreSQL syntax
    REPLACEMENTS = [
        # Language declaration
        (r'\bLANGUAGE\s+SQL\b', 'LANGUAGE plpgsql'),
        
        # Parameter declarations
        (r'\bIN\s+OUT\b', 'INOUT'),
        
        # BEGIN/END blocks
        (r'\bBEGIN\s+ATOMIC\b', 'BEGIN'),
        
        # Variable declarations
        (r'\bDECLARE\s+', '  DECLARE\n    '),
        
        # Assignment operator
        (r'\bSET\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*=', r'\1 :='),
        
        # CALL statement
        (r'\bCALL\s+', 'PERFORM '),
        
        # SELECT INTO
        (r'\bSELECT\s+(.*?)\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)', r'SELECT \1 INTO \2'),
        
        # Exception handling - basic conversion
        (r'\bDECLARE\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+CONDITION', r'-- CONDITION: \1'),
        (r'\bDECLARE\s+CONTINUE\s+HANDLER', 'EXCEPTION'),
        (r'\bDECLARE\s+EXIT\s+HANDLER', 'EXCEPTION'),
        
        # Data types
        (r'\bDECIMAL\((\d+),(\d+)\)', r'NUMERIC(\1,\2)'),
        (r'\bSMALLINT\b', 'SMALLINT'),
        (r'\bINTEGER\b', 'INTEGER'),
        (r'\bBIGINT\b', 'BIGINT'),
        (r'\bVARCHAR\((\d+)\)', r'VARCHAR(\1)'),
        (r'\bCHAR\((\d+)\)', r'CHAR(\1)'),
        (r'\bDATE\b', 'DATE'),
        (r'\bTIMESTAMP\b', 'TIMESTAMP'),
        
        # DB2 specific functions to PostgreSQL equivalents
        (r'\bCURRENT\s+DATE\b', 'CURRENT_DATE'),
        (r'\bCURRENT\s+TIME\b', 'CURRENT_TIME'),
        (r'\bCURRENT\s+TIMESTAMP\b', 'CURRENT_TIMESTAMP'),
        (r'\bVALUE\((.*?),(.*?)\)', r'COALESCE(\1,\2)'),
        
        # String functions
        (r'\bLENGTH\((.*?)\)', r'LENGTH(\1)'),
        (r'\bSUBSTR\((.*?)\)', r'SUBSTRING(\1)'),
        (r'\bCONCAT\((.*?)\)', r'CONCAT(\1)'),
        
        # Transaction control
        (r'\bCOMMIT\s+WORK\b', 'COMMIT'),
        (r'\bROLLBACK\s+WORK\b', 'ROLLBACK'),
    ]
    
    # Patterns that indicate complex features requiring manual review
    COMPLEX_PATTERNS = [
        (r'\bCURSOR\b', 'cursor usage'),
        (r'\bFOR\s+LOOP\b', 'FOR loop'),
        (r'\bWHILE\s+LOOP\b', 'WHILE loop'),
        (r'\bREPEAT\s+UNTIL\b', 'REPEAT UNTIL loop'),
        (r'\bGOTO\b', 'GOTO statement'),
        (r'\bLABEL\b', 'label usage'),
        (r'\bSIGNAL\b', 'SIGNAL statement'),
        (r'\bRESIGNAL\b', 'RESIGNAL statement'),
        (r'\bDYNAMIC\s+SQL\b', 'dynamic SQL'),
        (r'\bEXECUTE\s+IMMEDIATE\b', 'EXECUTE IMMEDIATE'),
        (r'\bPREPARE\b', 'prepared statement'),
        (r'\bOPEN\b.*\bCURSOR\b', 'cursor operations'),
        (r'\bFETCH\b', 'cursor fetch'),
        (r'\bTRIGGER\b', 'trigger definition'),
        (r'\bOLD\s+TABLE\b', 'transition table reference'),
        (r'\bNEW\s+TABLE\b', 'transition table reference'),
    ]
    
    def __init__(self, output_dir: str = './failed_conversions'):
        """Initialize PLSQLConverter.
        
        Args:
            output_dir: Directory to save failed conversion attempts
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def convert_procedure(self, procedure_def: Dict) -> Tuple[str, List[str]]:
        """Convert a DB2 procedure to PostgreSQL PL/pgSQL.
        
        Args:
            procedure_def: Dictionary with 'name', 'definition', 'parameters', etc.
            
        Returns:
            Tuple of (converted_code, warnings_list)
        """
        name = procedure_def.get('name', 'UNKNOWN')
        definition = procedure_def.get('definition', '')
        
        warnings = []
        
        if not definition:
            warnings.append(f"No definition found for procedure {name}")
            return '', warnings
        
        # Check for complex patterns
        complex_features = self._detect_complex_features(definition)
        if complex_features:
            warnings.extend([
                f"Complex feature detected: {feature}" 
                for feature in complex_features
            ])
            warnings.append("Manual review required")
        
        # Apply basic replacements
        converted = definition
        for pattern, replacement in self.REPLACEMENTS:
            converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        # Add header comment
        header = f"-- Converted from DB2 procedure: {name}\n"
        header += f"-- Original definition may require manual review\n"
        if warnings:
            header += f"-- WARNINGS: {len(warnings)} issues detected\n"
        header += "\n"
        
        converted = header + converted
        
        # Save failed conversions (those with warnings)
        if warnings:
            self._save_failed_conversion(name, definition, converted, warnings)
        
        return converted, warnings
    
    def convert_function(self, function_def: Dict) -> Tuple[str, List[str]]:
        """Convert a DB2 function to PostgreSQL function.
        
        Args:
            function_def: Dictionary with 'name', 'definition', etc.
            
        Returns:
            Tuple of (converted_code, warnings_list)
        """
        # Similar logic to convert_procedure but for functions
        name = function_def.get('name', 'UNKNOWN')
        definition = function_def.get('definition', '')
        
        warnings = []
        
        if not definition:
            warnings.append(f"No definition found for function {name}")
            return '', warnings
        
        # Check for complex patterns
        complex_features = self._detect_complex_features(definition)
        if complex_features:
            warnings.extend([
                f"Complex feature detected: {feature}"
                for feature in complex_features
            ])
        
        # Apply replacements
        converted = definition
        for pattern, replacement in self.REPLACEMENTS:
            converted = re.sub(pattern, replacement, converted, flags=re.IGNORECASE)
        
        # Add RETURNS clause handling for functions
        converted = re.sub(
            r'\bRETURNS\s+TABLE\b',
            'RETURNS TABLE',
            converted,
            flags=re.IGNORECASE
        )
        
        header = f"-- Converted from DB2 function: {name}\n\n"
        converted = header + converted
        
        if warnings:
            self._save_failed_conversion(name, definition, converted, warnings)
        
        return converted, warnings
    
    def _detect_complex_features(self, definition: str) -> List[str]:
        """Detect complex features that may not convert cleanly.
        
        Args:
            definition: Procedure or function definition
            
        Returns:
            List of detected complex features
        """
        detected = []
        for pattern, feature_name in self.COMPLEX_PATTERNS:
            if re.search(pattern, definition, re.IGNORECASE):
                detected.append(feature_name)
        
        return detected
    
    def _save_failed_conversion(self, name: str, original: str, 
                                converted: str, warnings: List[str]):
        """Save a failed conversion to a file for manual review.
        
        Args:
            name: Procedure/function name
            original: Original DB2 definition
            converted: Attempted conversion
            warnings: List of warnings
        """
        filename = self.output_dir / f"{name}_conversion.sql"
        
        with open(filename, 'w') as f:
            f.write(f"-- CONVERSION FAILED: {name}\n")
            f.write(f"-- Warnings:\n")
            for warning in warnings:
                f.write(f"--   - {warning}\n")
            f.write("\n")
            f.write("-- ORIGINAL DB2 CODE:\n")
            f.write("/*\n")
            f.write(original)
            f.write("\n*/\n\n")
            f.write("-- ATTEMPTED CONVERSION:\n")
            f.write(converted)
            f.write("\n")
    
    def get_conversion_summary(self, results: List[Tuple[str, List[str]]]) -> Dict[str, int]:
        """Generate summary statistics for conversions.
        
        Args:
            results: List of (converted_code, warnings) tuples
            
        Returns:
            Dictionary with conversion statistics
        """
        total = len(results)
        with_warnings = sum(1 for _, warnings in results if warnings)
        clean = total - with_warnings
        
        return {
            'total': total,
            'clean_conversions': clean,
            'conversions_with_warnings': with_warnings,
            'success_rate': (clean / total * 100) if total > 0 else 0
        }
