"""Progress tracking and resume capability for migrations."""
import json
from pathlib import Path
from typing import Dict, Optional, Any
from datetime import datetime


class ProgressTracker:
    """Track migration progress with resume capability."""
    
    def __init__(self, state_file: str):
        """
        Initialize progress tracker.
        
        Args:
            state_file: Path to JSON file for storing progress state
        """
        self.state_file = Path(state_file)
        self.state: Dict[str, Any] = {
            "phase": "not_started",
            "completed_phases": [],
            "tables": {},
            "last_updated": None,
        }
        
        # Load existing state if available
        if self.state_file.exists():
            self._load()
    
    def _load(self):
        """Load progress state from file."""
        try:
            with open(self.state_file, 'r') as f:
                self.state = json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            # If file is corrupted, start fresh
            pass
    
    def save(self):
        """Save current progress state to file."""
        self.state["last_updated"] = datetime.now().isoformat()
        
        # Ensure parent directory exists
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f, indent=2)
    
    def get_phase(self) -> str:
        """
        Get current migration phase.
        
        Returns:
            Current phase name
        """
        return self.state["phase"]
    
    def set_phase(self, phase: str):
        """
        Set current migration phase.
        
        Args:
            phase: Phase name (e.g., 'schema', 'data', 'constraints')
        """
        self.state["phase"] = phase
        self.save()
    
    def is_completed(self, phase: str) -> bool:
        """
        Check if a phase is completed.
        
        Args:
            phase: Phase name
            
        Returns:
            True if phase is completed
        """
        return phase in self.state["completed_phases"]
    
    def mark_completed(self, phase: str):
        """
        Mark a phase as completed.
        
        Args:
            phase: Phase name
        """
        if phase not in self.state["completed_phases"]:
            self.state["completed_phases"].append(phase)
            self.save()
    
    def update_table_progress(self, table_name: str, rows_migrated: int, total_rows: int):
        """
        Update progress for a specific table.
        
        Args:
            table_name: Name of the table
            rows_migrated: Number of rows migrated so far
            total_rows: Total number of rows to migrate
        """
        self.state["tables"][table_name] = {
            "rows_migrated": rows_migrated,
            "total_rows": total_rows,
            "percentage": round((rows_migrated / total_rows * 100), 2) if total_rows > 0 else 0,
            "last_updated": datetime.now().isoformat(),
        }
        self.save()
    
    def update_progress(self, table_name: str, rows_transferred: int):
        """
        Update progress for a table (simplified interface).
        
        Args:
            table_name: Name of the table
            rows_transferred: Number of rows transferred so far
        """
        # Get existing progress or create new entry
        existing = self.state["tables"].get(table_name, {})
        total_rows = existing.get("total_rows", rows_transferred)
        
        self.update_table_progress(table_name, rows_transferred, total_rows)
    
    def get_table_progress(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get progress for a specific table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dictionary with progress info or None if table not found
        """
        return self.state["tables"].get(table_name)
    
    def get_all_table_progress(self) -> Dict[str, Dict[str, Any]]:
        """
        Get progress for all tables.
        
        Returns:
            Dictionary mapping table names to progress info
        """
        return self.state["tables"]
    
    def reset(self):
        """Reset all progress state."""
        self.state = {
            "phase": "not_started",
            "completed_phases": [],
            "tables": {},
            "last_updated": None,
        }
        self.save()
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary of migration progress.
        
        Returns:
            Dictionary with summary statistics
        """
        tables = self.state["tables"]
        total_tables = len(tables)
        completed_tables = sum(1 for t in tables.values() if t["rows_migrated"] == t["total_rows"])
        
        total_rows = sum(t["total_rows"] for t in tables.values())
        migrated_rows = sum(t["rows_migrated"] for t in tables.values())
        
        return {
            "current_phase": self.state["phase"],
            "completed_phases": self.state["completed_phases"],
            "total_tables": total_tables,
            "completed_tables": completed_tables,
            "total_rows": total_rows,
            "migrated_rows": migrated_rows,
            "overall_percentage": round((migrated_rows / total_rows * 100), 2) if total_rows > 0 else 0,
            "last_updated": self.state["last_updated"],
        }
