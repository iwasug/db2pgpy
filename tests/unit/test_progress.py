import json
import pytest
from pathlib import Path
from db2pgpy.progress import ProgressTracker


@pytest.fixture
def tracker(tmp_path):
    """Create a ProgressTracker instance."""
    state_file = tmp_path / "progress.json"
    return ProgressTracker(str(state_file))


def test_initial_state(tracker):
    """Test initial progress state."""
    assert tracker.get_phase() == "not_started"
    assert not tracker.is_completed("schema")
    assert not tracker.is_completed("data")


def test_mark_completed(tracker):
    """Test marking a phase as completed."""
    tracker.mark_completed("schema")
    assert tracker.is_completed("schema")
    assert not tracker.is_completed("data")


def test_set_phase(tracker):
    """Test setting current phase."""
    tracker.set_phase("schema")
    assert tracker.get_phase() == "schema"
    
    tracker.set_phase("data")
    assert tracker.get_phase() == "data"


def test_persistence(tmp_path):
    """Test that progress is persisted to disk."""
    state_file = tmp_path / "progress.json"
    
    # Create tracker and set state
    tracker1 = ProgressTracker(str(state_file))
    tracker1.set_phase("schema")
    tracker1.mark_completed("schema")
    tracker1.save()
    
    # Create new tracker and verify state is loaded
    tracker2 = ProgressTracker(str(state_file))
    assert tracker2.get_phase() == "schema"
    assert tracker2.is_completed("schema")


def test_resume_capability(tmp_path):
    """Test resume from saved state."""
    state_file = tmp_path / "progress.json"
    
    # Save initial progress
    tracker1 = ProgressTracker(str(state_file))
    tracker1.set_phase("data")
    tracker1.mark_completed("schema")
    tracker1.update_table_progress("table1", 100, 500)
    tracker1.save()
    
    # Resume with new tracker
    tracker2 = ProgressTracker(str(state_file))
    assert tracker2.get_phase() == "data"
    assert tracker2.is_completed("schema")
    
    table_progress = tracker2.get_table_progress("table1")
    assert table_progress["rows_migrated"] == 100
    assert table_progress["total_rows"] == 500


def test_table_progress(tracker):
    """Test table-level progress tracking."""
    tracker.update_table_progress("users", 50, 100)
    
    progress = tracker.get_table_progress("users")
    assert progress["rows_migrated"] == 50
    assert progress["total_rows"] == 100
    assert progress["percentage"] == 50.0
    
    # Update progress
    tracker.update_table_progress("users", 75, 100)
    progress = tracker.get_table_progress("users")
    assert progress["rows_migrated"] == 75
    assert progress["percentage"] == 75.0


def test_reset(tracker):
    """Test resetting progress."""
    tracker.set_phase("data")
    tracker.mark_completed("schema")
    tracker.update_table_progress("table1", 50, 100)
    
    tracker.reset()
    
    assert tracker.get_phase() == "not_started"
    assert not tracker.is_completed("schema")
    assert tracker.get_table_progress("table1") is None
