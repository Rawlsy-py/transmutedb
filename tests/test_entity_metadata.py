"""Tests for entity metadata functionality."""
import tempfile
from pathlib import Path

import duckdb
import pytest

from transmutedb.ctl.schema import ensure_ctl_tables
from transmutedb.scaffold.generate import init_project


def test_ensure_ctl_tables_creates_entity_metadata_tables():
    """Test that ensure_ctl_tables creates entity metadata tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Verify entity_metadata table exists
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        assert "entity_metadata" in table_names
        assert "entity_column_metadata" in table_names
        
        # Verify entity_metadata schema
        schema = con.execute("DESCRIBE entity_metadata").fetchall()
        column_names = [col[0] for col in schema]
        assert "entity_id" in column_names
        assert "entity_name" in column_names
        assert "source_table" in column_names
        assert "target_schema" in column_names
        
        # Verify entity_column_metadata schema
        schema = con.execute("DESCRIBE entity_column_metadata").fetchall()
        column_names = [col[0] for col in schema]
        assert "column_id" in column_names
        assert "entity_id" in column_names
        assert "column_name" in column_names
        assert "data_type" in column_names
        assert "is_measure" in column_names
        assert "is_dimension" in column_names
        assert "dq_rule_type" in column_names
        
        con.close()


def test_init_project_creates_entity_metadata():
    """Test that init_project creates entity metadata tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        init_project(project_path)
        
        db_path = project_path / "ctl.duckdb"
        assert db_path.exists()
        
        con = duckdb.connect(str(db_path))
        
        # Verify entity tables exist
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        assert "entity_metadata" in table_names
        assert "entity_column_metadata" in table_names
        
        con.close()
