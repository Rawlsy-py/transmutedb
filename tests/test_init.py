"""Tests for the CLI init command."""
import tempfile
from pathlib import Path

import duckdb
import pytest
from typer.testing import CliRunner

from transmutedb.cli import app
from transmutedb.scaffold.generate import init_project


runner = CliRunner()


def test_init_project_creates_folders():
    """Test that init_project creates the required folder structure."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        init_project(project_path)
        
        # Check main directory exists
        assert project_path.exists()
        
        # Check required folders exist
        assert (project_path / "pipelines").exists()
        assert (project_path / "data" / "bronze").exists()
        assert (project_path / "data" / "silver").exists()
        assert (project_path / "data" / "gold").exists()
        assert (project_path / "logs").exists()
        assert (project_path / "scripts").exists()
        
        # Check .gitkeep files exist
        assert (project_path / "data" / "bronze" / ".gitkeep").exists()
        assert (project_path / "data" / "silver" / ".gitkeep").exists()
        assert (project_path / "data" / "gold" / ".gitkeep").exists()
        assert (project_path / "logs" / ".gitkeep").exists()


def test_init_project_creates_duckdb():
    """Test that init_project creates a DuckDB database with metadata tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        init_project(project_path)
        
        db_path = project_path / "ctl.duckdb"
        assert db_path.exists()
        
        # Verify tables exist
        con = duckdb.connect(str(db_path))
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        assert "run_log" in table_names
        assert "dq_results" in table_names
        
        # Verify run_log schema
        schema = con.execute("DESCRIBE run_log").fetchall()
        column_names = [col[0] for col in schema]
        assert "run_id" in column_names
        assert "pipeline" in column_names
        assert "status" in column_names
        
        # Verify dq_results schema
        schema = con.execute("DESCRIBE dq_results").fetchall()
        column_names = [col[0] for col in schema]
        assert "dq_id" in column_names
        assert "run_id" in column_names
        assert "check_name" in column_names
        
        con.close()


def test_init_project_creates_files():
    """Test that init_project creates README and .gitignore files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        init_project(project_path)
        
        # Check files exist
        assert (project_path / "README.md").exists()
        assert (project_path / ".gitignore").exists()
        
        # Check README content
        readme_content = (project_path / "README.md").read_text()
        assert "TransmuteDB Project" in readme_content
        assert "pipelines/" in readme_content
        
        # Check .gitignore content
        gitignore_content = (project_path / ".gitignore").read_text()
        assert "*.duckdb" in gitignore_content
        assert "__pycache__" in gitignore_content


def test_init_command_via_cli():
    """Test the init command through the CLI."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_cli_project"
        result = runner.invoke(app, ["init", str(project_path)])
        
        assert result.exit_code == 0
        assert "project initialized" in result.stdout
        assert project_path.exists()
        assert (project_path / "ctl.duckdb").exists()


def test_init_force_flag():
    """Test that --force flag overwrites existing files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        
        # First initialization
        init_project(project_path)
        
        # Modify README
        readme_path = project_path / "README.md"
        readme_path.write_text("Modified content")
        assert readme_path.read_text() == "Modified content"
        
        # Re-initialize with force
        init_project(project_path, force=True)
        
        # Check README was overwritten
        readme_content = readme_path.read_text()
        assert "TransmuteDB Project" in readme_content
        assert "Modified content" not in readme_content


def test_init_without_force_preserves_files():
    """Test that init without force doesn't overwrite existing files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir) / "test_project"
        
        # First initialization
        init_project(project_path)
        
        # Modify README
        readme_path = project_path / "README.md"
        original_content = readme_path.read_text()
        readme_path.write_text("Modified content")
        
        # Re-initialize without force
        init_project(project_path, force=False)
        
        # Check README was preserved
        assert readme_path.read_text() == "Modified content"


def test_init_in_existing_directory():
    """Test that init works in an existing directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        
        # Project path already exists (it's tmpdir)
        init_project(project_path)
        
        assert (project_path / "pipelines").exists()
        assert (project_path / "ctl.duckdb").exists()
