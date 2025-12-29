"""Project scaffolding and initialization for TransmuteDB."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb


def init_project(path: Path, force: bool = False) -> None:
    """
    Initialize a new TransmuteDB project with required folder structure
    and metadata database.
    
    Args:
        path: Path where the project should be created
        force: If True, overwrite existing files
        
    Creates:
        - pipelines/ directory for pipeline definitions
        - data/ directory with bronze, silver, gold subdirectories
        - logs/ directory for pipeline logs
        - ctl.duckdb metadata database
    """
    project_path = path.resolve()
    
    # Create main directory if it doesn't exist
    if not project_path.exists():
        project_path.mkdir(parents=True, exist_ok=True)
    
    # Define folder structure for ETL + Orchestration
    folders = [
        "pipelines",           # Pipeline definitions (TOML configs)
        "data/bronze",         # Raw/staging data
        "data/silver",         # Cleaned/transformed data
        "data/gold",           # Aggregated/dimensional models
        "logs",                # Pipeline execution logs
        "scripts",             # Custom scripts
    ]
    
    # Folders that should have .gitkeep files to preserve structure
    gitkeep_folders = ["data/bronze", "data/silver", "data/gold", "logs"]
    
    # Create folders
    for folder in folders:
        folder_path = project_path / folder
        if folder_path.exists() and not force:
            continue
        folder_path.mkdir(parents=True, exist_ok=True)
    
    # Create DuckDB database for metadata
    db_path = project_path / "ctl.duckdb"
    
    # Only create if doesn't exist or force is True
    if not db_path.exists() or force:
        con = duckdb.connect(str(db_path))
        
        # Create run_log table for pipeline execution tracking
        con.execute("""
            CREATE TABLE IF NOT EXISTS run_log (
                run_id INTEGER PRIMARY KEY,
                pipeline VARCHAR,
                step VARCHAR,
                entity VARCHAR,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status VARCHAR,
                rows_in BIGINT,
                rows_out BIGINT,
                error_message VARCHAR
            )
        """)
        
        # Create sequence for run_id
        con.execute("""
            CREATE SEQUENCE IF NOT EXISTS run_log_seq START 1
        """)
        
        # Create dq_results table for data quality checks
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                dq_id INTEGER PRIMARY KEY,
                run_id INTEGER,
                pipeline VARCHAR,
                entity VARCHAR,
                check_name VARCHAR,
                check_type VARCHAR,
                passed BOOLEAN,
                rows_checked BIGINT,
                rows_failed BIGINT,
                created_at TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES run_log(run_id)
            )
        """)
        
        # Create sequence for dq_id
        con.execute("""
            CREATE SEQUENCE IF NOT EXISTS dq_results_seq START 1
        """)
        
        con.close()
    
    # Create a sample .gitignore file
    gitignore_path = project_path / ".gitignore"
    if not gitignore_path.exists() or force:
        gitignore_content = """# DuckDB database files
*.duckdb
*.duckdb.wal

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
env/
venv/
.venv

# Data files
data/bronze/*
data/silver/*
data/gold/*
!data/bronze/.gitkeep
!data/silver/.gitkeep
!data/gold/.gitkeep

# Logs
logs/*
!logs/.gitkeep

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db
"""
        gitignore_path.write_text(gitignore_content)
    
    # Create .gitkeep files in data directories
    for folder in gitkeep_folders:
        gitkeep_path = project_path / folder / ".gitkeep"
        if not gitkeep_path.exists():
            gitkeep_path.touch()
    
    # Create a sample README
    readme_path = project_path / "README.md"
    if not readme_path.exists() or force:
        readme_content = f"""# TransmuteDB Project

This project was initialized with TransmuteDB.

## Structure

- `pipelines/` - Pipeline configuration files (TOML)
- `data/bronze/` - Raw/staging data layer
- `data/silver/` - Cleaned/transformed data layer  
- `data/gold/` - Aggregated/dimensional models
- `logs/` - Pipeline execution logs
- `scripts/` - Custom scripts
- `ctl.duckdb` - Metadata database

## Getting Started

1. Create a pipeline configuration in `pipelines/`
2. Run your pipeline:
   ```bash
   transmutedb run <pipeline-name>
   ```

## Documentation

See the [TransmuteDB documentation](https://github.com/Rawlsy-py/transmutedb) for more information.
"""
        readme_path.write_text(readme_content)


def make_entity_wizard(pipeline: str, use_defaults: bool = False) -> None:
    """
    Interactively add an entity to a pipeline's TOML configuration.
    
    Args:
        pipeline: Name of the pipeline
        use_defaults: If True, use default values without prompting
    
    Note: This is a placeholder implementation.
    """
    # TODO: Implement entity wizard
    pass


def make_activity_wizard(pipeline: str) -> None:
    """
    Interactively add an activity to a pipeline flow.
    
    Args:
        pipeline: Name of the pipeline
    
    Note: This is a placeholder implementation.
    """
    # TODO: Implement activity wizard
    pass
