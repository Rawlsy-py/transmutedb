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
        
        # Use ensure_ctl_tables from the ctl.schema module to create all control tables
        # Import here to avoid circular dependency
        from transmutedb.ctl.schema import ensure_ctl_tables
        ensure_ctl_tables(con)
        
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


def make_fact_wizard(
    fact_name: str,
    tier: str = "all",
    project_dir: Path = Path("."),
    use_defaults: bool = False,
) -> None:
    """
    Scaffold a new fact table with metadata-driven configuration.
    
    Creates metadata entries and optionally generates sample data loading scripts
    for bronze, silver, and gold tiers.
    
    Args:
        fact_name: Name of the fact table to create
        tier: Which tier(s) to scaffold ('bronze', 'silver', 'gold', or 'all')
        project_dir: Project root directory
        use_defaults: If True, use default values without prompting
    
    Creates:
        - Metadata entries in fact_metadata and fact_column_metadata
        - Sample data loading script for the specified tier(s)
        - Documentation file explaining the fact table structure
    """
    from transmutedb.engine.duckdb import connect
    from transmutedb.ctl.schema import ensure_ctl_tables
    from transmutedb.flow.fact_builder import update_fact_metadata, add_fact_column
    
    # Connect to metadata database
    db_path = project_dir / "ctl.duckdb"
    if not db_path.exists():
        raise FileNotFoundError(
            f"Metadata database not found at {db_path}. "
            "Run 'transmutedb init' first to create a project."
        )
    
    con = connect(str(db_path))
    ensure_ctl_tables(con)
    
    # Create fact metadata entry
    fact_id = update_fact_metadata(
        con,
        fact_name=fact_name,
        source_table=None,
        target_schema="gold",
        description=f"Auto-generated fact table: {fact_name}",
    )
    
    # Add sample column metadata (user would extend this)
    # Add a sample dimension column
    add_fact_column(
        con,
        fact_name=fact_name,
        column_name="date_key",
        data_type="INTEGER",
        is_nullable=False,
        is_dimension=True,
        description="Date dimension key",
    )
    
    # Add a sample measure column
    add_fact_column(
        con,
        fact_name=fact_name,
        column_name="amount",
        data_type="DECIMAL(18,2)",
        is_nullable=False,
        is_measure=True,
        description="Transaction amount",
    )
    
    # Create scripts directory if it doesn't exist
    scripts_dir = project_dir / "scripts" / "facts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate appropriate tier scripts
    if tier in ("all", "bronze"):
        _generate_bronze_script(fact_name, scripts_dir)
    
    if tier in ("all", "silver"):
        _generate_silver_script(fact_name, scripts_dir)
    
    if tier in ("all", "gold"):
        _generate_gold_script(fact_name, scripts_dir)
    
    # Generate documentation
    _generate_fact_docs(fact_name, project_dir, tier)
    
    con.close()


def _generate_bronze_script(fact_name: str, scripts_dir: Path) -> None:
    """Generate a sample bronze tier loading script."""
    script_path = scripts_dir / f"{fact_name}_bronze_load.py"
    
    script_content = f'''"""
Bronze tier loading script for {fact_name} fact table.

This script demonstrates how to load raw data into the bronze tier.
Bronze tier characteristics:
- No schema or data type enforcement
- Auto-generated metadata columns (_load_date, _row_hash)
- Raw data stored as-is for audit purposes
"""
import polars as pl
from transmutedb.engine.duckdb import connect
from transmutedb.flow.fact_builder import load_bronze_fact


def load_{fact_name}_bronze():
    """Load sample data into bronze tier."""
    # Connect to database
    con = connect("ctl.duckdb")
    
    # Example: Create sample source data
    # In production, replace this with actual data loading logic
    source_data = pl.DataFrame({{
        "date_key": [20240101, 20240102, 20240103],
        "amount": [100.50, 200.75, 150.00],
    }})
    
    # Load to bronze tier with auto-generated metadata
    bronze_df = load_bronze_fact(
        con=con,
        fact_name="{fact_name}",
        source_data=source_data,
        bronze_schema="bronze",
    )
    
    print(f"Loaded {{len(bronze_df)}} rows to bronze.{fact_name}_bronze")
    print(f"Metadata columns added: _load_date, _row_hash")
    
    con.close()


if __name__ == "__main__":
    load_{fact_name}_bronze()
'''
    
    script_path.write_text(script_content)


def _generate_silver_script(fact_name: str, scripts_dir: Path) -> None:
    """Generate a sample silver tier processing script."""
    script_path = scripts_dir / f"{fact_name}_silver_process.py"
    
    script_content = f'''"""
Silver tier processing script for {fact_name} fact table.

This script applies schema and data quality rules from metadata.
Silver tier characteristics:
- Enforces data types from metadata
- Validates nullability constraints
- Applies data quality rules
- Tracks validation status
"""
from transmutedb.engine.duckdb import connect
from transmutedb.flow.fact_builder import apply_silver_rules


def process_{fact_name}_silver():
    """Apply silver tier rules and create validated table."""
    # Connect to database
    con = connect("ctl.duckdb")
    
    # Apply schema and validation rules from metadata
    result = apply_silver_rules(
        con=con,
        fact_name="{fact_name}",
        bronze_schema="bronze",
        silver_schema="silver",
    )
    
    print(f"Silver tier processing complete for {{result['fact_name']}}")
    print(f"Table created: {{result['silver_table']}}")
    print(f"Total rows: {{result['total_rows']}}")
    print(f"Valid rows: {{result['valid_rows']}}")
    print(f"Invalid rows: {{result['invalid_rows']}}")
    
    con.close()


if __name__ == "__main__":
    process_{fact_name}_silver()
'''
    
    script_path.write_text(script_content)


def _generate_gold_script(fact_name: str, scripts_dir: Path) -> None:
    """Generate a sample gold tier building script."""
    script_path = scripts_dir / f"{fact_name}_gold_build.py"
    
    script_content = f'''"""
Gold tier building script for {fact_name} fact table.

This script creates the final analytics-ready fact table.
Gold tier characteristics:
- Contains only validated data from silver tier
- Separates measures from dimensions
- Ready for reporting and analytics
"""
from transmutedb.engine.duckdb import connect
from transmutedb.flow.fact_builder import build_gold_fact


def build_{fact_name}_gold():
    """Build gold tier fact table from validated silver data."""
    # Connect to database
    con = connect("ctl.duckdb")
    
    # Build final fact table
    result = build_gold_fact(
        con=con,
        fact_name="{fact_name}",
        silver_schema="silver",
        gold_schema="gold",
    )
    
    print(f"Gold tier build complete for {{result['fact_name']}}")
    print(f"Table created: {{result['gold_table']}}")
    print(f"Total rows: {{result['total_rows']}}")
    print(f"Measures: {{', '.join(result['measures'])}}")
    print(f"Dimensions: {{', '.join(result['dimensions'])}}")
    
    con.close()


if __name__ == "__main__":
    build_{fact_name}_gold()
'''
    
    script_path.write_text(script_content)


def _generate_fact_docs(fact_name: str, project_dir: Path, tier: str) -> None:
    """Generate documentation for the fact table."""
    docs_dir = project_dir / "docs" / "facts"
    docs_dir.mkdir(parents=True, exist_ok=True)
    
    doc_path = docs_dir / f"{fact_name}.md"
    
    doc_content = f'''# {fact_name} Fact Table

Auto-generated fact table with metadata-driven configuration.

## Overview

This fact table follows the bronze-silver-gold medallion architecture:

### Bronze Tier
- **Purpose**: Raw data landing zone
- **Table**: `bronze.{fact_name}_bronze`
- **Features**: 
  - No schema enforcement
  - Auto-generated `_load_date` timestamp
  - Auto-generated `_row_hash` for change detection
  - Preserves raw data for audit purposes

### Silver Tier
- **Purpose**: Validated and typed data
- **Table**: `silver.{fact_name}_silver`
- **Features**:
  - Schema enforced from metadata (`fact_column_metadata`)
  - Data type validation
  - Nullability constraints
  - Data quality rules applied
  - Validation status tracked in `_is_valid` column

### Gold Tier
- **Purpose**: Analytics-ready fact table
- **Table**: `gold.{fact_name}`
- **Features**:
  - Contains only validated data
  - Measures and dimensions separated
  - Ready for reporting and BI tools
  - Optimized for query performance

## Metadata Configuration

The fact table structure is defined in:
- `fact_metadata` - Fact table configuration
- `fact_column_metadata` - Column definitions, types, and DQ rules

## Adding New Columns

To add a new column to the fact table:

1. Insert column metadata:
```sql
INSERT INTO fact_column_metadata (
    column_id, fact_id, column_name, data_type, 
    is_nullable, is_measure, is_dimension, description
)
VALUES (
    nextval('fact_column_metadata_seq'),
    (SELECT fact_id FROM fact_metadata WHERE fact_name = '{fact_name}'),
    'new_column_name',
    'VARCHAR',
    TRUE,
    FALSE,
    TRUE,
    'Description of the new column'
);
```

2. Re-run the silver and gold tier scripts to apply the new schema.

## Data Quality Rules

You can add data quality rules by updating the `dq_rule_type` and `dq_rule_params` 
columns in `fact_column_metadata`.

Example rule types:
- `range` - Validate numeric ranges
- `pattern` - Validate string patterns (regex)
- `lookup` - Validate against reference data
- `not_null` - Enforce non-null values

## Generated Scripts

The following scripts have been generated:
{f"- `scripts/facts/{fact_name}_bronze_load.py` - Load data to bronze tier" if tier in ("all", "bronze") else ""}
{f"- `scripts/facts/{fact_name}_silver_process.py` - Process bronze to silver tier" if tier in ("all", "silver") else ""}
{f"- `scripts/facts/{fact_name}_gold_build.py` - Build gold tier fact table" if tier in ("all", "gold") else ""}

## Usage

Run the scripts in order:

```bash
# 1. Load raw data to bronze
python scripts/facts/{fact_name}_bronze_load.py

# 2. Apply validation and create silver tier
python scripts/facts/{fact_name}_silver_process.py

# 3. Build final gold tier fact table
python scripts/facts/{fact_name}_gold_build.py
```
'''
    
    doc_path.write_text(doc_content)
