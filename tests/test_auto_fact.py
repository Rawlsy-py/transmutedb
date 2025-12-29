"""Tests for auto fact table feature."""
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from transmutedb.cli import app
from transmutedb.ctl.schema import ensure_ctl_tables
from transmutedb.engine.duckdb import connect
from transmutedb.flow.fact_builder import (
    add_fact_column,
    apply_silver_rules,
    build_gold_fact,
    load_bronze_fact,
    update_fact_metadata,
)
from transmutedb.scaffold.generate import init_project, make_fact_wizard


runner = CliRunner()


def test_ensure_ctl_tables_creates_fact_metadata_tables():
    """Test that ensure_ctl_tables creates fact metadata tables."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Verify fact_metadata table exists
        tables = con.execute("SHOW TABLES").fetchall()
        table_names = [t[0] for t in tables]
        
        assert "fact_metadata" in table_names
        assert "fact_column_metadata" in table_names
        
        # Verify fact_metadata schema
        schema = con.execute("DESCRIBE fact_metadata").fetchall()
        column_names = [col[0] for col in schema]
        assert "fact_id" in column_names
        assert "fact_name" in column_names
        assert "source_table" in column_names
        assert "target_schema" in column_names
        
        # Verify fact_column_metadata schema
        schema = con.execute("DESCRIBE fact_column_metadata").fetchall()
        column_names = [col[0] for col in schema]
        assert "column_id" in column_names
        assert "fact_id" in column_names
        assert "column_name" in column_names
        assert "data_type" in column_names
        assert "is_measure" in column_names
        assert "is_dimension" in column_names
        assert "dq_rule_type" in column_names
        
        con.close()


def test_update_fact_metadata():
    """Test creating and updating fact metadata."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        ensure_ctl_tables(con)
        
        # Create new fact metadata
        fact_id = update_fact_metadata(
            con,
            fact_name="test_fact",
            source_table="source.table",
            target_schema="gold",
            description="Test fact table",
        )
        
        assert fact_id is not None
        assert fact_id > 0
        
        # Verify metadata was created
        result = con.execute(
            "SELECT fact_name, source_table, target_schema, description "
            "FROM fact_metadata WHERE fact_id = ?",
            [fact_id]
        ).fetchone()
        
        assert result[0] == "test_fact"
        assert result[1] == "source.table"
        assert result[2] == "gold"
        assert result[3] == "Test fact table"
        
        # Update existing fact metadata
        fact_id2 = update_fact_metadata(
            con,
            fact_name="test_fact",
            source_table="new_source.table",
            description="Updated description",
        )
        
        # Should return same fact_id
        assert fact_id2 == fact_id
        
        # Verify update
        result = con.execute(
            "SELECT source_table, description FROM fact_metadata WHERE fact_id = ?",
            [fact_id]
        ).fetchone()
        
        assert result[0] == "new_source.table"
        assert result[1] == "Updated description"
        
        con.close()


def test_add_fact_column():
    """Test adding column metadata to a fact table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        ensure_ctl_tables(con)
        
        # Create fact metadata
        fact_id = update_fact_metadata(con, fact_name="test_fact")
        
        # Add measure column
        column_id = add_fact_column(
            con,
            fact_name="test_fact",
            column_name="total_amount",
            data_type="DECIMAL(18,2)",
            is_nullable=False,
            is_measure=True,
            description="Total transaction amount",
        )
        
        assert column_id > 0
        
        # Verify column metadata
        result = con.execute(
            "SELECT column_name, data_type, is_measure, is_dimension "
            "FROM fact_column_metadata WHERE column_id = ?",
            [column_id]
        ).fetchone()
        
        assert result[0] == "total_amount"
        assert result[1] == "DECIMAL(18,2)"
        assert result[2] is True
        assert result[3] is False
        
        # Add dimension column
        column_id2 = add_fact_column(
            con,
            fact_name="test_fact",
            column_name="customer_id",
            data_type="INTEGER",
            is_dimension=True,
            dq_rule_type="not_null",
        )
        
        assert column_id2 > column_id
        
        # Verify both columns exist
        count = con.execute(
            "SELECT COUNT(*) FROM fact_column_metadata WHERE fact_id = ?",
            [fact_id]
        ).fetchone()[0]
        
        assert count == 2
        
        con.close()


def test_load_bronze_fact():
    """Test loading data to bronze tier with metadata columns."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        # Create sample data
        source_data = pl.DataFrame({
            "customer_id": [1, 2, 3],
            "amount": [100.50, 200.75, 150.25],
            "date_key": [20240101, 20240102, 20240103],
        })
        
        # Load to bronze
        result_df = load_bronze_fact(
            con,
            fact_name="test_fact",
            source_data=source_data,
        )
        
        # Verify metadata columns were added
        assert "_load_date" in result_df.columns
        assert "_row_hash" in result_df.columns
        
        # Verify all original columns are present
        assert "customer_id" in result_df.columns
        assert "amount" in result_df.columns
        assert "date_key" in result_df.columns
        
        # Verify row hashes are unique (for different data)
        hashes = result_df["_row_hash"].to_list()
        assert len(hashes) == len(set(hashes))
        
        # Verify table was created
        tables = con.execute("SHOW TABLES FROM bronze").fetchall()
        table_names = [t[0] for t in tables]
        assert "test_fact_bronze" in table_names
        
        # Verify data in table
        count = con.execute("SELECT COUNT(*) FROM bronze.test_fact_bronze").fetchone()[0]
        assert count == 3
        
        con.close()


def test_apply_silver_rules():
    """Test applying silver tier rules from metadata."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        ensure_ctl_tables(con)
        
        # Setup metadata
        fact_id = update_fact_metadata(con, fact_name="test_fact")
        add_fact_column(
            con, "test_fact", "customer_id", "INTEGER", is_nullable=False, is_dimension=True
        )
        add_fact_column(
            con, "test_fact", "amount", "DECIMAL(18,2)", is_nullable=False, is_measure=True
        )
        add_fact_column(
            con, "test_fact", "date_key", "INTEGER", is_nullable=False, is_dimension=True
        )
        
        # Load bronze data
        source_data = pl.DataFrame({
            "customer_id": [1, 2, 3],
            "amount": [100.50, 200.75, 150.25],
            "date_key": [20240101, 20240102, 20240103],
        })
        load_bronze_fact(con, "test_fact", source_data)
        
        # Apply silver rules
        result = apply_silver_rules(
            con,
            fact_name="test_fact",
            bronze_schema="bronze",
            silver_schema="silver",
        )
        
        # Verify result
        assert result["fact_name"] == "test_fact"
        assert result["total_rows"] == 3
        assert result["valid_rows"] == 3
        assert result["invalid_rows"] == 0
        
        # Verify silver table exists
        tables = con.execute("SHOW TABLES FROM silver").fetchall()
        table_names = [t[0] for t in tables]
        assert "test_fact_silver" in table_names
        
        # Verify schema enforcement
        schema = con.execute("DESCRIBE silver.test_fact_silver").fetchall()
        column_types = {col[0]: col[1] for col in schema}
        assert "INTEGER" in column_types["customer_id"].upper()
        assert "DECIMAL" in column_types["amount"].upper()
        
        con.close()


def test_build_gold_fact():
    """Test building gold tier fact table."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        ensure_ctl_tables(con)
        
        # Setup complete pipeline
        fact_id = update_fact_metadata(con, fact_name="test_fact", target_schema="gold")
        add_fact_column(
            con, "test_fact", "customer_id", "INTEGER", is_nullable=False, is_dimension=True
        )
        add_fact_column(
            con, "test_fact", "amount", "DECIMAL(18,2)", is_nullable=False, is_measure=True
        )
        add_fact_column(
            con, "test_fact", "date_key", "INTEGER", is_nullable=False, is_dimension=True
        )
        
        # Load bronze
        source_data = pl.DataFrame({
            "customer_id": [1, 2, 3, 4],
            "amount": [100.50, 200.75, 150.25, 300.00],
            "date_key": [20240101, 20240102, 20240103, 20240104],
        })
        load_bronze_fact(con, "test_fact", source_data)
        
        # Apply silver
        apply_silver_rules(con, "test_fact")
        
        # Build gold
        result = build_gold_fact(
            con,
            fact_name="test_fact",
            silver_schema="silver",
            gold_schema="gold",
        )
        
        # Verify result
        assert result["fact_name"] == "test_fact"
        assert result["total_rows"] == 4
        assert "amount" in result["measures"]
        assert "customer_id" in result["dimensions"]
        assert "date_key" in result["dimensions"]
        
        # Verify gold table exists
        tables = con.execute("SHOW TABLES FROM gold").fetchall()
        table_names = [t[0] for t in tables]
        assert "test_fact" in table_names
        
        # Verify data quality
        gold_data = con.execute("SELECT * FROM gold.test_fact ORDER BY customer_id").fetchall()
        assert len(gold_data) == 4
        assert gold_data[0][0] == 1  # customer_id
        assert float(gold_data[0][1]) == 100.50  # amount
        
        con.close()


def test_make_fact_wizard_creates_metadata_and_scripts():
    """Test that make_fact_wizard creates metadata entries and scripts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        
        # Initialize project
        init_project(project_path)
        
        # Create fact table
        make_fact_wizard(
            fact_name="sales_transactions",
            tier="all",
            project_dir=project_path,
            use_defaults=True,
        )
        
        # Verify metadata was created
        db_path = project_path / "ctl.duckdb"
        con = duckdb.connect(str(db_path))
        
        # Check fact metadata
        fact = con.execute(
            "SELECT fact_name FROM fact_metadata WHERE fact_name = 'sales_transactions'"
        ).fetchone()
        assert fact is not None
        
        # Check column metadata (should have sample columns)
        columns = con.execute(
            "SELECT COUNT(*) FROM fact_column_metadata WHERE fact_id = "
            "(SELECT fact_id FROM fact_metadata WHERE fact_name = 'sales_transactions')"
        ).fetchone()[0]
        assert columns >= 2  # At least the sample columns
        
        con.close()
        
        # Verify scripts were created
        scripts_dir = project_path / "scripts" / "facts"
        assert scripts_dir.exists()
        assert (scripts_dir / "sales_transactions_bronze_load.py").exists()
        assert (scripts_dir / "sales_transactions_silver_process.py").exists()
        assert (scripts_dir / "sales_transactions_gold_build.py").exists()
        
        # Verify documentation was created
        docs_dir = project_path / "docs" / "facts"
        assert docs_dir.exists()
        assert (docs_dir / "sales_transactions.md").exists()
        
        # Verify script content
        bronze_script = (scripts_dir / "sales_transactions_bronze_load.py").read_text()
        assert "load_bronze_fact" in bronze_script
        assert "sales_transactions" in bronze_script


def test_make_fact_wizard_tier_option():
    """Test that make_fact_wizard respects tier option."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        init_project(project_path)
        
        # Create fact with only bronze tier
        make_fact_wizard(
            fact_name="bronze_only_fact",
            tier="bronze",
            project_dir=project_path,
            use_defaults=True,
        )
        
        scripts_dir = project_path / "scripts" / "facts"
        
        # Verify only bronze script was created
        assert (scripts_dir / "bronze_only_fact_bronze_load.py").exists()
        assert not (scripts_dir / "bronze_only_fact_silver_process.py").exists()
        assert not (scripts_dir / "bronze_only_fact_gold_build.py").exists()


def test_make_fact_cli_command():
    """Test the make fact CLI command."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        
        # Initialize project first
        init_project(project_path)
        
        # Run make fact command
        result = runner.invoke(
            app,
            [
                "make",
                "fact",
                "test_fact",
                "--tier",
                "all",
                "--defaults",
                "--project-dir",
                str(project_path),
            ],
        )
        
        assert result.exit_code == 0
        assert "scaffolded successfully" in result.stdout
        assert "Metadata entries created" in result.stdout
        assert "Scripts generated" in result.stdout
        
        # Verify files were created
        assert (project_path / "scripts" / "facts" / "test_fact_bronze_load.py").exists()
        assert (project_path / "docs" / "facts" / "test_fact.md").exists()


def test_make_fact_cli_without_init_fails():
    """Test that make fact fails if project is not initialized."""
    with tempfile.TemporaryDirectory() as tmpdir:
        project_path = Path(tmpdir)
        
        # Try to create fact without initializing project
        result = runner.invoke(
            app,
            [
                "make",
                "fact",
                "test_fact",
                "--project-dir",
                str(project_path),
            ],
        )
        
        assert result.exit_code == 1
        assert "Error" in result.stdout or "not found" in result.stdout.lower()


def test_full_bronze_silver_gold_workflow():
    """Test complete workflow from bronze to gold."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        ensure_ctl_tables(con)
        
        # Setup metadata
        update_fact_metadata(con, fact_name="complete_test", target_schema="gold")
        add_fact_column(con, "complete_test", "id", "INTEGER", is_nullable=False)
        add_fact_column(
            con, "complete_test", "revenue", "DECIMAL(18,2)", is_nullable=False, is_measure=True
        )
        add_fact_column(
            con, "complete_test", "region", "VARCHAR", is_nullable=False, is_dimension=True
        )
        
        # 1. Bronze: Load raw data
        source_data = pl.DataFrame({
            "id": [1, 2, 3],
            "revenue": [1000.50, 2500.75, 1800.25],
            "region": ["North", "South", "East"],
        })
        bronze_df = load_bronze_fact(con, "complete_test", source_data)
        assert len(bronze_df) == 3
        assert "_load_date" in bronze_df.columns
        
        # 2. Silver: Apply validation
        silver_result = apply_silver_rules(con, "complete_test")
        assert silver_result["total_rows"] == 3
        assert silver_result["valid_rows"] == 3
        
        # 3. Gold: Build final table
        gold_result = build_gold_fact(con, "complete_test")
        assert gold_result["total_rows"] == 3
        assert "revenue" in gold_result["measures"]
        assert "region" in gold_result["dimensions"]
        
        # Verify final data
        gold_data = con.execute(
            "SELECT id, revenue, region FROM gold.complete_test ORDER BY id"
        ).fetchall()
        assert len(gold_data) == 3
        assert gold_data[0][0] == 1
        assert float(gold_data[0][1]) == 1000.50
        assert gold_data[0][2] == "North"
        
        con.close()
