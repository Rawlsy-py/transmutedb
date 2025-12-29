"""Tests for Type 2 Slowly Changing Dimension functionality."""
import tempfile
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl
import pytest

from transmutedb.ctl.schema import ensure_ctl_tables
from transmutedb.flow.entity_builder import (
    add_entity_column,
    build_gold_entity,
    load_bronze_entity,
    process_silver_entity,
    update_entity_metadata,
)


def test_type2_dimension_initial_load():
    """Test initial load of a Type 2 dimension."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Create entity metadata for a customer dimension
        entity_id = update_entity_metadata(
            con,
            entity_name="customer",
            source_table="source_customers",
            target_schema="gold",
            entity_type="type2_dimension",
            description="Customer dimension with Type 2 SCD"
        )
        
        # Add columns with business key and tracked attributes
        add_entity_column(con, "customer", "customer_id", "INTEGER", is_nullable=False, is_business_key=True)
        add_entity_column(con, "customer", "customer_name", "VARCHAR", is_nullable=False, track_history=True)
        add_entity_column(con, "customer", "email", "VARCHAR", is_nullable=False, track_history=True)
        add_entity_column(con, "customer", "city", "VARCHAR", is_nullable=True, track_history=True)
        
        # Create sample source data
        source_data = pl.DataFrame({
            "customer_id": [1, 2, 3],
            "customer_name": ["Alice", "Bob", "Charlie"],
            "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
            "city": ["New York", "Los Angeles", "Chicago"]
        })
        
        # Load through bronze, silver, and gold tiers
        load_bronze_entity(con, "customer", source_data)
        process_silver_entity(con, "customer")
        result = build_gold_entity(con, "customer")
        
        # Verify results
        assert result["entity_type"] == "type2_dimension"
        assert result["total_rows"] == 3
        assert result["new_rows"] == 3
        assert result["updated_rows"] == 0
        assert "customer_id" in result["business_keys"]
        
        # Verify dimension table structure
        gold_data = con.execute("SELECT * FROM gold.customer").fetchdf()
        assert len(gold_data) == 3
        assert "customer_key" in gold_data.columns
        assert "_valid_from" in gold_data.columns
        assert "_valid_to" in gold_data.columns
        assert "_is_current" in gold_data.columns
        
        # All records should be current
        assert gold_data["_is_current"].all()
        assert gold_data["_valid_to"].isna().all()
        
        con.close()


def test_type2_dimension_update_with_changes():
    """Test Type 2 dimension update with changed records."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Setup entity
        update_entity_metadata(
            con,
            entity_name="customer",
            entity_type="type2_dimension"
        )
        
        add_entity_column(con, "customer", "customer_id", "INTEGER", is_nullable=False, is_business_key=True)
        add_entity_column(con, "customer", "customer_name", "VARCHAR", is_nullable=False, track_history=True)
        add_entity_column(con, "customer", "email", "VARCHAR", is_nullable=False, track_history=True)
        
        # Initial load
        initial_data = pl.DataFrame({
            "customer_id": [1, 2],
            "customer_name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"]
        })
        
        load_bronze_entity(con, "customer", initial_data)
        process_silver_entity(con, "customer")
        build_gold_entity(con, "customer")
        
        # Update with changes - Alice changes email
        updated_data = pl.DataFrame({
            "customer_id": [1, 2],
            "customer_name": ["Alice", "Bob"],
            "email": ["alice.new@example.com", "bob@example.com"]  # Alice's email changed
        })
        
        load_bronze_entity(con, "customer", updated_data)
        process_silver_entity(con, "customer")
        result = build_gold_entity(con, "customer")
        
        # Verify Type 2 behavior
        gold_data = con.execute("""
            SELECT customer_id, customer_name, email, _is_current, _valid_to 
            FROM gold.customer 
            ORDER BY customer_id, _valid_from
        """).fetchdf()
        
        # Should have 3 records: 2 for Alice (old and new), 1 for Bob
        assert len(gold_data) == 3
        
        # Check Alice's records
        alice_records = gold_data[gold_data["customer_id"] == 1]
        assert len(alice_records) == 2
        
        # Old record should be closed
        old_alice = alice_records[alice_records["email"] == "alice@example.com"].iloc[0]
        assert not old_alice["_is_current"]
        assert old_alice["_valid_to"] is not None and not pd.isna(old_alice["_valid_to"])
        
        # New record should be current
        new_alice = alice_records[alice_records["email"] == "alice.new@example.com"].iloc[0]
        assert new_alice["_is_current"]
        assert pd.isna(new_alice["_valid_to"])  # Use pd.isna for NULL timestamp
        
        # Bob should have only one current record
        bob_records = gold_data[gold_data["customer_id"] == 2]
        assert len(bob_records) == 1
        assert bob_records.iloc[0]["_is_current"]
        
        con.close()


def test_type2_dimension_new_records():
    """Test Type 2 dimension with new records added."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Setup entity
        update_entity_metadata(
            con,
            entity_name="customer",
            entity_type="type2_dimension"
        )
        
        add_entity_column(con, "customer", "customer_id", "INTEGER", is_nullable=False, is_business_key=True)
        add_entity_column(con, "customer", "customer_name", "VARCHAR", is_nullable=False, track_history=True)
        
        # Initial load
        initial_data = pl.DataFrame({
            "customer_id": [1, 2],
            "customer_name": ["Alice", "Bob"]
        })
        
        load_bronze_entity(con, "customer", initial_data)
        process_silver_entity(con, "customer")
        build_gold_entity(con, "customer")
        
        # Add new customer
        new_data = pl.DataFrame({
            "customer_id": [1, 2, 3],  # Added customer 3
            "customer_name": ["Alice", "Bob", "Charlie"]
        })
        
        load_bronze_entity(con, "customer", new_data)
        process_silver_entity(con, "customer")
        result = build_gold_entity(con, "customer")
        
        # Verify new record was added
        gold_data = con.execute("""
            SELECT customer_id, customer_name, _is_current 
            FROM gold.customer 
            ORDER BY customer_id
        """).fetchdf()
        
        assert len(gold_data) == 3
        assert gold_data["_is_current"].all()
        assert 3 in gold_data["customer_id"].values
        
        con.close()


def test_type2_dimension_no_changes():
    """Test Type 2 dimension with no changes (idempotent)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Setup entity
        update_entity_metadata(
            con,
            entity_name="customer",
            entity_type="type2_dimension"
        )
        
        add_entity_column(con, "customer", "customer_id", "INTEGER", is_nullable=False, is_business_key=True)
        add_entity_column(con, "customer", "customer_name", "VARCHAR", is_nullable=False, track_history=True)
        
        # Initial load
        data = pl.DataFrame({
            "customer_id": [1, 2],
            "customer_name": ["Alice", "Bob"]
        })
        
        load_bronze_entity(con, "customer", data)
        process_silver_entity(con, "customer")
        build_gold_entity(con, "customer")
        
        # Reload same data
        load_bronze_entity(con, "customer", data)
        process_silver_entity(con, "customer")
        result = build_gold_entity(con, "customer")
        
        # Should still have only 2 records, all current
        gold_data = con.execute("SELECT * FROM gold.customer").fetchdf()
        assert len(gold_data) == 2
        assert gold_data["_is_current"].all()
        assert gold_data["_valid_to"].isna().all()
        
        con.close()


def test_type2_dimension_requires_business_key():
    """Test that Type 2 dimension requires at least one business key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Setup entity without business key
        update_entity_metadata(
            con,
            entity_name="customer",
            entity_type="type2_dimension"
        )
        
        # Add columns but no business key
        add_entity_column(con, "customer", "customer_name", "VARCHAR", is_nullable=False)
        add_entity_column(con, "customer", "email", "VARCHAR", is_nullable=False)
        
        # Load data
        data = pl.DataFrame({
            "customer_name": ["Alice", "Bob"],
            "email": ["alice@example.com", "bob@example.com"]
        })
        
        load_bronze_entity(con, "customer", data)
        process_silver_entity(con, "customer")
        
        # Should raise error when building Type 2 dimension without business key
        with pytest.raises(ValueError, match="must have at least one business key"):
            build_gold_entity(con, "customer")
        
        con.close()


def test_type2_dimension_composite_business_key():
    """Test Type 2 dimension with composite business key."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Setup entity with composite business key
        update_entity_metadata(
            con,
            entity_name="product",
            entity_type="type2_dimension"
        )
        
        # Multiple business keys
        add_entity_column(con, "product", "product_code", "VARCHAR", is_nullable=False, is_business_key=True)
        add_entity_column(con, "product", "vendor_id", "INTEGER", is_nullable=False, is_business_key=True)
        add_entity_column(con, "product", "product_name", "VARCHAR", is_nullable=False, track_history=True)
        add_entity_column(con, "product", "price", "DECIMAL(10,2)", is_nullable=False, track_history=True)
        
        # Initial load
        initial_data = pl.DataFrame({
            "product_code": ["P001", "P002"],
            "vendor_id": [1, 1],
            "product_name": ["Widget A", "Widget B"],
            "price": [10.99, 15.99]
        })
        
        load_bronze_entity(con, "product", initial_data)
        process_silver_entity(con, "product")
        build_gold_entity(con, "product")
        
        # Update price for P001 from vendor 1
        updated_data = pl.DataFrame({
            "product_code": ["P001", "P002"],
            "vendor_id": [1, 1],
            "product_name": ["Widget A", "Widget B"],
            "price": [12.99, 15.99]  # P001 price changed
        })
        
        load_bronze_entity(con, "product", updated_data)
        process_silver_entity(con, "product")
        result = build_gold_entity(con, "product")
        
        # Should have 3 records: 2 for P001, 1 for P002
        gold_data = con.execute("SELECT * FROM gold.product").fetchdf()
        assert len(gold_data) == 3
        
        # Check P001 has 2 versions
        p001_records = gold_data[
            (gold_data["product_code"] == "P001") & (gold_data["vendor_id"] == 1)
        ]
        assert len(p001_records) == 2
        
        con.close()


def test_entity_type_metadata():
    """Test that entity_type is properly stored and retrieved."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        # Create different entity types
        update_entity_metadata(con, "fact_sales", entity_type="fact")
        update_entity_metadata(con, "dim_customer", entity_type="dimension")
        update_entity_metadata(con, "dim_product", entity_type="type2_dimension")
        
        # Verify entity types are stored
        result = con.execute("""
            SELECT entity_name, entity_type 
            FROM entity_metadata 
            ORDER BY entity_name
        """).fetchall()
        
        assert len(result) == 3
        entity_types = {name: etype for name, etype in result}
        
        assert entity_types["fact_sales"] == "fact"
        assert entity_types["dim_customer"] == "dimension"
        assert entity_types["dim_product"] == "type2_dimension"
        
        con.close()


def test_column_metadata_with_business_key_and_track_history():
    """Test that column metadata properly stores business key and track_history flags."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        con = duckdb.connect(str(db_path))
        
        ensure_ctl_tables(con)
        
        update_entity_metadata(con, "customer", entity_type="type2_dimension")
        
        # Add columns with different flags
        add_entity_column(con, "customer", "customer_id", "INTEGER", 
                         is_nullable=False, is_business_key=True)
        add_entity_column(con, "customer", "customer_name", "VARCHAR", 
                         is_nullable=False, track_history=True)
        add_entity_column(con, "customer", "created_date", "DATE", 
                         is_nullable=False, track_history=False)
        
        # Verify column metadata
        result = con.execute("""
            SELECT column_name, is_business_key, track_history
            FROM entity_column_metadata ecm
            JOIN entity_metadata em ON ecm.entity_id = em.entity_id
            WHERE em.entity_name = 'customer'
            ORDER BY column_name
        """).fetchall()
        
        assert len(result) == 3
        
        # Convert to dict for easier testing
        col_meta = {name: {"is_business_key": bk, "track_history": th} 
                    for name, bk, th in result}
        
        assert col_meta["customer_id"]["is_business_key"] is True
        assert col_meta["customer_id"]["track_history"] is False
        
        assert col_meta["customer_name"]["is_business_key"] is False
        assert col_meta["customer_name"]["track_history"] is True
        
        assert col_meta["created_date"]["is_business_key"] is False
        assert col_meta["created_date"]["track_history"] is False
        
        con.close()
