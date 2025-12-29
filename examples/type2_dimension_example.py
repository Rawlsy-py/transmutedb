"""
Example: Using Type 2 Slowly Changing Dimensions in TransmuteDB

This example demonstrates how to create and use Type 2 dimensions
to track historical changes in dimensional data.

Type 2 Slowly Changing Dimensions (SCD Type 2) track the complete history
of changes to dimension records, allowing you to analyze data as it was 
at any point in time.
"""

import duckdb
import polars as pl
from pathlib import Path

from transmutedb.ctl.schema import ensure_ctl_tables
from transmutedb.flow.entity_builder import (
    add_entity_column,
    build_gold_entity,
    load_bronze_entity,
    process_silver_entity,
    update_entity_metadata,
)


def main():
    """
    Example demonstrating Type 2 dimension functionality.
    
    We'll create a customer dimension that tracks changes to customer
    information over time.
    """
    
    # Connect to DuckDB and set up metadata tables
    con = duckdb.connect(":memory:")  # Use in-memory database for example
    ensure_ctl_tables(con)
    
    print("=" * 70)
    print("Type 2 Slowly Changing Dimension Example")
    print("=" * 70)
    print()
    
    # Step 1: Define entity metadata
    print("Step 1: Creating customer dimension entity...")
    update_entity_metadata(
        con,
        entity_name="customer",
        source_table="source_customers",
        target_schema="gold",
        entity_type="type2_dimension",  # Specify Type 2 dimension
        description="Customer dimension with historical tracking"
    )
    
    # Step 2: Define columns
    print("Step 2: Defining columns...")
    # Business key - identifies unique customers
    add_entity_column(
        con, "customer", "customer_id", "INTEGER", 
        is_nullable=False, 
        is_business_key=True  # Mark as business key
    )
    
    # Tracked attributes - changes will be historized
    add_entity_column(
        con, "customer", "customer_name", "VARCHAR", 
        is_nullable=False, 
        track_history=True  # Track changes to this field
    )
    
    add_entity_column(
        con, "customer", "email", "VARCHAR", 
        is_nullable=False, 
        track_history=True  # Track changes to this field
    )
    
    add_entity_column(
        con, "customer", "city", "VARCHAR", 
        is_nullable=True, 
        track_history=True  # Track changes to this field
    )
    
    # Step 3: Initial load
    print("\nStep 3: Initial load of customer data...")
    initial_data = pl.DataFrame({
        "customer_id": [1, 2, 3],
        "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
        "city": ["New York", "Los Angeles", "Chicago"]
    })
    
    print("Loading initial data:")
    print(initial_data)
    
    load_bronze_entity(con, "customer", initial_data)
    process_silver_entity(con, "customer")
    result = build_gold_entity(con, "customer")
    
    print(f"\n✓ Loaded {result['total_rows']} customer records")
    print(f"  Business Keys: {', '.join(result['business_keys'])}")
    print(f"  Tracked Columns: {', '.join(result['tracked_columns'])}")
    
    # Show initial dimension
    print("\nInitial dimension state:")
    dimension = con.execute("""
        SELECT customer_id, customer_name, email, city, 
               _is_current, _valid_from, _valid_to
        FROM gold.customer
        ORDER BY customer_id
    """).fetchdf()
    print(dimension.to_string())
    
    # Step 4: Update with changes
    print("\n" + "=" * 70)
    print("Step 4: Simulating changes to customer data...")
    
    # Alice changes her email and moves to Boston
    # Bob stays the same
    # Charlie changes his email
    updated_data = pl.DataFrame({
        "customer_id": [1, 2, 3],
        "customer_name": ["Alice Smith", "Bob Johnson", "Charlie Brown"],
        "email": ["alice.smith@newcompany.com", "bob@example.com", "charlie.b@example.com"],
        "city": ["Boston", "Los Angeles", "Chicago"]
    })
    
    print("\nUpdating with changed data:")
    print(updated_data)
    
    load_bronze_entity(con, "customer", updated_data)
    process_silver_entity(con, "customer")
    result = build_gold_entity(con, "customer")
    
    print(f"\n✓ Processed update")
    print(f"  Total rows in dimension: {result['total_rows']}")
    print(f"  New records added: {result['new_rows']}")
    print(f"  Records updated (historized): {result['updated_rows']}")
    
    # Show dimension after changes
    print("\nDimension state after changes:")
    dimension = con.execute("""
        SELECT customer_key, customer_id, customer_name, email, city,
               _is_current, _valid_from, _valid_to
        FROM gold.customer
        ORDER BY customer_id, _valid_from
    """).fetchdf()
    print(dimension.to_string())
    
    # Step 5: Query historical data
    print("\n" + "=" * 70)
    print("Step 5: Querying historical data...")
    
    # Show current records
    print("\nCurrent customer records (as of now):")
    current = con.execute("""
        SELECT customer_id, customer_name, email, city
        FROM gold.customer
        WHERE _is_current = TRUE
        ORDER BY customer_id
    """).fetchdf()
    print(current.to_string())
    
    # Show historical changes for customer 1 (Alice)
    print("\nHistory of changes for Alice (customer_id=1):")
    alice_history = con.execute("""
        SELECT customer_key, customer_name, email, city,
               _valid_from, _valid_to, _is_current
        FROM gold.customer
        WHERE customer_id = 1
        ORDER BY _valid_from
    """).fetchdf()
    print(alice_history.to_string())
    
    print("\n✓ Example complete!")
    print("\nKey concepts demonstrated:")
    print("  • Business keys uniquely identify dimension records")
    print("  • track_history=True marks columns for historical tracking")
    print("  • Old versions are closed with _valid_to timestamp")
    print("  • New versions are marked with _is_current=TRUE")
    print("  • Surrogate keys (customer_key) provide stable references")
    
    con.close()


if __name__ == "__main__":
    main()
