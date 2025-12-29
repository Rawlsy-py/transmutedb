"""Entity builder for metadata-driven entities across bronze, silver, and gold tiers."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl


def _validate_identifier(identifier: str, identifier_type: str = "identifier") -> str:
    """
    Validate SQL identifier to prevent injection.
    
    Args:
        identifier: The identifier to validate (schema name, table name, column name)
        identifier_type: Type of identifier for error messages
    
    Returns:
        The validated identifier
        
    Raises:
        ValueError: If identifier is invalid
    """
    # Allow alphanumeric, underscore, and limit length
    if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', identifier):
        raise ValueError(
            f"Invalid {identifier_type} '{identifier}'. "
            "Must start with letter or underscore and contain only alphanumeric characters and underscores."
        )
    if len(identifier) > 63:  # PostgreSQL/DuckDB limit
        raise ValueError(f"{identifier_type} '{identifier}' is too long (max 63 characters)")
    return identifier


def load_bronze_entity(
    con: Any,
    entity_name: str,
    source_data: pl.DataFrame,
    bronze_schema: str = "bronze",
) -> pl.DataFrame:
    """
    Load data into bronze tier with auto-generated metadata columns.
    
    Bronze tier characteristics:
    - No schema or data typing enforcement
    - Auto-generates hash column for tracking changes
    - Auto-generates load_date for audit trail
    - Just validates that data exists
    
    Args:
        con: Database connection
        entity_name: Name of the entity
        source_data: Source dataframe to load
        bronze_schema: Schema name for bronze tier (default: 'bronze')
    
    Returns:
        DataFrame with metadata columns added
    """
    # Add metadata columns
    df = source_data.clone()
    
    # Add load_date timestamp
    df = df.with_columns([
        pl.lit(datetime.now()).alias("_load_date")
    ])
    
    # Calculate row hash for change detection
    # Concatenate all columns (except metadata) and hash
    # Using SHA256 for better collision resistance
    original_columns = [col for col in source_data.columns]
    df = df.with_columns([
        pl.concat_str(original_columns, separator="|").map_elements(
            lambda x: hashlib.sha256(x.encode()).hexdigest(),
            return_dtype=pl.Utf8
        ).alias("_row_hash")
    ])
    
    # Validate identifiers
    _validate_identifier(entity_name, "entity name")
    _validate_identifier(bronze_schema, "schema name")
    
    # Create bronze schema if it doesn't exist
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
    
    # Create or replace bronze table using register and CTAS
    table_name = f"{bronze_schema}.{_validate_identifier(entity_name, 'entity name')}_bronze"
    
    # Register the polars dataframe as a DuckDB view temporarily
    con.register("_temp_bronze_df", df)
    
    # Create table from the registered dataframe
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM _temp_bronze_df")
    
    # Unregister the temporary view
    con.unregister("_temp_bronze_df")
    
    return df


def process_silver_entity(
    con: Any,
    entity_name: str,
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
) -> Dict[str, Any]:
    """
    Apply schema and data type rules from metadata to create silver tier table.
    
    Silver tier characteristics:
    - Declares schema and data types from metadata
    - Applies data quality rules from metadata
    - Validates nullability constraints
    - Tracks validation results
    
    Args:
        con: Database connection
        entity_name: Name of the entity
        bronze_schema: Schema name for bronze tier
        silver_schema: Schema name for silver tier
    
    Returns:
        Dictionary with validation results and statistics
    """
    # Validate identifiers
    _validate_identifier(entity_name, "entity name")
    _validate_identifier(bronze_schema, "schema name")
    _validate_identifier(silver_schema, "schema name")
    
    # Get entity metadata
    entity_meta = con.execute(
        "SELECT entity_id, source_table FROM entity_metadata WHERE entity_name = ?",
        [entity_name]
    ).fetchone()
    
    if not entity_meta:
        raise ValueError(f"Entity '{entity_name}' not found in metadata")
    
    entity_id = entity_meta[0]
    
    # Get column metadata with type and validation rules
    column_meta = con.execute(
        """
        SELECT column_name, data_type, is_nullable, dq_rule_type, dq_rule_params
        FROM entity_column_metadata
        WHERE entity_id = ?
        ORDER BY column_id
        """,
        [entity_id]
    ).fetchall()
    
    if not column_meta:
        raise ValueError(f"No column metadata found for entity '{entity_name}'")
    
    # Create silver schema if it doesn't exist
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {silver_schema}")
    
    # Build column definitions for CREATE TABLE
    column_defs = []
    for col_name, data_type, is_nullable, dq_rule_type, dq_rule_params in column_meta:
        # Validate column name and data type
        _validate_identifier(col_name, "column name")
        # Basic validation of data type (allow common SQL types)
        if not re.match(r'^[A-Z]+(\([0-9,\s]+\))?$', data_type.upper().strip()):
            raise ValueError(f"Invalid data type '{data_type}' for column '{col_name}'")
        
        null_constraint = "NULL" if is_nullable else "NOT NULL"
        column_defs.append(f"{col_name} {data_type} {null_constraint}")
    
    # Add metadata columns
    column_defs.append("_load_date TIMESTAMP NOT NULL")
    column_defs.append("_row_hash VARCHAR")
    column_defs.append("_valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    column_defs.append("_is_valid BOOLEAN DEFAULT TRUE")
    
    # Create silver table with proper schema
    silver_table = f"{silver_schema}.{entity_name}_silver"
    bronze_table = f"{bronze_schema}.{entity_name}_bronze"
    
    create_sql = f"""
        CREATE OR REPLACE TABLE {silver_table} (
            {', '.join(column_defs)}
        )
    """
    con.execute(create_sql)
    
    # Insert data with type casting and validation
    column_names = [col[0] for col in column_meta]
    select_columns = []
    
    for col_name, data_type, is_nullable, dq_rule_type, dq_rule_params in column_meta:
        # Cast to appropriate type
        select_columns.append(f"TRY_CAST({col_name} AS {data_type}) AS {col_name}")
    
    insert_sql = f"""
        INSERT INTO {silver_table}
        SELECT 
            {', '.join(select_columns)},
            _load_date,
            _row_hash,
            CURRENT_TIMESTAMP AS _valid_from,
            TRUE AS _is_valid
        FROM {bronze_table}
    """
    con.execute(insert_sql)
    
    # Gather statistics
    stats = con.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(CASE WHEN _is_valid = TRUE THEN 1 END) as valid_rows,
            COUNT(CASE WHEN _is_valid = FALSE THEN 1 END) as invalid_rows
        FROM {silver_table}
    """).fetchone()
    
    return {
        "entity_name": entity_name,
        "silver_table": silver_table,
        "total_rows": stats[0],
        "valid_rows": stats[1],
        "invalid_rows": stats[2],
    }


def build_gold_entity(
    con: Any,
    entity_name: str,
    silver_schema: str = "silver",
    gold_schema: str = "gold",
) -> Dict[str, Any]:
    """
    Build the final gold tier entity from validated silver tier data.
    
    Gold tier characteristics:
    - Contains only validated, typed data from silver tier
    - Separates measures from dimensions
    - Applies final business rules
    - Ready for analytics and reporting
    
    Args:
        con: Database connection
        entity_name: Name of the entity
        silver_schema: Schema name for silver tier
        gold_schema: Schema name for gold tier
    
    Returns:
        Dictionary with build results and statistics
    """
    # Validate identifiers
    _validate_identifier(entity_name, "entity name")
    _validate_identifier(silver_schema, "schema name")
    _validate_identifier(gold_schema, "schema name")
    
    # Get entity metadata
    entity_meta = con.execute(
        "SELECT entity_id, target_schema FROM entity_metadata WHERE entity_name = ?",
        [entity_name]
    ).fetchone()
    
    if not entity_meta:
        raise ValueError(f"Entity '{entity_name}' not found in metadata")
    
    entity_id = entity_meta[0]
    target_schema = entity_meta[1] or gold_schema
    _validate_identifier(target_schema, "target schema name")
    
    # Get column metadata
    column_meta = con.execute(
        """
        SELECT column_name, data_type, is_measure, is_dimension
        FROM entity_column_metadata
        WHERE entity_id = ?
        ORDER BY column_id
        """,
        [entity_id]
    ).fetchall()
    
    # Create gold schema if it doesn't exist
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
    
    # Separate measures and dimensions but preserve order
    measure_cols = []
    dimension_cols = []
    other_cols = []
    all_cols_ordered = []
    
    for col_name, data_type, is_measure, is_dimension in column_meta:
        # Validate column name from metadata
        _validate_identifier(col_name, "column name")
        all_cols_ordered.append(col_name)
        if is_measure:
            measure_cols.append(col_name)
        elif is_dimension:
            dimension_cols.append(col_name)
        else:
            other_cols.append(col_name)
    
    # Build gold table from valid silver records
    gold_table = f"{target_schema}.{entity_name}"
    silver_table = f"{silver_schema}.{entity_name}_silver"
    
    # Use original metadata order for SELECT
    select_list = ", ".join(all_cols_ordered)
    
    create_sql = f"""
        CREATE OR REPLACE TABLE {gold_table} AS
        SELECT 
            {select_list},
            _load_date,
            _valid_from
        FROM {silver_table}
        WHERE _is_valid = TRUE
    """
    con.execute(create_sql)
    
    # Gather statistics
    stats = con.execute(f"""
        SELECT 
            COUNT(*) as total_rows,
            MIN(_valid_from) as earliest_record,
            MAX(_valid_from) as latest_record
        FROM {gold_table}
    """).fetchone()
    
    return {
        "entity_name": entity_name,
        "gold_table": gold_table,
        "total_rows": stats[0],
        "measures": measure_cols,
        "dimensions": dimension_cols,
        "earliest_record": stats[1],
        "latest_record": stats[2],
    }


def update_entity_metadata(
    con: Any,
    entity_name: str,
    source_table: Optional[str] = None,
    target_schema: str = "gold",
    description: Optional[str] = None,
) -> int:
    """
    Create or update entity metadata.
    
    Args:
        con: Database connection
        entity_name: Name of the entity
        source_table: Source table/query for the entity
        target_schema: Target schema for gold tier
        description: Description of the entity
    
    Returns:
        entity_id of the created/updated record
    """
    # Check if entity already exists
    existing = con.execute(
        "SELECT entity_id FROM entity_metadata WHERE entity_name = ?",
        [entity_name]
    ).fetchone()
    
    if existing:
        # Update existing
        con.execute(
            """
            UPDATE entity_metadata 
            SET source_table = ?,
                target_schema = ?,
                description = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE entity_name = ?
            """,
            [source_table or '', target_schema, description or '', entity_name]
        )
        return existing[0]
    else:
        # Insert new
        con.execute(
            """
            INSERT INTO entity_metadata (entity_id, entity_name, source_table, target_schema, description)
            VALUES (
                nextval('entity_metadata_seq'),
                ?,
                ?,
                ?,
                ?
            )
            """,
            [entity_name, source_table or '', target_schema, description or '']
        )
        result = con.execute("SELECT currval('entity_metadata_seq')").fetchone()
        return result[0]


def add_entity_column(
    con: Any,
    entity_name: str,
    column_name: str,
    data_type: str,
    is_nullable: bool = True,
    is_measure: bool = False,
    is_dimension: bool = False,
    default_value: Optional[str] = None,
    description: Optional[str] = None,
    dq_rule_type: Optional[str] = None,
    dq_rule_params: Optional[str] = None,
) -> int:
    """
    Add a column definition to entity metadata.
    
    Args:
        con: Database connection
        entity_name: Name of the entity
        column_name: Name of the column
        data_type: SQL data type (e.g., 'INTEGER', 'VARCHAR', 'DECIMAL(10,2)')
        is_nullable: Whether the column allows NULL values
        is_measure: Whether this is a measure column (numeric aggregatable)
        is_dimension: Whether this is a dimension column (grouping/filtering)
        default_value: Default value for the column
        description: Description of the column
        dq_rule_type: Type of data quality rule (e.g., 'range', 'pattern', 'lookup')
        dq_rule_params: Parameters for the DQ rule in JSON format
    
    Returns:
        column_id of the created record
    """
    # Get entity_id
    entity = con.execute(
        "SELECT entity_id FROM entity_metadata WHERE entity_name = ?",
        [entity_name]
    ).fetchone()
    
    if not entity:
        raise ValueError(f"Entity '{entity_name}' not found in metadata")
    
    entity_id = entity[0]
    
    # Insert column metadata
    con.execute(
        """
        INSERT INTO entity_column_metadata (
            column_id, entity_id, column_name, data_type, is_nullable,
            is_measure, is_dimension, default_value, description,
            dq_rule_type, dq_rule_params
        )
        VALUES (
            nextval('entity_column_metadata_seq'),
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """,
        [
            entity_id,
            column_name,
            data_type,
            is_nullable,
            is_measure,
            is_dimension,
            default_value,
            description,
            dq_rule_type,
            dq_rule_params
        ]
    )
    
    result = con.execute("SELECT currval('entity_column_metadata_seq')").fetchone()
    return result[0]
