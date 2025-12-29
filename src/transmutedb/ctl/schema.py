"""Control table schema management."""
from __future__ import annotations

from typing import Any


def ensure_ctl_tables(con: Any) -> None:
    """
    Ensure control/metadata tables exist in the database.
    
    Args:
        con: Database connection
    """
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
    con.execute("CREATE SEQUENCE IF NOT EXISTS run_log_seq START 1")
    
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
            created_at TIMESTAMP
        )
    """)
    
    # Create sequence for dq_id
    con.execute("CREATE SEQUENCE IF NOT EXISTS dq_results_seq START 1")
    
    # Create entity_metadata table for entity configurations
    con.execute("""
        CREATE TABLE IF NOT EXISTS entity_metadata (
            entity_id INTEGER PRIMARY KEY,
            entity_name VARCHAR NOT NULL,
            source_table VARCHAR,
            target_schema VARCHAR DEFAULT 'gold',
            entity_type VARCHAR DEFAULT 'fact',
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(entity_name)
        )
    """)
    
    # Create sequence for entity_id
    con.execute("CREATE SEQUENCE IF NOT EXISTS entity_metadata_seq START 1")
    
    # Create entity_column_metadata table for column definitions and rules
    con.execute("""
        CREATE TABLE IF NOT EXISTS entity_column_metadata (
            column_id INTEGER PRIMARY KEY,
            entity_id INTEGER NOT NULL,
            column_name VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            is_nullable BOOLEAN DEFAULT TRUE,
            is_measure BOOLEAN DEFAULT FALSE,
            is_dimension BOOLEAN DEFAULT FALSE,
            is_business_key BOOLEAN DEFAULT FALSE,
            track_history BOOLEAN DEFAULT FALSE,
            default_value VARCHAR,
            description VARCHAR,
            dq_rule_type VARCHAR,
            dq_rule_params VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(entity_id, column_name),
            FOREIGN KEY (entity_id) REFERENCES entity_metadata(entity_id)
        )
    """)
    
    # Create sequence for column_id
    con.execute("CREATE SEQUENCE IF NOT EXISTS entity_column_metadata_seq START 1")
