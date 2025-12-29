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
    
    # Create fact_metadata table for fact table configurations
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_metadata (
            fact_id INTEGER PRIMARY KEY,
            fact_name VARCHAR NOT NULL,
            source_table VARCHAR,
            target_schema VARCHAR DEFAULT 'gold',
            description VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fact_name)
        )
    """)
    
    # Create sequence for fact_id
    con.execute("CREATE SEQUENCE IF NOT EXISTS fact_metadata_seq START 1")
    
    # Create fact_column_metadata table for column definitions and rules
    con.execute("""
        CREATE TABLE IF NOT EXISTS fact_column_metadata (
            column_id INTEGER PRIMARY KEY,
            fact_id INTEGER NOT NULL,
            column_name VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            is_nullable BOOLEAN DEFAULT TRUE,
            is_measure BOOLEAN DEFAULT FALSE,
            is_dimension BOOLEAN DEFAULT FALSE,
            default_value VARCHAR,
            description VARCHAR,
            dq_rule_type VARCHAR,
            dq_rule_params VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fact_id, column_name),
            FOREIGN KEY (fact_id) REFERENCES fact_metadata(fact_id)
        )
    """)
    
    # Create sequence for column_id
    con.execute("CREATE SEQUENCE IF NOT EXISTS fact_column_metadata_seq START 1")
