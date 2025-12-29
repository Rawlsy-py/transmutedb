"""DuckDB engine utilities."""
from __future__ import annotations

from typing import Any

import duckdb


def connect(uri: str) -> duckdb.DuckDBPyConnection:
    """
    Connect to a DuckDB database.
    
    Args:
        uri: Connection URI (e.g., 'duckdb://file:db.duckdb')
        
    Returns:
        DuckDB connection object
    """
    # Parse URI - support duckdb://file:path format
    if uri.startswith("duckdb://file:"):
        db_path = uri.replace("duckdb://file:", "")
    elif uri.startswith("duckdb://"):
        db_path = uri.replace("duckdb://", "")
    else:
        db_path = uri
    
    return duckdb.connect(db_path)


def fetch_df(con: duckdb.DuckDBPyConnection, query: str) -> Any:
    """
    Execute a query and return results as a DataFrame.
    
    Args:
        con: Database connection
        query: SQL query to execute
        
    Returns:
        Query results as DataFrame (Polars)
        
    Note: This is a placeholder implementation.
    """
    # TODO: Return polars DataFrame
    return con.execute(query).pl()
