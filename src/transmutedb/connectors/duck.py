import duckdb, polars as pl


def write_table(df: pl.DataFrame, *, path: str, schema: str, table: str):
    con = duckdb.connect(path)
    con.register("df", df)
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(f"CREATE OR REPLACE TABLE {schema}.{table} AS SELECT * FROM df")
    con.close()
