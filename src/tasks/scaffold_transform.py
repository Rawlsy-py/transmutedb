import polars as pl

def transform_scaffold(records: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(records)
    return df
