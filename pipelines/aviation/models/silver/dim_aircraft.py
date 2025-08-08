import polars as pl
from transmutedb.transforms.polars_ops import coerce_types, scd2_merge
from transmutedb.connectors.duck import write_table

# NOTE: in real code, this comes from a connector by kind; we inline simple demo:
df = pl.from_dicts([{"aircraft_id": 1, "model": "A320", "seats": 180}])

df = coerce_types(df, {"aircraft_id": "i64", "model": "str", "seats": "i64"})

current = pl.DataFrame()
df = scd2_merge(df, current,
    ["aircraft_id"],
    ["model", "seats"])


write_table(df, path="./aviation.duckdb", schema="silver", table="dim_aircraft")