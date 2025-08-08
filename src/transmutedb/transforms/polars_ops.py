import polars as pl


def coerce_types(df: pl.DataFrame, mapping: dict[str, str]) -> pl.DataFrame:
    casts = []
    for col, ty in mapping.items():
        try:
            casts.append(pl.col(col).cast(pl.datatypes.dtype(ty)))
        except Exception:
            casts.append(pl.col(col))  # fail-soft for now
    return df.with_columns(casts)


def scd2_merge(
    staging: pl.DataFrame,
    current: pl.DataFrame,
    business_key: list[str],
    hash_cols: list[str],
    effective_col: str = "valid_from",
    end_col: str = "valid_to",
    current_flag_col: str = "is_current",
) -> pl.DataFrame:
    # simple, single-batch demo merge; we’ll harden later
    stg = staging.with_columns(
        [
            pl.concat_str([pl.col(c).cast(pl.Utf8) for c in hash_cols], separator="|")
            .hash()
            .alias("_hash"),
            pl.lit(None).alias(end_col),
            pl.lit(True).alias(current_flag_col),
            pl.lit(pl.datetime.now()).alias(effective_col),
        ]
    )
    if current.is_empty():
        return stg

    cur = current
    j = cur.join(stg.select(business_key + ["_hash"]), on=business_key, how="left")
    closing = (
        j.filter(
            pl.col("_hash_right").is_not_null()
            & (pl.col("_hash_right") != pl.col("_hash"))
        )
        .with_columns(
            [
                pl.lit(pl.datetime.now()).alias(end_col),
                pl.lit(False).alias(current_flag_col),
            ]
        )
        .drop("_hash_right")
    )
    new_curr = stg.join(cur.select(business_key), on=business_key, how="left_anti")
    merged = pl.concat([cur, closing, new_curr], how="vertical_relaxed")
    return merged
