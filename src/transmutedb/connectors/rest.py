import httpx, polars as pl


def read_jsonl(
    url: str, headers: dict | None = None, params: dict | None = None
) -> pl.DataFrame:
    r = httpx.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and "results" in data:
        data = data["results"]
    return pl.from_dicts(data if isinstance(data, list) else [data])
