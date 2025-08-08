from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Literal, Optional, Dict, List

Medallion = Literal["bronze", "silver", "gold"]


class Source(BaseModel):
    name: str
    kind: Literal["rest", "s3", "gcs", "local", "jdbc", "duck"]
    options: Dict[str, object] = Field(default_factory=dict)
    schema: Optional[Dict[str, str]] = None


class SCD2(BaseModel):
    business_key: List[str]
    hashdiff_cols: List[str]
    effective_col: str = "valid_from"
    end_col: str = "valid_to"
    current_flag_col: str = "is_current"


class Model(BaseModel):
    name: str
    layer: Medallion
    materialization: Literal["view", "table", "incremental"] = "incremental"
    transform: Optional[str] = None
    sql: Optional[str] = None
    scd2: Optional[SCD2] = None
    dq_rules: Dict[str, Dict] = Field(default_factory=dict)


class Pipeline(BaseModel):
    name: str
    schedule: str  # cron
    target: Dict[
        str, object
    ]  # e.g., {"kind":"duck","path":"./demo.duckdb","schema":"gold"}
    sources: List[Source]
    models: List[Model]


class ProjectConfig(BaseModel):
    pipelines: List[Pipeline] = Field(default_factory=list)
