"""Pydantic models for pipeline configuration."""
from __future__ import annotations

from pydantic import BaseModel


class PipelineConfig(BaseModel):
    """
    Pipeline configuration model.
    
    Note: This is a placeholder implementation.
    """
    name: str = "default"
    version: str = "0.1.0"
