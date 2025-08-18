# src/transmutedb/cli/main.py
import typer
from rich import print as rprint

from . import generate, doctor

app = typer.Typer(add_completion=False, no_args_is_help=True)
