# src/transmutedb/cli/main.py
import typer
from rich import print as rprint

from . import generate, doctor

app = typer.Typer(add_completion=False, no_args_is_help=True)
app.add_typer(generate.app, name="generate", help="Render DAGs & transforms from YAML")
app.add_typer(doctor.app, name="doctor", help="Environment checks")


@app.command()
def run(pipeline: str, profile: str = "default"):
    """Execute a rendered pipeline locally (Polars+DuckDB) or hand off to Airflow (later)."""
    rprint(f":rocket: Running pipeline '{pipeline}' with profile '{profile}' (stub)")
    # TODO: load config, dispatch to backend runner


if __name__ == "__main__":
    app()
