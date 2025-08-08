# src/transmutedb/cli/doctor.py
import typer, shutil
from rich import print as rprint

app = typer.Typer()


@app.command("all")
def doctor_all():
    ok = True
    if shutil.which("airflow") is None:
        rprint(":warning: Airflow not found (optional)")
    try:
        import duckdb  # noqa

        rprint(":white_check_mark: DuckDB import OK")
    except Exception as e:
        ok = False
        rprint(f":x: DuckDB import failed: {e}")
    try:
        import polars  # noqa

        rprint(":white_check_mark: Polars import OK")
    except Exception as e:
        ok = False
        rprint(f":x: Polars import failed: {e}")
    rprint(":sparkles: Env looks good!" if ok else ":boom: Fix issues above.")
