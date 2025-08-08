# src/transmutedb/cli/generate.py
import typer
from rich import print as rprint

app = typer.Typer()


@app.command("all")
def generate_all(project_dir: str = "."):
    """Validate YAML, render transforms & DAGs from templates (stub for now)."""
    rprint(f":wrench: Generating code from YAML under {project_dir} (stub)")


# src/transmutedb/cli/generate.py
import typer
from rich import print as rprint

app = typer.Typer()


@app.command("all")
def generate_all(project_dir: str = "."):
    """Validate YAML, render transforms & DAGs from templates (stub for now)."""
    rprint(f":wrench: Generating code from YAML under {project_dir} (stub)")
