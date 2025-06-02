from pathlib import Path

import typer
from jinja2 import Environment, FileSystemLoader

scaffold_app = typer.Typer()

TEMPLATES_DIR = Path(__file__).parent / "templates"


@scaffold_app.command("flow")
def scaffold_flow(object_name: str = typer.Argument(..., help="Name of the object")):
    """Scaffold a flow file for a given object."""
    typer.echo(f"Scaffolding: {object_name}")

    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template("flow.jinja")
    rendered = template.render(object_name=object_name.lower())

    output_dir = Path.cwd() / "src" / "flows"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{object_name.lower()}_flow.py"
    output_path.write_text(rendered)

    typer.echo(f"✅ Created: {output_path}")


@scaffold_app.command("task")
def scaffold_task(object_name: str = typer.Argument(..., help="Name of the object")):
    """Scaffold a task file for given object."""
    typer.echo(f"Scaffolding: {object_name}")

    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))
    template = env.get_template("task.jinja")
    rendered = template.render(object_name=object_name.lower())

    output_dir = Path.cwd() / "src" / "task"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{object_name.lower()}_task.py"
    output_path.write_text(rendered)

    typer.echo(f"✅ Created: {output_path}")
