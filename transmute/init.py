import typer
from pathlib import Path
import shutil

init_app = typer.Typer()


@init_app.command("project")
def init_project(
    project_name: str = typer.Argument(..., help="Name of the new TransmuteDB project"),
):
    """Initialize a new TransmuteDB data project."""
    project_path = Path.cwd() / project_name
    if project_path.exists():
        typer.echo(f"❌ Project folder '{project_name}' already exists.")
        raise typer.Exit(1)

    # Folder structure
    dirs = [
        project_path / "src" / "flows",
        project_path / "src" / "tasks",
        project_path / "src" / "sql",
        project_path / "src" / "config",
        project_path / "src" / "tests",
    ]
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

    # Sample config file
    (project_path / "transmute.config.yaml").write_text(
        "project_name: {}\ndatabase_url: postgres://user:pass@localhost:5432/db\n".format(
            project_name
        )
    )

    # Optional: hello world flow
    hello_flow = """from prefect import flow

@flow
def hello_world_flow():
    print("👋 Hello from TransmuteDB!")

if __name__ == "__main__":
    hello_world_flow()
"""
    (project_path / "src" / "flows" / "hello_world_flow.py").write_text(hello_flow)

    # .gitignore
    (project_path / ".gitignore").write_text(".env\n.venv\n__pycache__\n*.pyc\n")

    typer.echo(f"✅ Created new project in: {project_path}")
