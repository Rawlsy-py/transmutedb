from pathlib import Path
import typer
from jinja2 import Environment, FileSystemLoader

init_app = typer.Typer()

TEMPLATES_DIR = Path(__file__).parent / "templates"


@init_app.command("project")
def init_project(
    name: str = typer.Argument(..., help="Project name"),
    db: str = typer.Option("postgres", help="Default database backend"),
):
    """Initialize a new TransmuteDB project."""
    typer.echo(f"Initializing project: {name}")

    env = Environment(loader=FileSystemLoader(TEMPLATES_DIR))

    # Create project structure
    for dir_ in ["src/flows", "src/tasks", "src/config", "src/sql/bronze", "src/tests"]:
        Path(dir_).mkdir(parents=True, exist_ok=True)

    # Dockerfile
    dockerfile = env.get_template("Dockerfile.jinja").render(project_name=name)
    Path("Dockerfile").write_text(dockerfile)
    typer.echo("✅ Created: Dockerfile")

    # .env
    env_file = env.get_template("env.jinja").render(db=db)
    Path(".env").write_text(env_file)
    typer.echo("✅ Created: .env")

    # .gitignore
    gitignore = env.get_template("gitignore.jinja").render()
    Path(".gitignore").write_text(gitignore)
    typer.echo("✅ Created: .gitignore")

    # README.md
    readme = env.get_template("README.jinja").render(project_name=name)
    Path("README.md").write_text(readme)
    typer.echo("✅ Created: README.md")

    # pre-commit config
    precommit = env.get_template("pre-commit.jinja").render()
    Path(".pre-commit-config.yaml").write_text(precommit)
    typer.echo("✅ Created: .pre-commit-config.yaml")

    typer.echo("🎉 Project initialized!")
