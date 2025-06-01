import typer
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

app = typer.Typer()

TEMPLATE_DIR = Path(__file__).parent / "templates"
OUTPUT_DIR = Path(__file__).parent.parent / "src" / "flows"
env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


@app.command()
def scaffold(object_name: str):
    """Scaffold a flow file from templates."""
    print(f"Scaffolding flow for object: {object_name}")

    # Load Template
    template_dir = Path(__file__).parent / "templates"
    output_dir = Path(__file__).parent.parent / "src" / "flows"
    output_dir.mkdir(parents=True, exist_ok=True)

    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("flow.jinja")
    rendered = template.render(object_name=object_name.lower())

    output_path = output_dir / f"{object_name.lower()}_flow.py"
    with open(output_path, "w") as f:
        f.write(rendered)

    print(f"Created: {output_path}")


@app.command()
def say_hello(name: str):
    print(f"hello, {name.capitalize()}")


if __name__ == "__main__":
    app()
