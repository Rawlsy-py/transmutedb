import typer

from transmute.scaffold import scaffold_app
from transmute.init import init_app

app = typer.Typer()
app.add_typer(scaffold_app, name="scaffold")
app.add_typer(init_app, name="init")

if __name__ == "__main__":
    app()
