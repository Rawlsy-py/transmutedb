import typer

from transmute.scaffold import scaffold_app

app = typer.Typer()
app.add_typer(scaffold_app, name="scaffold")

if __name__ == "__main__":
    app()
