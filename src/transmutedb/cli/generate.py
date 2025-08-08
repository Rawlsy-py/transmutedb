import typer, yaml, os
from jinja2 import Environment, PackageLoader, select_autoescape
from rich import print as rprint

env = Environment(
    loader=PackageLoader("transmutedb", "templates"), autoescape=select_autoescape()
)

app = typer.Typer()


@app.command("all")
def generate_all(project_dir: str = "."):
    cfg_path = os.path.join(project_dir, "pipelines", "aviation", "pipeline.yaml")
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)

    template = env.get_template("polars/bronze_to_silver.py.j2")

    for m in cfg["models"]:
        out_path = os.path.join(
            project_dir,
            "pipelines",
            "aviation",
            "models",
            m["layer"],
            f"{m['name']}.py",
        )
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        # demo context—hard-coded rows + type map for now
        code = template.render(
            model=m,
            target=cfg["target"],
            rows=[{"aircraft_id": 1, "model": "A320", "seats": 180}],
            type_map={"aircraft_id": "i64", "model": "str", "seats": "i64"},
        )
        with open(out_path, "w") as f:
            f.write(code)
        rprint(f":page_facing_up: rendered {out_path}")
