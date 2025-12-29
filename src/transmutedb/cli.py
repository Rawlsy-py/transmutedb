# src/transmutedb/cli.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

# --- internal imports (implement these in your MVP) --------------------------
# Scaffold (wizards or flag-based) – keep them small & idempotent
from transmutedb.scaffold.generate import (
    init_project,
    make_entity_wizard,
    make_activity_wizard,
)

# Config loader & validators
from transmutedb.config.loader import (
    load_pipeline_config,
    resolve_overrides,
    print_config,
)
from transmutedb.config.models import PipelineConfig  # pydantic models

# Pipeline runner
from transmutedb.flow.runner import run_pipeline  # executes activities in order

# DQ & logs helpers (thin wrappers over DuckDB)
from transmutedb.flow.dq import lint_dq  # validates DQ sections only (syntax/refs)
from transmutedb.ctl.schema import ensure_ctl_tables
from transmutedb.engine.duckdb import connect as duck_connect, fetch_df

# ---------------------------------------------------------------------------

app = typer.Typer(no_args_is_help=True)
make_app = typer.Typer(help="scaffold entities/activities/pipelines")
config_app = typer.Typer(help="inspect and validate configuration")
dq_app = typer.Typer(help="data quality helpers")
logs_app = typer.Typer(help="view run logs")
schedule_app = typer.Typer(help="lightweight scheduling helpers")

app.add_typer(make_app, name="make")
app.add_typer(config_app, name="config")
app.add_typer(dq_app, name="dq")
app.add_typer(logs_app, name="logs")
app.add_typer(schedule_app, name="schedule")


# --------------------------- utils -----------------------------------------
def _parse_kv(pairs: List[str]) -> dict:
    """Parse CLI --set key=val pairs into a dict with simple casting."""
    out: dict = {}
    for item in pairs:
        if "=" not in item:
            raise typer.BadParameter(
                f"Invalid --set value '{item}', expected key=value."
            )
        k, v = item.split("=", 1)
        v_strip = v.strip()
        if v_strip.lower() in ("true", "false"):
            out[k] = v_strip.lower() == "true"
        else:
            try:
                out[k] = int(v_strip)
            except ValueError:
                try:
                    out[k] = float(v_strip)
                except ValueError:
                    out[k] = v_strip
    return out


# --------------------------- top-level cmds --------------------------------
@app.command(help="Create a new TransmuteDB project scaffold in PATH.")
def init(
    path: Path = typer.Argument(
        Path("."), exists=False, file_okay=False, dir_okay=True
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite existing files if present."
    ),
):
    init_project(path, force=force)
    typer.echo(f"✅ project initialized at {path.resolve()}")


# --------------------------- MAKE ------------------------------------------
@make_app.command("entity", help="Interactively add an entity to a pipeline's TOML.")
def make_entity(
    pipeline: str = typer.Argument(
        ..., help="Pipeline name (folder under pipelines/)."
    ),
    defaults: bool = typer.Option(
        False, "--defaults", help="Accept sensible defaults without prompts."
    ),
):
    make_entity_wizard(pipeline, use_defaults=defaults)
    typer.echo(f"✅ entity added to pipeline '{pipeline}'")


@make_app.command(
    "activity", help="Interactively add an activity to pipeline flow (stg|dim|fact)."
)
def make_activity(
    pipeline: str = typer.Argument(...),
    # You can add --kind/--scope flags later; wizard first for DX.
):
    make_activity_wizard(pipeline)
    typer.echo(f"✅ activity added to pipeline '{pipeline}'")


# --------------------------- CONFIG ----------------------------------------
@config_app.command("show", help="Print resolved configuration for a pipeline.")
def config_show(
    pipeline: str = typer.Argument(...),
    project_dir: Path = typer.Option(Path("."), "--project-dir", help="Project root."),
):
    cfg: PipelineConfig = load_pipeline_config(
        project_dir / "pipelines" / pipeline / "pipeline.toml"
    )
    print_config(cfg)  # pretty print; implement tiny printer in loader
    # (Optional) return non-zero if invalid; keep show read-only.


@config_app.command("lint", help="Validate pipeline TOML (structure/types).")
def config_lint(
    pipeline: str = typer.Argument(...),
    project_dir: Path = typer.Option(Path("."), "--project-dir"),
):
    try:
        _ = load_pipeline_config(project_dir / "pipelines" / pipeline / "pipeline.toml")
        typer.echo("✅ config is valid")
    except Exception as e:  # catch Pydantic/TOML errors and show friendly message
        typer.echo(f"❌ config validation failed:\n{e}")
        raise typer.Exit(code=1)


# --------------------------- RUN -------------------------------------------
@app.command(help="Run pipeline steps in order (stg|dim|fact|all).")
def run(
    pipeline: str = typer.Argument(...),
    step: str = typer.Option("all", "--step", help="all|stg|dim|fact"),
    env: Optional[str] = typer.Option(
        None, "--env", help="Environment overlay (e.g. dev, prod)"
    ),
    entity: Optional[str] = typer.Option(
        None, "--entity", help="Limit to one entity for stg"
    ),
    set: List[str] = typer.Option([], "--set", help="Override parameters: key=value"),
    project_dir: Path = typer.Option(Path("."), "--project-dir"),
    warehouse_uri: Optional[str] = typer.Option(
        None, "--warehouse", help="duckdb://file:dw.duckdb"
    ),
):
    cfg = load_pipeline_config(project_dir / "pipelines" / pipeline / "pipeline.toml")
    overrides = _parse_kv(set)
    cfg = resolve_overrides(
        cfg, overrides=overrides, env=env
    )  # no-op for MVP if you like

    # open duckdb, ensure ctl tables exist (logs/dq)
    uri = warehouse_uri or "duckdb://file:dw.duckdb"
    con = duck_connect(uri)
    ensure_ctl_tables(con)

    # execute flow
    result = run_pipeline(con, cfg, step=step, only_entity=entity)
    typer.echo(f"✅ run complete: {result.summary}")


# --------------------------- DQ --------------------------------------------
@dq_app.command(
    "lint", help="Validate DQ rule references/types without running the pipeline."
)
def dq_lint_cmd(
    pipeline: str = typer.Argument(...),
    project_dir: Path = typer.Option(Path("."), "--project-dir"),
):
    cfg = load_pipeline_config(project_dir / "pipelines" / pipeline / "pipeline.toml")
    errors = lint_dq(cfg)
    if errors:
        typer.echo("❌ dq lint failed:")
        for err in errors:
            typer.echo(f" - {err}")
        raise typer.Exit(code=1)
    typer.echo("✅ dq rules look good")


# --------------------------- LOGS ------------------------------------------
@logs_app.command("tail", help="Tail recent run logs from run_log table (DuckDB).")
def logs_tail(
    pipeline: Optional[str] = typer.Option(None, "--pipeline"),
    limit: int = typer.Option(50, "--limit"),
    warehouse_uri: str = typer.Option("duckdb://file:dw.duckdb", "--warehouse"),
):
    con = duck_connect(warehouse_uri)
    ensure_ctl_tables(con)
    where = "WHERE 1=1"
    if pipeline:
        where += f" AND pipeline = '{pipeline}'"
    df = fetch_df(
        con,
        f"""
        SELECT started_at, pipeline, step, entity, status, rows_in, rows_out, error_message
        FROM run_log
        {where}
        ORDER BY started_at DESC
        LIMIT {limit}
        """,
    )
    # Simple print (you can pretty-print with tabulate later)
    for row in df.iter_rows(named=True):
        typer.echo(
            f"[{row['started_at']}] {row['pipeline']}.{row['step']} "
            f"{'(' + row['entity'] + ')' if row['entity'] else ''} "
            f"- {row['status']} ri={row['rows_in']} ro={row['rows_out']} "
            f"{'err=' + (row['error_message'] or '') if row['error_message'] else ''}"
        )


# --------------------------- SCHEDULE --------------------------------------
@schedule_app.command(
    "add", help="Add/append a cron-like schedule to schedules.toml in project root."
)
def schedule_add(
    pipeline: str = typer.Argument(...),
    cron: str = typer.Option(..., "--cron", help='e.g. "0 * * * *"'),
    env: str = typer.Option("dev", "--env"),
    step: str = typer.Option("all", "--step"),
    project_dir: Path = typer.Option(Path("."), "--project-dir"),
):
    import tomllib, tomli_w  # tomli_w for writing TOML

    sched_path = project_dir / "schedules.toml"
    data = {}
    if sched_path.exists():
        data = tomllib.loads(sched_path.read_text())
    data.setdefault("schedules", []).append(
        {"pipeline": pipeline, "cron": cron, "env": env, "step": step}
    )
    sched_path.write_text(tomli_w.dumps(data))
    typer.echo(f"✅ schedule added to {sched_path}")


@schedule_app.command("export", help="Export crontab lines for all schedules.")
def schedule_export_cron(
    project_dir: Path = typer.Option(Path("."), "--project-dir"),
    log_path: Path = typer.Option(Path("logs/cron.log"), "--log"),
):
    import tomllib

    sched_path = project_dir / "schedules.toml"
    if not sched_path.exists():
        typer.echo("ℹ️  no schedules.toml found.")
        raise typer.Exit(0)
    data = tomllib.loads(sched_path.read_text())
    typer.echo("# Add the following lines to your crontab:")
    for s in data.get("schedules", []):
        typer.echo(
            f"""{s['cron']} cd {project_dir.resolve()} && transmutedb run {s['pipeline']} --env {s['env']} --step {s['step']} >> {log_path} 2>&1"""
        )


# --------------------------- entrypoint ------------------------------------
def app_main():
    app()


if __name__ == "__main__":
    app_main()
