# TransmuteDB

⚗️ **TransmuteDB** is an **open-source, parameter-driven data engineering framework** for building **Kimball-style** dimensional models (including **Type 2 SCDs**) in a modern **data lakehouse/warehouse**.

It blends **Laravel-style scaffolding** for developer speed with a **declarative, metadata-driven pipeline engine** that runs on **Python**, **Polars**, or **PySpark** — making it suitable for everything from local dev on DuckDB to production-scale clusters.

---

## 🚀 What It Does

* **CLI-First** — Generate all your data engineering pipelines from the terminal.
* **Parameter-Driven** — All orchestration logic comes from TOML + metadata tables — no hardcoded pipelines.
* **Kimball-Ready** — Build facts, dimensions, and Type 2 SCD tables automatically from configs.
* **Data Quality First** — Built-in null, uniqueness, schema, and type checks with quarantine flows.
* **Flexible Compute** — Runs on Polars or PySpark.
* **Any Warehouse** — Start with DuckDB or PostgreSQL; scale to Snowflake, Databricks, Synapse, or others.

---

## 🛠 Architecture

TransmuteDB projects are **self-contained** and follow this structure:

```
tbd/
```

---

## 📦 Example Features

* **Orchestration Engine**

  * Reads from `pipeline.yaml` and metadata tables.
  * Handles parallel execution by file, entity, or notebook scope.

* **Dimension Builder** (`dim_build`)

  * Automatically applies Type 2 SCD logic based on metadata.
* **Fact Builder** (`fact_build`)

  * Joins to current dimensions and handles surrogate key creation.
* **Data Quality Engine**

  * Supports uniqueness, null, min/max, schema, and data type checks.
  * Optional record-level quarantine with separate storage paths.
* **Dev Mode**

  * Spin up pipelines without touching production configs or metadata.

---

## 🔧 Quickstart (Alpha Mode, No PyPi)

### 1. Install TransmuteDB

```bash
uv pip install -e .
```

### 2. Create a Project

```bash
uv run transmutedb init my_project
cd my_project
```

### 3. Scaffold Pipeline Components

```bash
# Add a new pipeline
uv run transmutedb scaffold pipeline aviation

# Add a dimension model
uv run transmutedb scaffold model silver.customer

# Add data quality rules
uv run transmutedb scaffold dq customer
```

### 4. Run the Orchestrator

```bash
uv run transmutedb run pipelines/aviation/pipeline.yaml
```

---

## 🧪 Testing

```bash
pytest
```

---

## 📍 Roadmap

* [ ] Out-of-the-box Airflow DAG generation.
* [ ] Built-in backfill support for SCD2 facts/dims.
* [ ] Incremental load strategies per source.
* [ ] Additional connectors (Snowflake, Synapse, REST APIs).
* [ ] CLI-driven data quality dashboards.

---

## 🧠 Design Principles

* ✅ **Convention over configuration** — sensible defaults.
* ✅ **Reproducible** — same config runs locally or in prod.
* ✅ **Observable** — rich logs and metadata capture.
* ✅ **Warehouse-agnostic** — SQL templates adapt to your target.
* ✅ **Dev-friendly** — zero to pipeline in minutes.

---

## 💬 Contributing

TransmuteDB is in active development and welcomes contributions.

1. Fork the repo
2. Branch from `main`
3. Use [Conventional Commits](https://www.conventionalcommits.org/)
4. Include tests & docs for new features
5. Open a PR
