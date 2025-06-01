
# TransmuteDB

⚗️ **TransmuteDB** is a Python-first data platform framework for fast, composable, CLI-driven data pipelines built on [Polars](https://www.pola.rs/), [Prefect](https://docs.prefect.io/), and Jinja2 templates.

It provides a Laravel-style development workflow for data engineers — scaffold flows and transformations with a single CLI command, run them with confidence, and scale from POC to production.

---

## 🚀 Features

- ⚙️ **CLI-first** design with [Typer](https://typer.tiangolo.com/)
- ⚡ **Polars** for fast, type-safe data transformation
- 🧬 **Prefect OSS** orchestration for modern workflows
- 🧱 **Layered architecture**: bronze → silver → gold
- 📦 Scaffold pipelines in seconds with Jinja2 templates
- 🧪 Extensible test and validation model (Pandera-ready)
- 🗃️ Supports PostgreSQL (initial MVP), future support for DuckDB, Azure SQL, etc.

---

## 🛠️ Project Structure

```
transmutedb/
├── cli/                  # Typer CLI commands and Jinja templates
│   ├── main.py
│   └── templates/
│       └── flow.jinja
├── src/
│   ├── flows/            # Generated Prefect flows
│   ├── tasks/            # Polars transformations
│   ├── sql/bronze/       # Raw ingest SQL
│   ├── config/           # Object metadata registry
│   └── tests/            # Pytest tests for pipelines
├── requirements.txt
└── README.md
```

---

## 🔧 Quickstart

### 1. Clone the repo and set up environment

```bash
git clone https://github.com/YOUR_USER/transmutedb.git
cd transmutedb
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Scaffold your first data pipeline

```bash
python cli/main.py scaffold chess
```

Generates:

- `src/flows/chess_flow.py`
- (More templates coming soon)

### 3. Run the flow

```bash
python src/flows/chess_flow.py
```

---

## 🧪 Testing (coming soon)

```bash
pytest
```

---

## 📦 Planned Features

- [ ] CLI scaffolding for tasks, tests, config, SQL
- [ ] Support for Salesforce, REST API, and file ingestion
- [ ] Backfill logic and snapshot metadata
- [ ] PostgreSQL + DuckDB + Azure SQL support
- [ ] Built-in validation via Pandera
- [ ] Data quality logging + dashboards
- [ ] Deployment via Docker and Prefect agent

---

## 🧠 Design Principles

- ✅ Convention over configuration
- ✅ Reproducible from the CLI
- ✅ Type-safe transformations
- ✅ Observable by default
- ✅ Developer-first DX

---

## 💬 Contributing

TransmuteDB is in early MVP. Contributions, feedback, and ideas are very welcome!

To contribute:
- Fork the repo
- Branch off `main`
- Use conventional commits
- PRs with tests/docs preferred

---
