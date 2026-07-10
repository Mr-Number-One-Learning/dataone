# Contributing to DataOne

Welcome! This guide outlines how to set up your local development environment, enforce coding standards, and submit pull requests for the DataOne lakehouse architecture.

## 💻 Local Development Setup

To ensure you don't clash with system Python dependencies or PySpark mismatch issues, use an isolated virtual environment.

1. **Initialize the Virtual Environment**:
   ```bash
   python -m venv .venv
   ```

2. **Activate the Environment**:
   - **Mac/Linux**: `source .venv/bin/activate`
   - **Windows**: `.venv\Scripts\activate`

3. **Install Dependencies**:
   Install the standard project dependencies and the development dependencies (which include formatting and testing tools):
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt
   ```

4. **Install the Package (recommended)**:
   Install the project itself in editable mode so `python -m dataone....` and the tests resolve the `dataone` package without any `PYTHONPATH` juggling:
   ```bash
   pip install -e .
   ```

---

## 🧹 Linting & Formatting Standards

We maintain a strict, easily readable codebase using modern Python static analysis tools.

- **[Black](https://black.readthedocs.io/en/stable/)**: The uncompromising Python code formatter.
- **[Ruff](https://docs.astral.sh/ruff/)**: An extremely fast Python linter, written in Rust.

**Checking your code (Linting):**
Before committing, ensure your code passes the linting checks without modifying the files automatically:
```bash
make lint
```

**Auto-formatting your code:**
If the linter flags style issues, auto-format the entire `src/` and `tests/` directories by running:
```bash
make fmt
```

### Documentation Standards
All new functions, classes, and modules **must** contain [Google-style Docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Furthermore, you must provide explicit type hints (`list`, `dict`, `SparkSession`, `DataFrame`, etc.) on all function signatures.

---

## 🚀 Pull Request Process

1. **Branching**: Branch off from `main` using a descriptive name (e.g., `feature/scd2-logic` or `bugfix/clickhouse-port`).
2. **Writing Tests**: Any new PySpark transformations, Quality Gate rules, or generator features must include accompanying tests in the `tests/` directory.
    - If testing pure Python logic, no marker is needed.
    - If testing a DataFrame transformation, use `@pytest.mark.spark`.
    - If testing Iceberg DDLs or Catalog behaviors, use `@pytest.mark.iceberg`.
3. **Run the Test Suite**:
   ```bash
   make test
   make test-spark
   make coverage   # pytest with a line-by-line coverage report
   ```
4. **Submission**: Ensure `make lint` passes with 0 errors. Open a PR describing your architectural decisions and the impact of the changes.

Thank you for contributing to DataOne!
