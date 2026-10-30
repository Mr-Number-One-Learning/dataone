# Quality and Validation Guide

This directory holds the testing suites and validation rules that enforce DataOne's reliability, preventing bad data from contaminating downstream analytics.

## 🧪 Testing Philosophy

Tests are categorized based on their dependency footprint. We avoid relying strictly on heavy JVM cluster startups where pure Python mocks are sufficient, while still ensuring integration tests have access to real Spark DataFrames and Iceberg catalogs when evaluating SQL APIs.

### Test Markers
- **(No marker)**: Pure Python tests. These run instantly and mock out Spark interactions (e.g., CDC logic, orchestration scheduling logic).
- **`@pytest.mark.spark`**: Requires a local `SparkSession` initialized in `conftest.py`. Used for verifying DataFrame transformations and Quality Gate behaviors without writing to disk.
- **`@pytest.mark.iceberg`**: Requires a full `SparkSession` armed with the Iceberg SQL Extensions and a throwaway local Hadoop catalog in a pytest `tmp_path` (same SQL surface as the production Postgres JDBC catalog, no database needed). Downloads Maven JARs on first run. Validates Iceberg-specific DDL syntax, partition logic, and SCD Type 2 `MERGE INTO` operations.
- **`@pytest.mark.minio`**: Opt-in. Writes and reads an Iceberg table through `S3FileIO` against a real MinIO; skipped unless `MINIO_TEST_ENDPOINT`, `MINIO_ACCESS_KEY` and `MINIO_SECRET_KEY` are set. Run with `make test-minio`.

The default `pytest` run (`make test`) excludes the `spark`, `iceberg` and `minio` markers (`addopts` in `pyproject.toml`). CI runs the default suite on every pull request and the Spark and Iceberg suites nightly.

A few tests worth knowing about:
- `tests/test_metadata_drift.py` regenerates `metadata/` with `scripts/generate_metadata.py` into a temp directory and fails if the committed files differ. Fix by re-running `python scripts/generate_metadata.py`.
- `tests/test_stage_orchestration.py` checks the wiring of the batch stages (dispatch, call order, data passed between steps) with every collaborator mocked, so it needs no Spark.
- `tests/conftest.py` sets throwaway `*_PASSWORD` values, because `dataone.config` has no in-code secret defaults.

---

## 🏃 Running Tests

Execute the testing suites using the `make` shortcuts defined in the project root:

**Run the default offline suite (fastest)**:
```bash
make test
```
*(Runs the pure Python tests; the `spark`, `iceberg` and `minio` markers are always excluded by `addopts` in `pyproject.toml`)*

**Run specifically Spark DataFrame tests**:
```bash
make test-spark
```

**Run Iceberg Integration tests**:
```bash
make test-iceberg
```
> **Note**: `make test-iceberg` requires Java installed locally and network access for first-time Maven JAR dependency resolution. It ensures structural fidelity against the real PySpark SQL API.

The Spark and Iceberg suites should pass. If `test-spark`/`test-iceberg` fail with a Java-not-found error,
install a JDK — this is expected on a machine that's never run PySpark locally before.

### Windows PySpark Test Setup

If you are running the PySpark tests on Windows, you may encounter `HADOOP_HOME` or `java` not recognized errors. 

1. First, download the required Hadoop binaries for Windows into a local `.hadoop` folder:
```powershell
$hadoopBin = ".hadoop\bin"
New-Item -ItemType Directory -Force -Path $hadoopBin
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/winutils.exe" -OutFile "$hadoopBin\winutils.exe"
Invoke-WebRequest -Uri "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/hadoop.dll" -OutFile "$hadoopBin\hadoop.dll"
```

2. Then, run the tests explicitly passing your Java runtime path (for example, the one bundled with PyCharm) and the local `.hadoop` folder:
```powershell
$env:JAVA_HOME="C:\Program Files\JetBrains\PyCharm Community Edition 2023.2.4\jbr"
$env:HADOOP_HOME="$(Get-Location)\.hadoop"
make test-iceberg
```

---

## 🛡 Data Quality Framework

DataOne implements a strict, generic PySpark Data Quality Gate at the boundary between the **Bronze** (raw) and **Silver** (curated) layers.

**Location:** `src/dataone/quality/validators.py`

### Mechanism
The `run_quality_gate` function ensures that bad data is trapped and routed, never silently dropped.
1. **Rule Evaluation**: It accepts a list of `required_columns` (must not be NULL) and a dictionary of `column_bounds` (enforcing min/max ranges).
2. **Partitioning**: Rows violating *any* rule are flagged with an internal `_quarantine_reason` (e.g., `"null_check_failed"` or `"range_check_failed"`).
3. **Routing**: 
   - **Passed Records**: Stripped of internal flags and returned to the pipeline for writing to the Silver layer.
   - **Quarantined Records**: Retain the error flags and are appended permanently to the `quarantine` layer (e.g., `quarantine.fact_order_items`) for data engineering audit and reprocessing.

The rules come from `metadata/quality/<dataset>.json` (loaded by the `MetadataRegistry`) when the caller passes `dataset_name`; explicit `required_columns` / `column_bounds` arguments override them.

*Example Validation from `bronze_to_silver.py`:*
```python
# metadata/quality/gold.fact_order_items.json:
#   required: order_id, customer_id, product_id; bounds: unit_price >= 0, quantity >= 1
quality_result = run_quality_gate(fact_order_items_df, dataset_name="gold.fact_order_items")
# Write quality_result.passed_df -> gold.fact_order_items
# Write quality_result.quarantined_df -> quarantine.fact_order_items
```

---

## 📋 Metadata & Contract Validation Testing

The platform enforces schemas and evolutionary controls dynamically at runtime via Data Contracts. 

**Test Suite:** `tests/test_metadata_contracts.py`

### 1. Metadata Registry Verification
Tests verify that the `MetadataRegistry` (configured in `src/dataone/metadata/registry.py`) correctly loads JSON files from the `metadata/` subfolders on startup, maps types correctly, and dynamically translates them into PySpark `StructType` definitions for table bootstrapping.

### 2. Schema Evolution & Violation Checks
Tests verify schema validation behavior under different evolution policies:
- **`none` Policy**: Extra columns injected during pipeline runs raise `DataContractViolation` exceptions immediately, shutting down the pipeline.
- **`backward` Policy**: Additional columns are permitted only if they are nullable (not forcing schema updates for consumers), while dropping fields or changing datatypes triggers contract failures.

### 3. Lineage Tracker Validation
Lineage runs are verified using simulated Python mocks, ensuring that:
- Input/output datasets are correctly added.
- Job metrics (records processed, quarantined counts) map to postgres tracking fields.
- OpenLineage JSON envelopes conform to the specification and trigger event emission.

