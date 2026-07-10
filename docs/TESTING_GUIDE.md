# Quality and Validation Guide

This directory holds the testing suites and validation rules that enforce DataOne's reliability, preventing bad data from contaminating downstream analytics.

## 🧪 Testing Philosophy

Tests are categorized based on their dependency footprint. We avoid relying strictly on heavy JVM cluster startups where pure Python mocks are sufficient, while still ensuring integration tests have access to real Spark DataFrames and Iceberg catalogs when evaluating SQL APIs.

### Test Markers
- **(No marker)**: Pure Python tests. These run instantly and mock out Spark interactions (e.g., CDC logic, orchestration scheduling logic).
- **`@pytest.mark.spark`**: Requires a local `SparkSession` initialized in `conftest.py`. Used for verifying DataFrame transformations and Quality Gate behaviors without writing to disk.
- **`@pytest.mark.iceberg`**: Requires a full `SparkSession` armed with the Iceberg SQL Extensions and PostgreSQL JDBC catalog. Downloads Maven JARs on first run. Validates Iceberg-specific DDL syntax, partition logic, and SCD Type 2 `MERGE INTO` operations.

---

## 🏃 Running Tests

Execute the testing suites using the `make` shortcuts defined in the project root:

**Run all standard and Spark tests (Fastest)**:
```bash
make test
```
*(Runs pure python tests, skipping Iceberg by default if unconfigured)*

**Run specifically Spark DataFrame tests**:
```bash
make test-spark
```

**Run Iceberg Integration tests**:
```bash
make test-iceberg
```
> **Note**: `make test-iceberg` requires Java installed locally and network access for first-time Maven JAR dependency resolution. It ensures structural fidelity against the real PySpark SQL API.

All three should pass. If `test-spark`/`test-iceberg` fail with a Java-not-found error,
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

*Example Validation from `bronze_to_silver.py`:*
```python
quality_result = run_quality_gate(
    fact_order_items_df,
    required_columns=["order_id", "customer_id", "product_id"],
    column_bounds={"unit_price": (0, None), "quantity": (1, None)},
)
# Write quality_result.passed_df -> gold.fact_order_items
# Write quality_result.quarantined_df -> quarantine.fact_order_items
```
