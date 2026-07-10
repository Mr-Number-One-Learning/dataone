"""
Spark + Iceberg catalog wiring against the local Postgres JDBC catalog — no
running Hadoop/Hive cluster.

Design note, corrected from the original stub: our batch/streaming jobs are
PySpark jobs using the Iceberg-Spark integration (the iceberg-spark-runtime
jar baked into the custom Spark image — see infra/docker/spark/Dockerfile),
NOT pyiceberg's standalone Python catalog API. "Wiring the catalog" here
means producing the spark.sql.catalog.* SparkConf properties a SparkSession
needs, plus generating the DDL strings the jobs execute via spark.sql(...) —
never a separate pyiceberg Catalog object. Property names below were checked
against current Apache Iceberg docs, not recalled from memory alone.
"""
from __future__ import annotations

from dataone.config import iceberg, postgres

LAYERS = ("bronze", "silver", "gold", "quarantine")

# Parquet writer tuning for low-RAM local containers: small 16 MiB row groups
# keep the FanoutDataWriter's per-partition buffers bounded, and a 1 MiB
# dictionary page is plenty for our low-cardinality string columns. Named here
# so the values are documented once instead of appearing as bare literals in
# generated DDL.
PARQUET_ROW_GROUP_BYTES = 16 * 1024 * 1024
PARQUET_DICT_BYTES = 1024 * 1024


def spark_catalog_conf() -> dict[str, str]:
    """Generates Spark configuration for the Iceberg catalog.

    spark.sql.catalog.* properties to merge into SparkSession.builder.config(...)
    so Spark recognizes our Iceberg catalog, backed by the Postgres JDBC
    catalog implementation. postgres.host resolves to the Docker service name
    here (these jobs run inside a container, so config.py's host
    auto-detection leaves it as "postgres", not "localhost" — see config.py).

    Returns:
        dict[str, str]: A dictionary of Spark configuration key-value pairs.
    """
    name = iceberg.catalog_name
    jdbc_uri = f"jdbc:postgresql://{postgres.host}:{postgres.port}/{postgres.db}"
    return {
        f"spark.sql.catalog.{name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{name}.catalog-impl": "org.apache.iceberg.jdbc.JdbcCatalog",
        f"spark.sql.catalog.{name}.uri": jdbc_uri,
        f"spark.sql.catalog.{name}.jdbc.user": postgres.user,
        f"spark.sql.catalog.{name}.jdbc.password": postgres.password,
        f"spark.sql.catalog.{name}.warehouse": f"file://{iceberg.warehouse_path}",
        # Required once per Spark session to enable Iceberg's extra SQL
        # commands (MERGE INTO, CALL ... stored procedures, etc).
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    }


def table_identifier(layer: str, table_name: str) -> str:
    """Generates a fully-qualified Spark SQL table identifier.

    Fully-qualified Spark SQL identifier: table_identifier("bronze", "orders")
    -> "dataone_catalog.bronze.orders". Needed because Spark's *default*
    catalog (spark_catalog) is different from ours — queries against our
    tables always need the catalog name spelled out.

    Args:
        layer (str): The medallion layer name (e.g., "bronze", "silver", "gold").
        table_name (str): The table name.

    Returns:
        str: The fully-qualified table identifier.
    
    Raises:
        ValueError: If the provided layer is not a valid predefined layer.
    """
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")
    return f"{iceberg.catalog_name}.{layer}.{table_name}"


def create_namespace_sql(layer: str) -> str:
    """Generates SQL to create a namespace (database) in the Iceberg catalog.

    Args:
        layer (str): The medallion layer name.

    Returns:
        str: The CREATE NAMESPACE IF NOT EXISTS SQL statement.

    Raises:
        ValueError: If the provided layer is not a valid predefined layer.
    """
    if layer not in LAYERS:
        raise ValueError(f"layer must be one of {LAYERS}, got {layer!r}")
    return f"CREATE NAMESPACE IF NOT EXISTS {iceberg.catalog_name}.{layer}"


def bootstrap_namespaces_sql() -> list[str]:
    """Generates SQL statements to bootstrap all required namespaces.

    One CREATE NAMESPACE statement per layer — run once at job startup,
    idempotent thanks to IF NOT EXISTS.

    Returns:
        list[str]: A list of SQL statements to create all namespaces.
    """
    return [create_namespace_sql(layer) for layer in LAYERS]


def create_table_sql(
    layer: str,
    table_name: str,
    columns: list[tuple[str, str]],
    partition_by: list[str] | None = None,
) -> str:
    """Builds a CREATE TABLE IF NOT EXISTS ... USING iceberg DDL string.

    `columns` is [(name, spark_sql_type), ...], e.g. [("order_id", "BIGINT")].
    `partition_by` entries are raw Iceberg partition-transform expressions —
    e.g. ["days(order_date)", "bucket(16, customer_id)"] — passed through
    verbatim, not validated here, since Iceberg's transform syntax (days,
    months, years, bucket(n, col), truncate(n, col), identity) is richer than
    worth re-implementing a validator for in Python.

    Args:
        layer (str): The medallion layer name.
        table_name (str): The target table name.
        columns (list[tuple[str, str]]): A list of tuples containing column names and their Spark SQL types.
        partition_by (list[str] | None, optional): A list of Iceberg partition expressions. Defaults to None.

    Returns:
        str: The generated CREATE TABLE SQL statement.

    Raises:
        ValueError: If the columns list is empty.
    """
    if not columns:
        raise ValueError("columns must be non-empty")
    ident = table_identifier(layer, table_name)
    cols_sql = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
    ddl = f"CREATE TABLE IF NOT EXISTS {ident} ({cols_sql}) USING iceberg"
    if partition_by:
        ddl += f" PARTITIONED BY ({', '.join(partition_by)})"

    # Minimize memory buffers for FanoutDataWriter in low-RAM local environments
    ddl += " TBLPROPERTIES ("
    ddl += "'write.distribution-mode'='hash',"
    ddl += "'write.parquet.compression-codec'='zstd',"
    ddl += f"'write.parquet.row-group-size-bytes'='{PARQUET_ROW_GROUP_BYTES}',"
    ddl += f"'write.parquet.dict-size-bytes'='{PARQUET_DICT_BYTES}',"
    ddl += "'write.spark.fanout.enabled'='false'"
    ddl += ")"

    return ddl


def make_surrogate_key(col_name: str, source_system: str = "postgres"):
    """Creates a surrogate key from a natural key and a source system identifier.

    SHA-256 hash of source_system + natural key, returned as a pyspark Column.
    Produces a consistent, portable surrogate key that survives source re-keys
    and is compatible with data from multiple source systems.

    pyspark is imported lazily so this module stays importable in
    Spark-free environments (the offline test suite imports it for the DDL
    string builders).

    Args:
        col_name (str): The name of the natural key column.
        source_system (str, optional): The name of the source system. Defaults to "postgres".

    Returns:
        Column: A pyspark Column object containing the generated surrogate key.
    """
    from pyspark.sql import functions as F

    return F.sha2(
        F.concat_ws("|", F.lit(source_system), F.col(col_name).cast("string")),
        256,
    )
