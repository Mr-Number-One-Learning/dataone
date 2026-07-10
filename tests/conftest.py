"""
Shared pytest fixtures.

The `spark` fixture spins up a real local-mode SparkSession for tests marked
@pytest.mark.spark. This needs Java available wherever `pytest` runs — the
JDK itself isn't a pip package (`pip install pyspark` is not enough); e.g.
on Debian/Ubuntu: `apt install openjdk-17-jdk-headless`.

Run just the fast, non-Spark tests with `pytest -m "not spark"` (this is what
`make test` does — see Makefile), or everything including the slower
Spark-backed ones with `make test-spark`.
"""
import pytest


@pytest.fixture(scope="session")
def spark():
    """Provides a session-scoped PySpark SparkSession.

    The `spark` fixture spins up a real local-mode SparkSession for tests marked
    @pytest.mark.spark. This needs Java available wherever `pytest` runs.

    Returns:
        SparkSession: The initialized SparkSession.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.appName("dataone-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def iceberg_spark(tmp_path_factory):
    """Provides a session-scoped SparkSession with an Iceberg catalog.

    A local SparkSession with a REAL (if throwaway) Iceberg catalog wired up
    — needed for anything doing MERGE INTO / CREATE TABLE ... USING iceberg,
    which the plain `spark` fixture above can't do (no Iceberg jar, no
    catalog registered). Pulls the Iceberg runtime jar from Maven on first
    use via spark.jars.packages — needs network the first time (Ivy caches
    it after that). Uses a local Hadoop-catalog pointed at a pytest tmp_path
    instead of the real Postgres JDBC catalog — same SQL surface, much
    faster to set up for a unit test than standing up Postgres.

    Args:
        tmp_path_factory: The pytest tmp_path_factory fixture for temporary directories.

    Returns:
        SparkSession: The initialized SparkSession configured with Iceberg.
    """
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("iceberg_warehouse")
    session = (
        SparkSession.builder.appName("dataone-iceberg-tests")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.1")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.dataone_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.dataone_catalog.type", "hadoop")
        .config("spark.sql.catalog.dataone_catalog.warehouse", warehouse.as_uri())
        .getOrCreate()
    )
    yield session
    session.stop()
