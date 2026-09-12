"""Build a SparkSession wired to the Iceberg REST catalog (Lakekeeper) + S3 (MinIO).

All connection details come from environment variables so the same image can
point at MinIO locally or AWS S3 in the cloud with no code changes.

Env vars (injected by the Dagster asset / container):
  ICEBERG_CATALOG_URI   REST catalog endpoint (e.g. http://lakekeeper:8181/catalog)
  ICEBERG_WAREHOUSE     Warehouse name registered in the catalog (e.g. lakehouse)
  ICEBERG_TOKEN         Bearer token for the catalog (unsecured dev: "dummy")
  S3_ENDPOINT           S3 endpoint (blank for real AWS S3)
  S3_KEY / S3_SECRET    Credentials
  S3_REGION             Region (default us-east-1)
  S3_PATH_STYLE         "true" for MinIO, "false" for AWS S3
  SPARK_MASTER          Spark master (default local[*])
"""
import os

from pyspark.sql import SparkSession

CATALOG = "lakehouse"


def _bool(name: str, default: str = "true") -> str:
    return os.environ.get(name, default).strip().lower()


def build_spark(app_name: str) -> SparkSession:
    prefix = f"spark.sql.catalog.{CATALOG}"
    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # Iceberg SQL extensions (MERGE INTO, stored procedures, etc.)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # REST catalog backed by Lakekeeper
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", os.environ["ICEBERG_CATALOG_URI"])
        .config(f"{prefix}.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "lakehouse"))
        .config(f"{prefix}.token", os.environ.get("ICEBERG_TOKEN", "dummy"))
        # Data files read/written directly to object storage via S3FileIO
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.access-key-id", os.environ.get("S3_KEY", "minioadmin"))
        .config(f"{prefix}.s3.secret-access-key", os.environ.get("S3_SECRET", "minioadmin"))
        .config(f"{prefix}.s3.region", os.environ.get("S3_REGION", "us-east-1"))
        .config(f"{prefix}.s3.path-style-access", _bool("S3_PATH_STYLE", "true"))
        .config("spark.sql.defaultCatalog", CATALOG)
        # Dynamic partition overwrite so a single month reload replaces only
        # that month's partition (used by bronze/silver writeTo overwritePartitions).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    # MinIO (or any custom S3) needs an explicit endpoint; real AWS S3 does not.
    s3_endpoint = os.environ.get("S3_ENDPOINT", "").strip()
    if s3_endpoint:
        builder = builder.config(f"{prefix}.s3.endpoint", s3_endpoint)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
