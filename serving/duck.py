"""DuckDB connection to the Iceberg lakehouse (Lakekeeper REST catalog + MinIO).

Reads the shared project .env. All values point at the HOST-visible ports
(localhost) since this runs natively on Windows, not inside the docker network.
"""
import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

S3_KEY = os.environ.get("S3_KEY", "minioadmin")
S3_SECRET = os.environ.get("S3_SECRET", "minioadmin")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
S3_HOST = os.environ.get("S3_HOST", "localhost:9000")
S3_USE_SSL = os.environ.get("S3_USE_SSL", "false").strip().lower()
WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "lakehouse")
CATALOG_URI = os.environ.get("ICEBERG_CATALOG_URI_HOST", "http://localhost:8181/catalog")


def connect() -> duckdb.DuckDBPyConnection:
    """Return a fresh DuckDB connection attached to the lakehouse catalog."""
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")

    # S3 secret pointing at MinIO on the host. path-style + no SSL for MinIO.
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{S3_KEY}',
            SECRET '{S3_SECRET}',
            REGION '{S3_REGION}',
            ENDPOINT '{S3_HOST}',
            USE_SSL {S3_USE_SSL},
            URL_STYLE 'path'
        )
        """
    )

    # ACCESS_DELEGATION_MODE 'none' -> use the S3 secret above instead of
    # (empty) vended credentials, avoiding MinIO 403s. AUTHORIZATION_TYPE 'none'
    # because this is an unsecured local Lakekeeper.
    con.execute(
        f"""
        ATTACH '{WAREHOUSE}' AS lake (
            TYPE iceberg,
            ENDPOINT '{CATALOG_URI}',
            AUTHORIZATION_TYPE 'none',
            ACCESS_DELEGATION_MODE 'none'
        )
        """
    )

    # Community-area dimension (names + airport flags) as a view, so queries can
    # join pickup/dropoff community-area codes to human-readable names.
    areas_csv = (ROOT / "data" / "community_areas.csv").as_posix()
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW areas AS
        SELECT community_area::INT AS area_no,
               area_name,
               (is_airport::INT = 1) AS is_airport
        FROM read_csv_auto('{areas_csv}')
        """
    )
    return con
