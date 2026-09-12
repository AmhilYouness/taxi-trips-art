"""Ad-hoc DuckDB query runner against the Chicago Taxi lakehouse.

DuckDB attaches the Lakekeeper Iceberg REST catalog as `lake` and reads the
Parquet data straight from MinIO (see duck.py). Tables are addressable as
`lake.<layer>.<table>`, e.g. lake.silver.taxi_trips, lake.gold.trips_daily.
The community-area dimension is exposed as the view `areas`.

Usage (from the serving/ directory):

  # one-shot query
  uv run python query.py "SELECT count(*) FROM lake.silver.taxi_trips"

  # run a .sql file
  uv run python query.py -f my_analysis.sql

  # interactive shell (Ctrl-D / Ctrl-Z+Enter to exit)
  uv run python query.py

Examples to try:
  SELECT payment_type, count(*) FROM lake.silver.taxi_trips GROUP BY 1 ORDER BY 2 DESC;
  SELECT a.area_name, count(*) trips
    FROM lake.silver.taxi_trips s JOIN areas a ON s.pickup_community_area=a.area_no
    GROUP BY 1 ORDER BY trips DESC LIMIT 10;
  SELECT * FROM lake.gold.trips_daily ORDER BY day DESC LIMIT 5;
"""
import sys

from duck import connect

# DuckDB's .show() prints Unicode box-drawing chars; force UTF-8 so it works
# even when stdout is a Windows cp1252 console or a redirected pipe.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass


def _run(con, sql: str) -> None:
    for stmt in [s for s in sql.split(";") if s.strip()]:
        con.sql(stmt).show(max_rows=100)


def main() -> None:
    args = sys.argv[1:]
    con = connect()
    try:
        if not args:
            print(
                "Connected to lakehouse. Query lake.bronze/silver/gold.* and the "
                "`areas` view.\nEnd a statement with ';'. Ctrl-D to exit.\n"
            )
            while True:
                try:
                    sql = input("duckdb> ")
                except EOFError:
                    print()
                    break
                if not sql.strip():
                    continue
                try:
                    _run(con, sql)
                except Exception as err:  # noqa: BLE001
                    print("ERROR:", err)
        elif args[0] == "-f":
            _run(con, open(args[1], encoding="utf-8").read())
        else:
            _run(con, " ".join(args))
    finally:
        con.close()


if __name__ == "__main__":
    main()
