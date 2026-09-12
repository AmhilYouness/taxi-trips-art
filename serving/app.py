"""FastAPI BI service for the Chicago Taxi lakehouse.

Every endpoint aggregates on-the-fly from the silver Iceberg table via DuckDB,
applying an optional, shared set of filters (date range, company, payment type,
pickup community area, weekday/weekend). This makes the dashboard fully
interactive instead of serving pre-baked gold marts.

Run:  uv run uvicorn app:app --reload --port 8000
"""
import os
from typing import Optional

from fastapi import Depends, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from duck import connect

app = FastAPI(title="Chicago Taxi Lakehouse BI API", version="3.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[os.environ.get("DASHBOARD_ORIGIN", "http://localhost:5173")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SILVER = "lake.silver.taxi_trips"


class Filters:
    """Shared query filters parsed from the query string."""

    def __init__(
        self,
        start: Optional[str] = Query(None, description="Start date (YYYY-MM-DD), inclusive"),
        end: Optional[str] = Query(None, description="End date (YYYY-MM-DD), inclusive"),
        company: Optional[str] = Query(None, description="Exact company name"),
        payment_type: Optional[str] = Query(None, description="Exact payment type"),
        pickup_area: Optional[int] = Query(None, description="Pickup community area code"),
        day_type: Optional[str] = Query(None, description="'weekday' or 'weekend'"),
    ):
        self.start = start
        self.end = end
        self.company = company
        self.payment_type = payment_type
        self.pickup_area = pickup_area
        self.day_type = day_type

    def where(self, extra: Optional[list[str]] = None) -> tuple[str, list]:
        conds: list[str] = list(extra or [])
        args: list = []
        if self.start:
            conds.append("trip_date >= ?")
            args.append(self.start)
        if self.end:
            conds.append("trip_date <= ?")
            args.append(self.end)
        if self.company:
            conds.append("company = ?")
            args.append(self.company)
        if self.payment_type:
            conds.append("payment_type = ?")
            args.append(self.payment_type)
        if self.pickup_area is not None:
            conds.append("pickup_community_area = ?")
            args.append(self.pickup_area)
        if self.day_type == "weekend":
            conds.append("dow_num IN (1, 7)")
        elif self.day_type == "weekday":
            conds.append("dow_num NOT IN (1, 7)")
        clause = ("WHERE " + " AND ".join(conds)) if conds else ""
        return clause, args


def _rows(sql: str, args: list | None = None) -> list[tuple]:
    con = connect()
    try:
        return con.execute(sql, args or []).fetchall()
    finally:
        con.close()


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/api/filters")
def filter_options() -> dict:
    """Distinct values for the dashboard's filter controls."""
    con = connect()
    try:
        dmin, dmax = con.execute(
            f"SELECT min(trip_date), max(trip_date) FROM {SILVER}"
        ).fetchone()
        companies = [
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT company FROM {SILVER} WHERE company IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        payments = [
            r[0]
            for r in con.execute(
                f"SELECT DISTINCT payment_type FROM {SILVER} WHERE payment_type IS NOT NULL ORDER BY 1"
            ).fetchall()
        ]
        areas = [
            {"code": int(r[0]), "name": r[1]}
            for r in con.execute(
                f"""
                SELECT s.pickup_community_area, a.area_name
                FROM {SILVER} s JOIN areas a ON s.pickup_community_area = a.area_no
                WHERE s.pickup_community_area IS NOT NULL
                GROUP BY 1, 2 ORDER BY count(*) DESC
                """
            ).fetchall()
        ]
    finally:
        con.close()
    return {
        "date_min": str(dmin) if dmin else None,
        "date_max": str(dmax) if dmax else None,
        "companies": companies,
        "payment_types": payments,
        "pickup_areas": areas,
    }


@app.get("/api/summary")
def summary(f: Filters = Depends()) -> dict:
    where, args = f.where()
    trips, revenue, days, avg_fare, avg_tip, avg_miles = _rows(
        f"""
        SELECT count(*), COALESCE(sum(trip_total), 0), count(DISTINCT trip_date),
               COALESCE(avg(fare), 0), COALESCE(avg(tip_pct), 0), COALESCE(avg(trip_miles), 0)
        FROM {SILVER} {where}
        """,
        args,
    )[0]
    return {
        "total_trips": int(trips),
        "total_revenue": round(float(revenue), 2),
        "days": int(days),
        "avg_fare": round(float(avg_fare), 2),
        "avg_tip_pct": round(float(avg_tip), 4),
        "avg_miles": round(float(avg_miles), 2),
    }


@app.get("/api/trips-daily")
def trips_daily(f: Filters = Depends()) -> list[dict]:
    where, args = f.where()
    rows = _rows(
        f"""
        SELECT trip_date, count(*), round(sum(trip_total), 2), round(avg(fare), 2),
               round(avg(tip_pct), 4), round(avg(trip_miles), 2), round(avg(duration_min), 2)
        FROM {SILVER} {where}
        GROUP BY trip_date ORDER BY trip_date
        """,
        args,
    )
    return [
        {
            "day": str(r[0]),
            "trips": int(r[1]),
            "revenue": float(r[2]),
            "avg_fare": float(r[3]) if r[3] is not None else None,
            "avg_tip_pct": float(r[4]) if r[4] is not None else None,
            "avg_miles": float(r[5]) if r[5] is not None else None,
            "avg_duration_min": float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]


@app.get("/api/trips-by-hour")
def trips_by_hour(f: Filters = Depends()) -> list[dict]:
    where, args = f.where()
    rows = _rows(
        f"""
        SELECT dow_num, day_name, trip_hour, count(*), round(avg(fare), 2)
        FROM {SILVER} {where}
        GROUP BY dow_num, day_name, trip_hour ORDER BY dow_num, trip_hour
        """,
        args,
    )
    return [
        {
            "dow_num": int(r[0]),
            "day_name": r[1],
            "hour": int(r[2]),
            "trips": int(r[3]),
            "avg_fare": float(r[4]) if r[4] is not None else None,
        }
        for r in rows
    ]


@app.get("/api/trips-by-company")
def trips_by_company(f: Filters = Depends()) -> list[dict]:
    where, args = f.where()
    rows = _rows(
        f"""
        SELECT COALESCE(company, 'Unknown'), count(*), round(sum(trip_total), 2), round(avg(fare), 2)
        FROM {SILVER} {where}
        GROUP BY 1 ORDER BY count(*) DESC LIMIT 15
        """,
        args,
    )
    return [
        {"company": r[0], "trips": int(r[1]), "revenue": float(r[2]),
         "avg_fare": float(r[3]) if r[3] is not None else None}
        for r in rows
    ]


@app.get("/api/payment-mix")
def payment_mix(f: Filters = Depends()) -> list[dict]:
    where, args = f.where()
    rows = _rows(
        f"""
        SELECT COALESCE(payment_type, 'Unknown'), count(*), round(sum(trip_total), 2),
               round(count(*) * 1.0 / SUM(count(*)) OVER (), 4)
        FROM {SILVER} {where}
        GROUP BY 1 ORDER BY count(*) DESC
        """,
        args,
    )
    return [
        {"payment_type": r[0], "trips": int(r[1]), "revenue": float(r[2]),
         "pct_of_trips": float(r[3])}
        for r in rows
    ]


@app.get("/api/top-routes")
def top_routes(f: Filters = Depends()) -> list[dict]:
    where, args = f.where(
        extra=["pickup_community_area IS NOT NULL", "dropoff_community_area IS NOT NULL"]
    )
    rows = _rows(
        f"""
        SELECT pu.area_name, do.area_name, s.pickup_community_area, s.dropoff_community_area,
               count(*), round(avg(s.fare), 2), round(avg(s.trip_miles), 2)
        FROM {SILVER} s
        LEFT JOIN areas pu ON s.pickup_community_area = pu.area_no
        LEFT JOIN areas do ON s.dropoff_community_area = do.area_no
        {where}
        GROUP BY 1, 2, 3, 4 ORDER BY count(*) DESC LIMIT 20
        """,
        args,
    )
    return [
        {
            "pickup_area": r[0] or f"Area {r[2]}",
            "dropoff_area": r[1] or f"Area {r[3]}",
            "trips": int(r[4]),
            "avg_fare": float(r[5]) if r[5] is not None else None,
            "avg_miles": float(r[6]) if r[6] is not None else None,
        }
        for r in rows
    ]


@app.get("/api/pickup-areas")
def pickup_areas(f: Filters = Depends()) -> list[dict]:
    where, args = f.where(extra=["s.pickup_community_area IS NOT NULL"])
    rows = _rows(
        f"""
        SELECT s.pickup_community_area, a.area_name, a.is_airport,
               count(*), round(sum(s.trip_total), 2), round(avg(s.fare), 2)
        FROM {SILVER} s
        LEFT JOIN areas a ON s.pickup_community_area = a.area_no
        {where}
        GROUP BY 1, 2, 3 ORDER BY count(*) DESC LIMIT 20
        """,
        args,
    )
    return [
        {
            "pickup_community_area": int(r[0]),
            "area_name": r[1] or f"Area {r[0]}",
            "is_airport": bool(r[2]),
            "trips": int(r[3]),
            "revenue": float(r[4]),
            "avg_fare": float(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]
