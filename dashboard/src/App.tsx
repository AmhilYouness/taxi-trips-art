import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  Line,
  LineChart,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  fetchFilters,
  fetchPaymentMix,
  fetchPickupAreas,
  fetchSummary,
  fetchTopRoutes,
  fetchTripsByCompany,
  fetchTripsByHour,
  fetchTripsDaily,
  type CompanyRow,
  type DailyRow,
  type FilterOptions,
  type FilterState,
  type HourRow,
  type PaymentRow,
  type PickupAreaRow,
  type RouteRow,
  type Summary,
} from "./api";

const currency = (n: number) =>
  new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(n);
const num = (n: number) => new Intl.NumberFormat("en-US").format(n);
const pct = (n: number) => `${(n * 100).toFixed(1)}%`;

const PIE_COLORS = ["#38bdf8", "#a78bfa", "#34d399", "#fbbf24", "#f87171", "#f472b6", "#60a5fa"];
const TT_STYLE = { background: "#1e293b", border: "1px solid #334155" };

/* ------------------------------- Filter bar ------------------------------- */

function FilterBar({
  filters,
  setFilters,
  options,
}: {
  filters: FilterState;
  setFilters: (f: FilterState) => void;
  options?: FilterOptions;
}) {
  const set = (patch: Partial<FilterState>) => setFilters({ ...filters, ...patch });
  const active = Object.entries(filters).filter(([, v]) => v !== undefined && v !== "");

  return (
    <div className="filterbar">
      <div className="filter-row">
        <label className="field">
          <span>From</span>
          <input
            type="date"
            min={options?.date_min ?? undefined}
            max={options?.date_max ?? undefined}
            value={filters.start ?? options?.date_min ?? ""}
            onChange={(e) => set({ start: e.target.value || undefined })}
          />
        </label>
        <label className="field">
          <span>To</span>
          <input
            type="date"
            min={options?.date_min ?? undefined}
            max={options?.date_max ?? undefined}
            value={filters.end ?? options?.date_max ?? ""}
            onChange={(e) => set({ end: e.target.value || undefined })}
          />
        </label>
        <label className="field">
          <span>Company</span>
          <select
            value={filters.company ?? ""}
            onChange={(e) => set({ company: e.target.value || undefined })}
          >
            <option value="">All companies</option>
            {options?.companies.map((c) => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Payment</span>
          <select
            value={filters.payment_type ?? ""}
            onChange={(e) => set({ payment_type: e.target.value || undefined })}
          >
            <option value="">All payments</option>
            {options?.payment_types.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Pickup area</span>
          <select
            value={filters.pickup_area ?? ""}
            onChange={(e) =>
              set({ pickup_area: e.target.value ? Number(e.target.value) : undefined })
            }
          >
            <option value="">All areas</option>
            {options?.pickup_areas.map((a) => (
              <option key={a.code} value={a.code}>{a.name}</option>
            ))}
          </select>
        </label>
        <label className="field">
          <span>Day type</span>
          <select
            value={filters.day_type ?? ""}
            onChange={(e) =>
              set({ day_type: (e.target.value || undefined) as FilterState["day_type"] })
            }
          >
            <option value="">All days</option>
            <option value="weekday">Weekdays</option>
            <option value="weekend">Weekends</option>
          </select>
        </label>
        <button className="btn" onClick={() => setFilters({})} disabled={active.length === 0}>
          Reset
        </button>
      </div>
      {active.length > 0 && (
        <div className="chips">
          {active.map(([k, v]) => (
            <span key={k} className="chip" onClick={() => set({ [k]: undefined } as FilterState)}>
              {k.replace("_", " ")}: <b>{String(v)}</b> ✕
            </span>
          ))}
        </div>
      )}
    </div>
  );
}

/* ------------------------------- Primitives ------------------------------- */

function Panel({
  title,
  children,
  loading,
  error,
  empty,
}: {
  title: string;
  children?: React.ReactNode;
  loading?: boolean;
  error?: boolean;
  empty?: boolean;
}) {
  return (
    <div className="panel">
      <h2>{title}</h2>
      {loading && <div className="state">Loading…</div>}
      {error && <div className="state error">Failed to load</div>}
      {!loading && !error && empty && <div className="state">No data for this selection</div>}
      {!loading && !error && !empty && children}
    </div>
  );
}

/* --------------------------------- Charts --------------------------------- */

function SummaryCards({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<Summary>({
    queryKey: ["summary", filters],
    queryFn: () => fetchSummary(filters),
  });
  const cards = [
    { label: "Total Trips", value: data ? num(data.total_trips) : "—" },
    { label: "Total Revenue", value: data ? currency(data.total_revenue) : "—" },
    { label: "Avg Fare", value: data ? currency(data.avg_fare) : "—" },
    { label: "Avg Tip", value: data ? pct(data.avg_tip_pct) : "—" },
    { label: "Avg Miles", value: data ? data.avg_miles.toFixed(2) : "—" },
    { label: "Days", value: data ? num(data.days) : "—" },
  ];
  if (error) return <div className="state error">Failed to load summary</div>;
  return (
    <div className="kpis">
      {cards.map((c) => (
        <div key={c.label} className="card">
          <div className="label">{c.label}</div>
          <div className="value">{isLoading ? "…" : c.value}</div>
        </div>
      ))}
    </div>
  );
}

function TripsDaily({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<DailyRow[]>({
    queryKey: ["trips-daily", filters],
    queryFn: () => fetchTripsDaily(filters),
  });
  return (
    <Panel title="Trips & Revenue by Day" loading={isLoading} error={!!error} empty={data?.length === 0}>
      {data && (
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data} margin={{ top: 8, right: 24, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
            <XAxis dataKey="day" stroke="#94a3b8" fontSize={12} />
            <YAxis yAxisId="left" stroke="#38bdf8" fontSize={12} />
            <YAxis yAxisId="right" orientation="right" stroke="#a78bfa" fontSize={12} />
            <Tooltip contentStyle={TT_STYLE} />
            <Legend />
            <Line yAxisId="left" type="monotone" dataKey="trips" name="Trips" stroke="#38bdf8" strokeWidth={2} dot={false} />
            <Line yAxisId="right" type="monotone" dataKey="revenue" name="Revenue" stroke="#a78bfa" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </Panel>
  );
}

const HOURS = Array.from({ length: 24 }, (_, h) => h);

function DemandHeatmap({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<HourRow[]>({
    queryKey: ["trips-by-hour", filters],
    queryFn: () => fetchTripsByHour(filters),
  });
  const grid = useMemo(() => {
    if (!data) return null;
    const days = Array.from(new Map(data.map((r) => [r.dow_num, r.day_name])).entries()).sort(
      (a, b) => a[0] - b[0]
    );
    const lookup = new Map(data.map((r) => [`${r.dow_num}-${r.hour}`, r.trips]));
    const max = Math.max(...data.map((r) => r.trips), 1);
    return { days, lookup, max };
  }, [data]);
  return (
    <Panel
      title="Demand Heatmap — Trips by Weekday × Hour"
      loading={isLoading}
      error={!!error}
      empty={data?.length === 0}
    >
      {grid && (
        <div className="heatmap">
          <div className="heat-row heat-head">
            <div className="heat-label" />
            {HOURS.map((h) => (
              <div key={h} className="heat-cell heat-hour">{h}</div>
            ))}
          </div>
          {grid.days.map(([dow, name]) => (
            <div key={dow} className="heat-row">
              <div className="heat-label">{name}</div>
              {HOURS.map((h) => {
                const v = grid.lookup.get(`${dow}-${h}`) ?? 0;
                const a = v / grid.max;
                return (
                  <div
                    key={h}
                    className="heat-cell"
                    title={`${name} ${h}:00 — ${num(v)} trips`}
                    style={{ background: `rgba(56, 189, 248, ${0.06 + a * 0.94})` }}
                  />
                );
              })}
            </div>
          ))}
        </div>
      )}
    </Panel>
  );
}

function TripsByCompany({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<CompanyRow[]>({
    queryKey: ["trips-by-company", filters],
    queryFn: () => fetchTripsByCompany(filters),
  });
  return (
    <Panel title="Top Companies by Trips" loading={isLoading} error={!!error} empty={data?.length === 0}>
      {data && (
        <ResponsiveContainer width="100%" height={340}>
          <BarChart data={data} layout="vertical" margin={{ top: 8, right: 24, bottom: 8, left: 8 }}>
            <CartesianGrid stroke="#334155" strokeDasharray="3 3" />
            <XAxis type="number" stroke="#94a3b8" fontSize={12} />
            <YAxis type="category" dataKey="company" stroke="#94a3b8" fontSize={11} width={160} />
            <Tooltip contentStyle={TT_STYLE} formatter={(v: number) => num(v)} />
            <Bar dataKey="trips" name="Trips" fill="#38bdf8" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      )}
    </Panel>
  );
}

function PaymentMix({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<PaymentRow[]>({
    queryKey: ["payment-mix", filters],
    queryFn: () => fetchPaymentMix(filters),
  });
  return (
    <Panel title="Payment Mix" loading={isLoading} error={!!error} empty={data?.length === 0}>
      {data && (
        <ResponsiveContainer width="100%" height={340}>
          <PieChart>
            <Pie
              data={data}
              dataKey="trips"
              nameKey="payment_type"
              cx="50%"
              cy="50%"
              outerRadius={120}
              label={(e: any) => `${e.payment_type} (${(e.pct_of_trips * 100).toFixed(0)}%)`}
            >
              {data.map((_, i) => (
                <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
              ))}
            </Pie>
            <Tooltip contentStyle={TT_STYLE} formatter={(v: number) => num(v)} />
          </PieChart>
        </ResponsiveContainer>
      )}
    </Panel>
  );
}

function TopRoutes({ filters }: { filters: FilterState }) {
  const { data, isLoading, error } = useQuery<RouteRow[]>({
    queryKey: ["top-routes", filters],
    queryFn: () => fetchTopRoutes(filters),
  });
  return (
    <Panel title="Busiest Routes" loading={isLoading} error={!!error} empty={data?.length === 0}>
      {data && (
        <table className="tbl">
          <thead>
            <tr><th>Pickup</th><th>Dropoff</th><th className="r">Trips</th><th className="r">Avg Fare</th><th className="r">Avg Miles</th></tr>
          </thead>
          <tbody>
            {data.map((r, i) => (
              <tr key={i}>
                <td>{r.pickup_area}</td>
                <td>{r.dropoff_area}</td>
                <td className="r">{num(r.trips)}</td>
                <td className="r">{r.avg_fare != null ? currency(r.avg_fare) : "—"}</td>
                <td className="r">{r.avg_miles != null ? r.avg_miles.toFixed(1) : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

function PickupAreas({
  filters,
  setFilters,
}: {
  filters: FilterState;
  setFilters: (f: FilterState) => void;
}) {
  const { data, isLoading, error } = useQuery<PickupAreaRow[]>({
    queryKey: ["pickup-areas", filters],
    queryFn: () => fetchPickupAreas(filters),
  });
  return (
    <Panel title="Top Pickup Community Areas" loading={isLoading} error={!!error} empty={data?.length === 0}>
      {data && (
        <table className="tbl">
          <thead>
            <tr><th>Community Area</th><th className="r">Trips</th><th className="r">Revenue</th><th className="r">Avg Fare</th></tr>
          </thead>
          <tbody>
            {data.map((r) => (
              <tr
                key={r.pickup_community_area}
                className="clickable"
                title="Click to filter by this pickup area"
                onClick={() => setFilters({ ...filters, pickup_area: r.pickup_community_area })}
              >
                <td>
                  {r.area_name}
                  {r.is_airport && <span className="badge">airport</span>}
                </td>
                <td className="r">{num(r.trips)}</td>
                <td className="r">{currency(r.revenue)}</td>
                <td className="r">{r.avg_fare != null ? currency(r.avg_fare) : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Panel>
  );
}

/* ---------------------------------- App ----------------------------------- */

export default function App() {
  const [filters, setFilters] = useState<FilterState>({});
  const { data: options } = useQuery<FilterOptions>({
    queryKey: ["filters"],
    queryFn: fetchFilters,
  });

  return (
    <div className="app">
      <header className="hdr">
        <h1>Chicago Taxi Lakehouse Dashboard</h1>
        <p className="subtitle">
          Interactive analytics · DuckDB aggregates the silver Iceberg table live per filter ·
          Socrata → MinIO → Iceberg (bronze/silver/gold) · Dagster + Spark
        </p>
      </header>

      <FilterBar filters={filters} setFilters={setFilters} options={options} />
      <SummaryCards filters={filters} />
      <TripsDaily filters={filters} />
      <DemandHeatmap filters={filters} />
      <div className="grid-2">
        <TripsByCompany filters={filters} />
        <PaymentMix filters={filters} />
      </div>
      <div className="grid-2">
        <TopRoutes filters={filters} />
        <PickupAreas filters={filters} setFilters={setFilters} />
      </div>
    </div>
  );
}
