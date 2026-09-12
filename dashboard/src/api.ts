const BASE = import.meta.env.VITE_API_BASE ?? "http://localhost:8000";

export interface FilterState {
  start?: string;
  end?: string;
  company?: string;
  payment_type?: string;
  pickup_area?: number;
  day_type?: "weekday" | "weekend";
}

export interface FilterOptions {
  date_min: string | null;
  date_max: string | null;
  companies: string[];
  payment_types: string[];
  pickup_areas: { code: number; name: string }[];
}

export interface Summary {
  total_trips: number;
  total_revenue: number;
  days: number;
  avg_fare: number;
  avg_tip_pct: number;
  avg_miles: number;
}

export interface DailyRow {
  day: string;
  trips: number;
  revenue: number;
  avg_fare: number | null;
  avg_tip_pct: number | null;
  avg_miles: number | null;
  avg_duration_min: number | null;
}

export interface HourRow {
  dow_num: number;
  day_name: string;
  hour: number;
  trips: number;
  avg_fare: number | null;
}

export interface CompanyRow {
  company: string;
  trips: number;
  revenue: number;
  avg_fare: number | null;
}

export interface PaymentRow {
  payment_type: string;
  trips: number;
  revenue: number;
  pct_of_trips: number;
}

export interface RouteRow {
  pickup_area: string;
  dropoff_area: string;
  trips: number;
  avg_fare: number | null;
  avg_miles: number | null;
}

export interface PickupAreaRow {
  pickup_community_area: number;
  area_name: string;
  is_airport: boolean;
  trips: number;
  revenue: number;
  avg_fare: number | null;
}

export function toQuery(f: FilterState): string {
  const p = new URLSearchParams();
  if (f.start) p.set("start", f.start);
  if (f.end) p.set("end", f.end);
  if (f.company) p.set("company", f.company);
  if (f.payment_type) p.set("payment_type", f.payment_type);
  if (f.pickup_area != null) p.set("pickup_area", String(f.pickup_area));
  if (f.day_type) p.set("day_type", f.day_type);
  const s = p.toString();
  return s ? `?${s}` : "";
}

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`${path} -> HTTP ${res.status}`);
  return res.json() as Promise<T>;
}

export const fetchFilters = () => get<FilterOptions>("/api/filters");
export const fetchSummary = (f: FilterState) => get<Summary>(`/api/summary${toQuery(f)}`);
export const fetchTripsDaily = (f: FilterState) => get<DailyRow[]>(`/api/trips-daily${toQuery(f)}`);
export const fetchTripsByHour = (f: FilterState) => get<HourRow[]>(`/api/trips-by-hour${toQuery(f)}`);
export const fetchTripsByCompany = (f: FilterState) => get<CompanyRow[]>(`/api/trips-by-company${toQuery(f)}`);
export const fetchPaymentMix = (f: FilterState) => get<PaymentRow[]>(`/api/payment-mix${toQuery(f)}`);
export const fetchTopRoutes = (f: FilterState) => get<RouteRow[]>(`/api/top-routes${toQuery(f)}`);
export const fetchPickupAreas = (f: FilterState) => get<PickupAreaRow[]>(`/api/pickup-areas${toQuery(f)}`);
