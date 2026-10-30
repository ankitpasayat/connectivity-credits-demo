export type Health = {
  status: "ok";
  mode: "demo";
  database: "ok";
  database_engine: "postgresql";
  transport: "local" | "kafka";
  broker: "not_used" | "ok" | "unavailable";
  broker_detail: string | null;
};
export type School = {
  id: string;
  name: string;
  country: string;
  region: string;
  provider_id: string;
  provider_name: string;
  status: "connected" | "needs_attention" | "awaiting_data";
  credits_earned: number;
  latest_download_mbps: number | null;
  latest_uptime_pct: number | null;
};
export type Provider = { id: string; name: string; country: string };
export type Rule = {
  version: number;
  min_download_mbps: number;
  min_upload_mbps: number;
  max_latency_ms: number;
  min_uptime_pct: number;
  credits_per_period: number;
  updated_at: string;
};
export type Check = {
  key: string;
  label: string;
  actual: number | string;
  required: number | string;
  passed: boolean;
};
export type Measurement = {
  id: string;
  source: string;
  external_id: string;
  school_id: string;
  school_name: string;
  provider_id: string;
  provider_name: string;
  country: string;
  period: string;
  download_mbps: number;
  upload_mbps: number;
  latency_ms: number;
  uptime_pct: number;
  verification_status: "verified" | "unverified";
  status: "pending" | "eligible" | "ineligible";
  checks: Check[];
  rule_version: number | null;
  credit_issued: boolean;
  created_at: string;
  evaluated_at: string | null;
};
export type Credit = {
  id: string;
  school_id: string;
  school_name: string;
  provider_id: string;
  provider_name: string;
  country: string;
  period: string;
  amount: number;
  measurement_id: string;
  rule_version: number;
  created_at: string;
};
export type Event = {
  id: string;
  measurement_id: string;
  school_name: string;
  type: string;
  status: "queued" | "processed" | "failed";
  attempts: number;
  last_error: string | null;
  created_at: string;
  processed_at: string | null;
};
export type Activity = {
  id: string;
  type: string;
  title: string;
  detail: string;
  created_at: string;
};
export type Overview = {
  summary: {
    schools_connected: number;
    schools_total: number;
    credits_issued: number;
    eligible_measurements: number;
    total_measurements: number;
    pass_rate: number;
    queued_events: number;
    failed_events: number;
  };
  trends: { date: string; credits: number; measurements: number }[];
  country_breakdown: {
    country: string;
    schools: number;
    credits: number;
    pass_rate: number;
  }[];
  recent_activity: Activity[];
};
export type Data = {
  health: Health;
  overview: Overview;
  schools: School[];
  providers: Provider[];
  measurements: Measurement[];
  credits: Credit[];
  events: Event[];
  rules: Rule;
};
export type Scenario = {
  title: string;
  message: string;
  measurement_ids: string[];
  event_ids: string[];
  processed: number;
  credits_created: number;
  duplicates_prevented: number;
};
