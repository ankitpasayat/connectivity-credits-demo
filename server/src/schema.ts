// PostgreSQL schema: TEXT snapshots and timestamps preserve imported records exactly.
export const SCHEMA = `
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS schools (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, country TEXT NOT NULL, region TEXT NOT NULL,
    provider_id TEXT NOT NULL REFERENCES providers(id)
);
CREATE TABLE IF NOT EXISTS rules (
    version INTEGER PRIMARY KEY,
    min_download_mbps DOUBLE PRECISION NOT NULL CHECK(min_download_mbps >= 0),
    min_upload_mbps DOUBLE PRECISION NOT NULL CHECK(min_upload_mbps >= 0),
    max_latency_ms DOUBLE PRECISION NOT NULL CHECK(max_latency_ms >= 0),
    min_uptime_pct DOUBLE PRECISION NOT NULL CHECK(min_uptime_pct BETWEEN 0 AND 100),
    credits_per_period INTEGER NOT NULL CHECK(credits_per_period > 0),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS measurements (
    id TEXT PRIMARY KEY, source TEXT NOT NULL, external_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    school_id TEXT NOT NULL REFERENCES schools(id),
    provider_id TEXT NOT NULL REFERENCES providers(id), period TEXT NOT NULL,
    download_mbps DOUBLE PRECISION NOT NULL CHECK(download_mbps >= 0),
    upload_mbps DOUBLE PRECISION NOT NULL CHECK(upload_mbps >= 0),
    latency_ms DOUBLE PRECISION NOT NULL CHECK(latency_ms >= 0),
    uptime_pct DOUBLE PRECISION NOT NULL CHECK(uptime_pct BETWEEN 0 AND 100),
    verification_status TEXT NOT NULL CHECK(verification_status IN ('verified','unverified')),
    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','eligible','ineligible')),
    checks_json TEXT NOT NULL DEFAULT '[]',
    rule_version INTEGER REFERENCES rules(version),
    credit_issued INTEGER NOT NULL DEFAULT 0 CHECK(credit_issued IN (0,1)),
    created_at TEXT NOT NULL, evaluated_at TEXT,
    UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS measurements_school_period ON measurements(school_id, period DESC);
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY, measurement_id TEXT NOT NULL UNIQUE REFERENCES measurements(id),
    type TEXT NOT NULL DEFAULT 'measurement.received',
    status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','processed','failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts >= 0),
    last_error TEXT, created_at TEXT NOT NULL, processed_at TEXT
);
CREATE INDEX IF NOT EXISTS events_queue ON events(status, created_at, id);
CREATE TABLE IF NOT EXISTS credits (
    id TEXT PRIMARY KEY, school_id TEXT NOT NULL REFERENCES schools(id),
    provider_id TEXT NOT NULL REFERENCES providers(id), period TEXT NOT NULL,
    amount INTEGER NOT NULL CHECK(amount > 0),
    measurement_id TEXT NOT NULL UNIQUE REFERENCES measurements(id),
    rule_version INTEGER NOT NULL REFERENCES rules(version), created_at TEXT NOT NULL,
    UNIQUE(school_id, period)
);
CREATE TABLE IF NOT EXISTS decisions (
    id TEXT PRIMARY KEY, measurement_id TEXT NOT NULL UNIQUE REFERENCES measurements(id),
    event_id TEXT NOT NULL UNIQUE REFERENCES events(id),
    status TEXT NOT NULL CHECK(status IN ('eligible','ineligible')),
    rule_version INTEGER NOT NULL REFERENCES rules(version),
    rule_snapshot_json TEXT NOT NULL, checks_json TEXT NOT NULL,
    credit_issued INTEGER NOT NULL CHECK(credit_issued IN (0,1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS activity (
    id TEXT PRIMARY KEY, type TEXT NOT NULL, title TEXT NOT NULL,
    detail TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS broker_outbox (
    id TEXT PRIMARY KEY, event_id TEXT NOT NULL REFERENCES events(id),
    kind TEXT NOT NULL DEFAULT 'measurement' CHECK(kind IN ('measurement','retry','replay')),
    namespace TEXT NOT NULL, payload_json TEXT NOT NULL, created_at TEXT NOT NULL,
    published_at TEXT, broker_partition INTEGER, broker_offset BIGINT,
    consumed_at TEXT, last_error TEXT
);
CREATE INDEX IF NOT EXISTS broker_outbox_pending ON broker_outbox(published_at,created_at,id);
CREATE TABLE IF NOT EXISTS event_transport_state (
    event_id TEXT PRIMARY KEY REFERENCES events(id),
    required_delivery_id TEXT NOT NULL REFERENCES broker_outbox(id)
);
CREATE TABLE IF NOT EXISTS broker_inbox (
    delivery_id TEXT PRIMARY KEY REFERENCES broker_outbox(id),
    event_id TEXT NOT NULL REFERENCES events(id), topic TEXT NOT NULL,
    broker_partition INTEGER NOT NULL, broker_offset BIGINT NOT NULL,
    received_at TEXT NOT NULL,
    UNIQUE(topic,broker_partition,broker_offset)
);

CREATE TABLE IF NOT EXISTS runtime_locks(name TEXT PRIMARY KEY,owner TEXT NOT NULL,expires_at_ms BIGINT NOT NULL);
`;
