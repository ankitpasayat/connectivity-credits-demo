import { useEffect, useMemo, useRef, useState } from "react";
import type { FormEvent, ReactNode } from "react";
import { api, fetchData } from "./api";
import type {
  Data,
  Health,
  Measurement,
  Rule,
  Scenario,
  Overview as OverviewData,
} from "./types";

type Tab =
  | "overview"
  | "schools"
  | "measurements"
  | "ledger"
  | "events"
  | "policy";
type Toast = { text: string; error?: boolean };
type TransportInfo = {
  label: string;
  detail: string;
  mode: "local" | "kafka" | "unknown";
  healthy: boolean;
};
function getTransportInfo(
  health: Health | undefined,
  stale: boolean,
): TransportInfo {
  if (!health || stale)
    return {
      label: "Transport status unavailable",
      detail: "Refresh to verify the active event transport.",
      mode: "unknown",
      healthy: false,
    };
  if (health.transport === "kafka")
    return health.broker === "ok"
      ? {
          label: "Kafka connected",
          detail: "Broker metadata verified · Kafka event transport",
          mode: "kafka",
          healthy: true,
        }
      : {
          label: "Kafka unavailable",
          detail:
            health.broker_detail ||
            "The Kafka broker is not reachable. Worker actions require the broker.",
          mode: "kafka",
          healthy: false,
        };
  if (health.transport === "local" && health.broker === "not_used")
    return {
      label: "Local queue active",
      detail: "Durable event queue · explicit worker processing",
      mode: "local",
      healthy: true,
    };
  return {
    label: "Transport status unavailable",
    detail: "The service did not report a verified transport state.",
    mode: "unknown",
    healthy: false,
  };
}
const labels: Record<Tab, string> = {
  overview: "Overview",
  schools: "Schools",
  measurements: "Measurements",
  ledger: "Credit ledger",
  events: "Event log",
  policy: "Policy",
};
const iconPaths: Record<string, ReactNode> = {
  grid: (
    <>
      <rect x="3" y="3" width="7" height="7" rx="1.5" />
      <rect x="14" y="3" width="7" height="7" rx="1.5" />
      <rect x="3" y="14" width="7" height="7" rx="1.5" />
      <rect x="14" y="14" width="7" height="7" rx="1.5" />
    </>
  ),
  school: (
    <>
      <path d="m3 10 9-7 9 7v11H3Z" />
      <path d="M9 21v-7h6v7M8 10h.01M16 10h.01" />
    </>
  ),
  pulse: <path d="M2 12h5l3-8 4 16 3-8h5" />,
  coins: (
    <>
      <ellipse cx="12" cy="6" rx="8" ry="3" />
      <path d="M4 6v6c0 4 16 4 16 0V6M4 12v6c0 4 16 4 16 0v-6" />
    </>
  ),
  list: (
    <>
      <path d="M8 6h13M8 12h13M8 18h13M3 6h.01M3 12h.01M3 18h.01" />
    </>
  ),
  sliders: (
    <>
      <path d="M4 7h4m5 0h7M4 17h10m5 0h1" />
      <circle cx="10" cy="7" r="3" />
      <circle cx="16" cy="17" r="3" />
    </>
  ),
  arrow: <path d="M5 12h14m-5-5 5 5-5 5" />,
  chevron: <path d="m9 5 7 7-7 7" />,
  down: <path d="m6 9 6 6 6-6" />,
  plus: <path d="M12 5v14M5 12h14" />,
  check: <path d="m5 12 4 4L19 6" />,
  close: <path d="m6 6 12 12M6 18 18 6" />,
  search: (
    <>
      <circle cx="10.5" cy="10.5" r="6.5" />
      <path d="m16 16 4 4" />
    </>
  ),
  refresh: (
    <>
      <path d="M20 7v5h-5M4 17v-5h5" />
      <path d="M5.5 7a7.5 7.5 0 0 1 12-2L20 8M4 16l2.5 3a7.5 7.5 0 0 0 12-2" />
    </>
  ),
  play: <path d="m8 4 12 8-12 8Z" />,
  shield: (
    <>
      <path d="m12 2 8 4v6c0 5-8 10-8 10S4 17 4 12V6Z" />
      <path d="m8 12 3 3 5-6" />
    </>
  ),
  globe: (
    <>
      <circle cx="12" cy="12" r="9" />
      <ellipse cx="12" cy="12" rx="4" ry="9" />
      <path d="M3 12h18M5 6h14M5 18h14" />
    </>
  ),
  clock: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 2" />
    </>
  ),
  download: (
    <>
      <path d="M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5" />
    </>
  ),
  alert: (
    <>
      <path d="m12 3 10 18H2Z" />
      <path d="M12 9v5m0 3h.01" />
    </>
  ),
  copy: (
    <>
      <rect x="8" y="8" width="12" height="13" rx="2" />
      <path d="M16 8V3H3v13h5" />
    </>
  ),
  spark: (
    <>
      <path d="m12 2 2.7 7.3L22 12l-7.3 2.7L12 22l-2.7-7.3L2 12l7.3-2.7Z" />
    </>
  ),
  menu: <path d="M4 6h16M4 12h16M4 18h16" />,
  info: (
    <>
      <circle cx="12" cy="12" r="9" />
      <path d="M12 11v6m0-10h.01" />
    </>
  ),
};
function Icon({
  name,
  size = 20,
  className = "",
}: {
  name: string;
  size?: number;
  className?: string;
}) {
  return (
    <svg
      className={className}
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {iconPaths[name] || iconPaths.grid}
    </svg>
  );
}
const format = (n: number) =>
  new Intl.NumberFormat("en", { maximumFractionDigits: 1 }).format(n);
const date = (value: string) =>
  new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(value.length === 10 ? `${value}T00:00:00Z` : value));
const shortDate = (value: string) =>
  new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    timeZone: "UTC",
  }).format(new Date(value.length === 10 ? `${value}T00:00:00Z` : value));
const time = (value: string) =>
  new Intl.DateTimeFormat("en", {
    hour: "2-digit",
    minute: "2-digit",
    timeZone: "UTC",
    hour12: false,
  }).format(new Date(value)) + " UTC";
function Badge({ status }: { status: string }) {
  const names: Record<string, string> = {
    connected: "Meeting policy",
    needs_attention: "Needs attention",
    awaiting_data: "Awaiting data",
    eligible: "Eligible",
    ineligible: "Ineligible",
    pending: "Pending",
    processed: "Processed",
    queued: "Queued",
    failed: "Failed",
    verified: "Verified",
    unverified: "Unverified",
  };
  return (
    <span className={`badge badge-${status}`}>
      <span />
      {names[status] || status}
    </span>
  );
}
function Empty({
  title = "No records found",
  text = "Try changing your search or filters.",
}: {
  title?: string;
  text?: string;
}) {
  return (
    <div className="empty">
      <Icon name="search" size={28} />
      <h3>{title}</h3>
      <p>{text}</p>
    </div>
  );
}
function Modal({
  title,
  eyebrow,
  onClose,
  children,
  wide = false,
}: {
  title: string;
  eyebrow?: string;
  onClose: () => void;
  children: ReactNode;
  wide?: boolean;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const closeRef = useRef(onClose);
  closeRef.current = onClose;
  useEffect(() => {
    const previous = document.activeElement as HTMLElement;
    const old = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    ref.current?.focus();
    const handle = (e: KeyboardEvent) => {
      if (e.key === "Escape") closeRef.current();
      if (e.key === "Tab") {
        const nodes = ref.current?.querySelectorAll<HTMLElement>(
          'button:not(:disabled), input, select, textarea, [tabindex="0"]',
        );
        if (nodes?.length) {
          const first = nodes[0],
            last = nodes[nodes.length - 1];
          if (
            e.shiftKey &&
            (document.activeElement === first ||
              document.activeElement === ref.current)
          ) {
            e.preventDefault();
            last.focus();
          } else if (!e.shiftKey && document.activeElement === last) {
            e.preventDefault();
            first.focus();
          }
        }
      }
    };
    document.addEventListener("keydown", handle);
    return () => {
      document.body.style.overflow = old;
      document.removeEventListener("keydown", handle);
      previous?.focus();
    };
  }, []);
  return (
    <div
      className="modal-backdrop"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className={`modal ${wide ? "modal-wide" : ""}`}
        ref={ref}
        tabIndex={-1}
        role="dialog"
        aria-modal="true"
        aria-label={title}
      >
        <div className="modal-heading">
          <div>
            {eyebrow && <div className="eyebrow">{eyebrow}</div>}
            <h2>{title}</h2>
          </div>
          <button
            className="icon-button"
            aria-label="Close dialog"
            onClick={onClose}
          >
            <Icon name="close" />
          </button>
        </div>
        {children}
      </div>
    </div>
  );
}

function CreditsChart({ trends }: { trends: OverviewData["trends"] }) {
  const [range, setRange] = useState(14);
  const values = trends.slice(-range);
  const width = 720,
    height = 230,
    left = 42,
    right = 16,
    top = 18,
    bottom = 39;
  const plotH = height - top - bottom,
    plotW = width - left - right;
  const max = Math.max(
    10,
    Math.ceil(Math.max(...values.map((t) => t.credits), 0) / 10) * 10,
  );
  const coords = values.map((t, i) => ({
    x:
      left +
      (values.length <= 1 ? plotW / 2 : (i * plotW) / (values.length - 1)),
    y: top + plotH * (1 - t.credits / max),
    ...t,
  }));
  const line = coords.map((p, i) => `${i ? "L" : "M"} ${p.x} ${p.y}`).join(" ");
  const area = coords.length
    ? `${line} L ${coords[coords.length - 1].x} ${top + plotH} L ${coords[0].x} ${top + plotH} Z`
    : "";
  const total = values.reduce((n, t) => n + t.credits, 0);
  return (
    <section className="panel chart-panel">
      <div className="panel-heading">
        <div>
          <h2>Credits over time</h2>
          <p>Daily issuance · demo credits</p>
        </div>
        <div className="segmented" aria-label="Chart date range">
          {[7, 14].map((n) => (
            <button
              key={n}
              className={n === range ? "active" : ""}
              onClick={() => setRange(n)}
              aria-pressed={n === range}
            >
              {n} days
            </button>
          ))}
        </div>
      </div>
      <div className="chart-summary">
        <strong>{format(total)}</strong>
        <span>demo credits in this period</span>
        <span className="chart-legend">
          <i /> Issued credits
        </span>
      </div>
      {values.length ? (
        <div className="chart-wrap">
          <svg
            viewBox={`0 0 ${width} ${height}`}
            role="img"
            aria-label={`${total} demo credits issued over the last ${range} days`}
          >
            <defs>
              <linearGradient id="area" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#0e9481" stopOpacity=".2" />
                <stop offset="100%" stopColor="#0e9481" stopOpacity="0" />
              </linearGradient>
            </defs>
            {[0, 1, 2, 3, 4].map((n) => (
              <g key={n}>
                <line
                  x1={left}
                  y1={top + (plotH * n) / 4}
                  x2={width - right}
                  y2={top + (plotH * n) / 4}
                  stroke="#e9eeeb"
                  strokeDasharray="3 5"
                />
                <text
                  x={left - 12}
                  y={top + (plotH * n) / 4 + 4}
                  textAnchor="end"
                  className="axis-text"
                >
                  {format(max * (1 - n / 4))}
                </text>
              </g>
            ))}
            <path d={area} fill="url(#area)" />
            <path
              d={line}
              fill="none"
              stroke="#118e7c"
              strokeWidth="3"
              strokeLinejoin="round"
              strokeLinecap="round"
            />
            {coords.map((p, i) => (
              <g key={p.date}>
                <circle
                  className="chart-point"
                  cx={p.x}
                  cy={p.y}
                  r="4"
                  fill="#fff"
                  stroke="#118e7c"
                  strokeWidth="2"
                >
                  <title>
                    {date(p.date)}: {p.credits} demo credits, {p.measurements}{" "}
                    measurements
                  </title>
                </circle>
                {(i === 0 ||
                  i === coords.length - 1 ||
                  i % Math.ceil(coords.length / 5) === 0) && (
                  <text
                    x={p.x}
                    y={height - 10}
                    textAnchor="middle"
                    className="axis-text"
                  >
                    {shortDate(p.date)}
                  </text>
                )}
              </g>
            ))}
          </svg>
        </div>
      ) : (
        <Empty
          title="No issuance history"
          text="Process measurements to see daily credit issuance."
        />
      )}
    </section>
  );
}

function Overview({
  data,
  transport,
  busy,
  onScenario,
  onNavigate,
  onCountry,
  onProcess,
}: {
  data: Data;
  transport: TransportInfo;
  busy: string;
  onScenario: (s: string) => void;
  onNavigate: (t: Tab) => void;
  onCountry: (c: string) => void;
  onProcess: () => void;
}) {
  const { summary, country_breakdown, recent_activity } = data.overview;
  const maxCountry = Math.max(...country_breakdown.map((c) => c.credits), 1);
  return (
    <>
      <div
        className={`overview-note ${transport.healthy ? "" : "transport-warning"}`}
      >
        <span role="status">
          <span className="live-dot" /> {transport.label}
        </span>
        <span className="transport-detail" title={transport.detail}>
          {transport.detail}
        </span>
        <Icon name="globe" size={18} />
      </div>
      <div className="metrics-grid">
        <Metric
          label="Schools meeting policy"
          value={
            <>
              {summary.schools_connected}
              <span className="metric-denominator">
                {" "}
                / {summary.schools_total}
              </span>
            </>
          }
          icon="school"
          note="Latest reading's policy status"
        />
        <Metric
          label="Credits issued"
          value={format(summary.credits_issued)}
          icon="coins"
          note="Demo credits · all time"
        />
        <Metric
          label="Eligibility rate"
          value={
            <>
              {format(summary.pass_rate)}
              <span className="metric-unit">%</span>
            </>
          }
          icon="shield"
          note={`${format(summary.eligible_measurements)} qualifying measurements`}
          progress={summary.pass_rate}
        />
        <Metric
          label="Queue to process"
          value={summary.queued_events}
          icon="list"
          note={
            summary.failed_events
              ? `${summary.failed_events} failed ${summary.failed_events === 1 ? "event needs" : "events need"} attention`
              : "Ready for the next reading"
          }
          attention={summary.failed_events > 0}
          onClick={() => onNavigate("events")}
        />
      </div>
      <div className="overview-main">
        <CreditsChart trends={data.overview.trends} />
        <section className="panel countries-panel">
          <div className="panel-heading">
            <div>
              <h2>Across the demo network</h2>
              <p>One eligibility policy across example countries.</p>
            </div>
            <Icon name="globe" size={19} />
          </div>
          <div className="country-list">
            {country_breakdown.map((c, i) => (
              <button
                className="country-row"
                key={c.country}
                onClick={() => onCountry(c.country)}
              >
                <span className={`country-marker marker-${i}`}>
                  {({ Kenya: "KE", Nepal: "NP", Rwanda: "RW" } as Record<string, string>)[c.country] || c.country.slice(0, 2).toUpperCase()}
                </span>
                <div className="country-content">
                  <div className="country-title">
                    <strong>{c.country}</strong>
                    <span>
                      {format(c.credits)} <small>demo credits</small>
                    </span>
                  </div>
                  <div className="country-track">
                    <i
                      style={{ width: `${(c.credits / maxCountry) * 100}%` }}
                    />
                  </div>
                  <div className="country-meta">
                    <span>{c.schools} schools</span>
                    <span>{format(c.pass_rate)}% eligible</span>
                  </div>
                </div>
                <Icon name="chevron" size={15} />
              </button>
            ))}
          </div>
          <div className="panel-footnote">
            <Icon name="info" size={15} />
            <span>Synthetic schools, providers and connectivity data.</span>
          </div>
        </section>
      </div>
      <section className="demo-section">
        <div className="section-title">
          <div>
            <span className="eyebrow">EXPLORE THE WORKFLOW</span>
            <h2>From signal to credit</h2>
            <p>
              Run a scenario to see the rules, queue and ledger work together.
            </p>
          </div>
          <span className="subtle-pill">
            <Icon name="spark" size={14} /> Interactive demo
          </span>
        </div>
        <div className="scenario-grid">
          {[
            {
              id: "clean_batch",
              icon: "check",
              tone: "teal",
              name: "A connection that qualifies",
              tag: "Happy path",
              desc: "Three verified readings meet the policy. Watch demo credits arrive.",
              button: "Run clean batch",
            },
            {
              id: "below_threshold",
              icon: "pulse",
              tone: "amber",
              name: "When service falls short",
              tag: "Policy checks",
              desc: "Inspect a weak, unverified reading against your current policy.",
              button: "Test below threshold",
            },
            {
              id: "duplicate_delivery",
              icon: "copy",
              tone: "blue",
              name: "Same signal. Just once.",
              tag: "Idempotency",
              desc: "Replay one reading twice. The ledger records one daily issuance.",
              button: "Replay a delivery",
            },
            {
              id: "worker_failure",
              icon: "refresh",
              tone: "purple",
              name: "A failure you can recover",
              tag: "Rollback & retry",
              desc: "Interrupt issuance, confirm the rollback, then safely retry the event.",
              button: "Simulate worker failure",
            },
          ].map((s) => (
            <article className={`scenario-card scenario-${s.tone}`} key={s.id}>
              <div className="scenario-top">
                <span className="scenario-icon">
                  <Icon name={s.icon} />
                </span>
                <span>{s.tag}</span>
              </div>
              <h3>{s.name}</h3>
              <p>{s.desc}</p>
              <button onClick={() => onScenario(s.id)} disabled={!!busy}>
                {busy === s.id ? "Running scenario…" : s.button}
                <Icon name="arrow" size={16} />
              </button>
            </article>
          ))}
        </div>
      </section>
      <div className="overview-bottom">
        <section className="panel activity-panel">
          <div className="panel-heading">
            <div>
              <h2>Latest activity</h2>
              <p>A traceable record of each step</p>
            </div>
            <button
              className="text-button"
              onClick={() => onNavigate("events")}
            >
              View event log <Icon name="arrow" size={15} />
            </button>
          </div>
          <div className="activity-list">
            {recent_activity.slice(0, 4).map((a, i) => (
              <div className="activity-row" key={a.id}>
                <span
                  className={`activity-icon ${a.type.includes("fail") ? "is-warning" : ""}`}
                >
                  <Icon
                    name={
                      a.type.includes("credit")
                        ? "coins"
                        : a.type.includes("fail")
                          ? "alert"
                          : "pulse"
                    }
                    size={17}
                  />
                </span>
                <div>
                  <strong>{a.title}</strong>
                  <p>{a.detail}</p>
                </div>
                <span className="activity-time">
                  {shortDate(a.created_at)}
                  <small>{time(a.created_at)}</small>
                </span>
                {i === 0 && <span className="new-activity" />}
              </div>
            ))}
            {!recent_activity.length && (
              <Empty
                title="Your activity starts here"
                text="Submit or process a measurement to create an audit trail."
              />
            )}
          </div>
        </section>
        <aside className="workflow-panel">
          <span className="workflow-kicker">
            <Icon name="shield" size={17} /> BUILT ON EVIDENCE
          </span>
          <h2>
            A clear path from
            <br />
            connectivity to credit.
          </h2>
          <div className="workflow-steps">
            <span>
              <i>01</i> Receive a service measurement
            </span>
            <span>
              <i>02</i> Evaluate the active policy
            </span>
            <span>
              <i>03</i> Record one daily issuance
            </span>
          </div>
          <button
            onClick={onProcess}
            disabled={!!busy}
            className="button workflow-button"
          >
            <Icon name="play" size={15} />
            {busy === "process" ? "Processing…" : "Process queued events"}
            <span>{summary.queued_events}</span>
          </button>
        </aside>
      </div>
    </>
  );
}
function Metric({
  label,
  value,
  icon,
  note,
  progress,
  attention,
  onClick,
}: {
  label: string;
  value: ReactNode;
  icon: string;
  note: string;
  progress?: number;
  attention?: boolean;
  onClick?: () => void;
}) {
  return (
    <div className={`metric-card ${attention ? "metric-attention" : ""}`}>
      <div className="metric-label">
        {label}
        <span className="metric-icon">
          <Icon name={icon} size={18} />
        </span>
      </div>
      <div className="metric-value">{value}</div>
      <div className="metric-note">
        {attention && <span className="amber-dot" />}
        {note}
        {onClick && (
          <button onClick={onClick} aria-label="Open event log">
            <Icon name="arrow" size={15} />
          </button>
        )}
      </div>
      {progress !== undefined && (
        <div className="metric-progress">
          <i style={{ width: `${progress}%` }} />
        </div>
      )}
    </div>
  );
}

function MeasurementDetail({
  m,
  onClose,
}: {
  m: Measurement;
  onClose: () => void;
}) {
  return (
    <Modal
      title="Measurement details"
      eyebrow="VERIFIABLE SERVICE EVIDENCE"
      onClose={onClose}
      wide
    >
      <div className="detail-school">
        <span className="detail-school-icon">
          <Icon name="school" size={27} />
        </span>
        <div>
          <h3>{m.school_name}</h3>
          <p>
            {m.country} <span>·</span> {m.provider_name}
          </p>
        </div>
        <Badge status={m.status} />
      </div>
      <div className="detail-summary">
        <div>
          <span>Service period</span>
          <strong>{date(m.period)}</strong>
        </div>
        <div>
          <span>Policy version</span>
          <strong>
            {m.rule_version ? `Version ${m.rule_version}` : "Not evaluated"}
          </strong>
        </div>
        <div>
          <span>Issuance</span>
          <strong className={m.credit_issued ? "teal-text" : ""}>
            {m.credit_issued
              ? "Demo credit issued"
              : m.status === "pending"
                ? "Awaiting evaluation"
                : "No credit issued"}
          </strong>
        </div>
      </div>
      <h3 className="detail-heading">
        Policy checks{" "}
        <span>
          {m.checks.filter((c) => c.passed).length} / {m.checks.length} passed
        </span>
      </h3>
      {m.checks.length ? (
        <div className="checks-table">
          <div className="check-row check-header">
            <span>Requirement</span>
            <span>Actual</span>
            <span>Required</span>
            <span>Result</span>
          </div>
          {m.checks.map((c) => (
            <div className="check-row" key={c.key}>
              <strong>{c.label}</strong>
              <span>{String(c.actual)}</span>
              <span>
                {{
                  download_mbps: "≥ ",
                  upload_mbps: "≥ ",
                  latency_ms: "≤ ",
                  uptime_pct: "≥ ",
                }[
                  c.key as
                    | "download_mbps"
                    | "upload_mbps"
                    | "latency_ms"
                    | "uptime_pct"
                ] || ""}
                {String(c.required)}
              </span>
              <span className={c.passed ? "check-pass" : "check-fail"}>
                <Icon name={c.passed ? "check" : "close"} size={15} />
                {c.passed ? "Passed" : "Failed"}
              </span>
            </div>
          ))}
        </div>
      ) : (
        <div className="info-callout">
          <Icon name="clock" size={20} />
          <p>
            This reading is waiting in the event queue. Process queued events to
            evaluate the active policy and determine credit eligibility.
          </p>
        </div>
      )}
      {m.status === "eligible" && !m.credit_issued && (
        <div className="info-callout">
          <Icon name="info" size={20} />
          <p>
            This measurement qualifies. A credit may already exist for this
            school and calendar day; daily issuance is limited to one across all
            policy versions.
          </p>
        </div>
      )}
      <div className="detail-readings">
        {[
          ["Download", `${m.download_mbps} Mbps`],
          ["Upload", `${m.upload_mbps} Mbps`],
          ["Latency", `${m.latency_ms} ms`],
          ["Uptime", `${m.uptime_pct}%`],
        ].map(([k, v]) => (
          <div key={k}>
            <span>{k}</span>
            <strong>{v}</strong>
          </div>
        ))}
      </div>
      <dl className="metadata">
        <div>
          <dt>Measurement ID</dt>
          <dd>{m.id}</dd>
        </div>
        <div>
          <dt>Source / external ID</dt>
          <dd>
            {m.source} / {m.external_id}
          </dd>
        </div>
        <div>
          <dt>Verification</dt>
          <dd>{m.verification_status}</dd>
        </div>
        <div>
          <dt>Received</dt>
          <dd>
            {date(m.created_at)}, {time(m.created_at)}
          </dd>
        </div>
        {m.evaluated_at && (
          <div>
            <dt>Evaluated</dt>
            <dd>
              {date(m.evaluated_at)}, {time(m.evaluated_at)}
            </dd>
          </div>
        )}
      </dl>
      <div className="modal-footer">
        <span>Historical decisions retain their policy snapshot.</span>
        <button className="button secondary" onClick={onClose}>
          Close details
        </button>
      </div>
    </Modal>
  );
}

function MeasurementForm({
  data,
  busy,
  onClose,
  onSubmit,
}: {
  data: Data;
  busy: boolean;
  onClose: () => void;
  onSubmit: (body: Record<string, unknown>) => Promise<void>;
}) {
  const [schoolId, setSchoolId] = useState(data.schools[0]?.id || "");
  const [verification, setVerification] = useState("verified");
  const [error, setError] = useState("");
  const school = data.schools.find((s) => s.id === schoolId);
  const rule = data.rules;
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    setError("");
    const values = new FormData(e.currentTarget);
    try {
      await onSubmit({
        source: String(values.get("source")).trim(),
        external_id: String(values.get("external_id")).trim(),
        school_id: schoolId,
        provider_id: school?.provider_id,
        period: values.get("period"),
        download_mbps: Number(values.get("download_mbps")),
        upload_mbps: Number(values.get("upload_mbps")),
        latency_ms: Number(values.get("latency_ms")),
        uptime_pct: Number(values.get("uptime_pct")),
        verification_status: verification,
      });
    } catch (err) {
      setError((err as Error).message);
    }
  }
  return (
    <Modal
      title="Submit a simulated reading"
      eyebrow="SYNTHETIC DATA INGESTION"
      onClose={onClose}
    >
      <form onSubmit={submit}>
        <p className="form-intro">
          Choose fictional values to test a policy decision. This form does not
          run a speed test or collect measurements from a school.
        </p>
        {error && (
          <div className="form-error" role="alert">
            {error}
          </div>
        )}
        <div className="form-grid">
          <label className="full">
            School
            <select
              value={schoolId}
              onChange={(e) => setSchoolId(e.target.value)}
              required
            >
              {data.schools.map((s) => (
                <option key={s.id} value={s.id}>
                  {s.name} · {s.country}
                </option>
              ))}
            </select>
          </label>
          <div className="derived-field full">
            <Icon name="globe" size={17} />
            <span>Associated provider</span>
            <strong>{school?.provider_name || "Select a school"}</strong>
          </div>
          <label>
            Service date
            <input
              type="date"
              name="period"
              defaultValue={new Date().toISOString().slice(0, 10)}
              max={new Date().toISOString().slice(0, 10)}
              required
            />
          </label>
          <label>
            Verification
            <select
              value={verification}
              onChange={(e) => setVerification(e.target.value)}
            >
              <option value="verified">Verified (simulated)</option>
              <option value="unverified">Unverified</option>
            </select>
          </label>
          <label>
            Download <small>Mbps</small>
            <input
              name="download_mbps"
              type="number"
              min="0"
              max="1000000"
              step="0.01"
              defaultValue={rule.min_download_mbps + 10}
              required
            />
          </label>
          <label>
            Upload <small>Mbps</small>
            <input
              name="upload_mbps"
              type="number"
              min="0"
              max="1000000"
              step="0.01"
              defaultValue={rule.min_upload_mbps + 3}
              required
            />
          </label>
          <label>
            Latency <small>ms</small>
            <input
              name="latency_ms"
              type="number"
              min="0"
              max="1000000"
              step="0.01"
              defaultValue={Math.min(rule.max_latency_ms, 35)}
              required
            />
          </label>
          <label>
            Uptime <small>%</small>
            <input
              name="uptime_pct"
              type="number"
              min="0"
              max="100"
              step="0.01"
              defaultValue={Math.min(100, rule.min_uptime_pct + 1)}
              required
            />
          </label>
          <label>
            Source
            <input
              name="source"
              defaultValue="console-demo"
              maxLength={100}
              required
            />
          </label>
          <label>
            External event ID
            <input
              name="external_id"
              defaultValue={`reading-${crypto.randomUUID().slice(0, 8)}`}
              maxLength={160}
              required
            />
          </label>
        </div>
        <div className="form-help">
          <Icon name="info" size={16} />
          <span>
            Source + external ID prevent duplicate deliveries. No real
            verification service is called.
          </span>
        </div>
        <div className="modal-footer">
          <button type="button" className="button secondary" onClick={onClose}>
            Cancel
          </button>
          <button className="button primary" disabled={busy || !school}>
            {busy ? "Submitting…" : "Add to queue"}
            <Icon name="arrow" size={16} />
          </button>
        </div>
      </form>
    </Modal>
  );
}

function Policy({
  rule,
  busy,
  onSave,
}: {
  rule: Rule;
  busy: boolean;
  onSave: (body: Record<string, number>) => Promise<void>;
}) {
  const [error, setError] = useState("");
  const [dirty, setDirty] = useState(false);
  const fields = [
    {
      key: "min_download_mbps",
      label: "Minimum download",
      unit: "Mbps",
      min: 0,
      max: 100000,
      step: 0.1,
      desc: "Minimum sustained download speed.",
    },
    {
      key: "min_upload_mbps",
      label: "Minimum upload",
      unit: "Mbps",
      min: 0,
      max: 100000,
      step: 0.1,
      desc: "Minimum sustained upload speed.",
    },
    {
      key: "max_latency_ms",
      label: "Maximum latency",
      unit: "ms",
      min: 0,
      max: 100000,
      step: 0.1,
      desc: "Maximum acceptable network latency.",
    },
    {
      key: "min_uptime_pct",
      label: "Minimum uptime",
      unit: "%",
      min: 0,
      max: 100,
      step: 0.01,
      desc: "Availability across the daily service period.",
    },
    {
      key: "credits_per_period",
      label: "Credits per qualifying period",
      unit: "demo credits",
      min: 1,
      max: 1000000,
      step: 1,
      desc: "Integer demo credits for each eligible school-day.",
    },
  ];
  async function submit(e: FormEvent<HTMLFormElement>) {
    e.preventDefault();
    const fd = new FormData(e.currentTarget);
    setError("");
    try {
      await onSave(
        Object.fromEntries(fields.map((f) => [f.key, Number(fd.get(f.key))])),
      );
      setDirty(false);
    } catch (err) {
      setError((err as Error).message);
    }
  }
  return (
    <div className="policy-layout">
      <section className="panel policy-panel">
        <div className="panel-heading">
          <div>
            <h2>Eligibility policy</h2>
            <p>The standards every daily reading must meet</p>
          </div>
          <span className="policy-version">Version {rule.version}</span>
        </div>
        <form
          key={rule.version}
          onSubmit={submit}
          onChange={() => setDirty(true)}
        >
          {error && (
            <div className="form-error" role="alert">
              {error}
            </div>
          )}
          <div className="policy-fields">
            {fields.map((f) => (
              <label className="policy-field" key={f.key}>
                <div>
                  <strong>{f.label}</strong>
                  <span>{f.desc}</span>
                </div>
                <div className="input-unit">
                  <input
                    aria-label={f.label}
                    name={f.key}
                    type="number"
                    defaultValue={rule[f.key as keyof Rule]}
                    min={f.min}
                    max={f.max}
                    step={f.step}
                    required
                  />
                  <span>{f.unit}</span>
                </div>
              </label>
            ))}
            <div className="policy-field">
              <div>
                <strong>Verification status</strong>
                <span>A reading must have simulated verification.</span>
              </div>
              <Badge status="verified" />
            </div>
          </div>
          <div className="policy-footer">
            <span>
              Updated {date(rule.updated_at)} · {time(rule.updated_at)}
            </span>
            <button className="button primary" disabled={busy || !dirty}>
              {busy ? "Saving…" : "Save new policy version"}
              <Icon name="check" size={16} />
            </button>
          </div>
        </form>
      </section>
      <div className="policy-aside">
        <div className="policy-explainer">
          <span className="scenario-icon">
            <Icon name="shield" size={23} />
          </span>
          <h2>
            Clear rules.
            <br />
            Durable decisions.
          </h2>
          <p>
            All five checks must pass before a measurement qualifies for demo
            credits.
          </p>
          <ul>
            <li>One credit issuance per school, per calendar day.</li>
            <li>
              Every decision retains its original policy version and check
              results.
            </li>
            <li>
              Policy edits apply to future evaluations. Existing decisions stay
              unchanged.
            </li>
          </ul>
        </div>
        <div className="info-callout">
          <Icon name="info" size={20} />
          <p>
            These thresholds are illustrative demo rules. They do not represent
            official UNICEF requirements.
          </p>
        </div>
      </div>
    </div>
  );
}

function App() {
  const [tab, setTab] = useState<Tab>("overview");
  const [data, setData] = useState<Data | null>(null);
  const [loadError, setLoadError] = useState("");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [busy, setBusy] = useState("");
  const [toast, setToast] = useState<Toast | null>(null);
  const [query, setQuery] = useState("");
  const [country, setCountry] = useState("all");
  const [status, setStatus] = useState("all");
  const [schoolFilter, setSchoolFilter] = useState("all");
  const [selected, setSelected] = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [showReset, setShowReset] = useState(false);
  const [mobileNav, setMobileNav] = useState(false);
  const [lastSync, setLastSync] = useState<Date | null>(null);
  async function refresh() {
    try {
      const next = await fetchData();
      setData(next);
      setLoadError("");
      setLastSync(new Date());
      return next;
    } catch (err) {
      setLoadError((err as Error).message);
      throw err;
    } finally {
      setLoading(false);
    }
  }
  useEffect(() => {
    void refresh().catch(() => {});
  }, []);
  useEffect(() => {
    if (!toast) return;
    const t = window.setTimeout(() => setToast(null), 8000);
    return () => window.clearTimeout(t);
  }, [toast]);
  function navigate(next: Tab) {
    setTab(next);
    setQuery("");
    setStatus("all");
    setSchoolFilter("all");
    setCountry("all");
    setMobileNav(false);
  }
  async function doRefresh() {
    setRefreshing(true);
    try {
      await refresh();
      setToast({ text: "Workspace refreshed from the demo database." });
    } catch (err) {
      setToast({ text: (err as Error).message, error: true });
    } finally {
      setRefreshing(false);
    }
  }
  async function perform<T>(
    key: string,
    work: () => Promise<T>,
    message: (result: T) => string,
  ): Promise<{ result: T; next: Data }> {
    setBusy(key);
    try {
      const result = await work();
      const next = await refresh();
      setToast({ text: message(result) });
      return { result, next };
    } catch (err) {
      // A broker outage can invalidate a previously healthy transport reading.
      await refresh().catch(() => {});
      setToast({ text: (err as Error).message, error: true });
      throw err;
    } finally {
      setBusy("");
    }
  }
  async function process() {
    await perform(
      "process",
      () =>
        api<{
          processed: number;
          issued: number;
          rejected: number;
          failed: number;
        }>("/worker/run", {
          method: "POST",
          body: JSON.stringify({ limit: 50 }),
        }),
      (r) =>
        `${r.processed} processed · ${r.issued} demo credit issuances · ${r.rejected} ineligible · ${r.failed} failed.`,
    ).catch(() => {});
  }
  async function scenario(name: string) {
    const value = await perform(
      name,
      () => api<Scenario>(`/demo/scenarios/${name}`, { method: "POST" }),
      (r) => `${r.title}: ${r.message}`,
    ).catch(() => null);
    if (value) {
      if (name === "worker_failure") navigate("events");
      if (name === "below_threshold")
        setSelected(value.result.measurement_ids[0]);
    }
  }
  async function retry(id: string) {
    await perform(
      id,
      () => api(`/events/${id}/retry`, { method: "POST" }),
      () =>
        "Event retried safely. The measurement, ledger and queue are up to date.",
    ).catch(() => {});
  }
  async function submitMeasurement(body: Record<string, unknown>) {
    await perform(
      "submit",
      () =>
        api<{ measurement: Measurement; duplicate: boolean }>("/measurements", {
          method: "POST",
          body: JSON.stringify(body),
        }),
      (r) =>
        r.duplicate
          ? "Duplicate delivery recognized. No new work was created."
          : "Measurement added to the queue. Process it to evaluate eligibility.",
    );
    setShowForm(false);
    navigate("measurements");
  }
  async function savePolicy(body: Record<string, number>) {
    await perform(
      "policy",
      () => api<Rule>("/rules", { method: "PUT", body: JSON.stringify(body) }),
      (r) =>
        `Policy version ${r.version} saved. Historical decisions are unchanged.`,
    );
  }
  async function reset() {
    const value = await perform(
      "reset",
      () => api("/demo/reset", { method: "POST" }),
      () => "Demo restored to its original synthetic seed.",
    ).catch(() => null);
    if (value) {
      setShowReset(false);
      setSelected(null);
      navigate("overview");
    }
  }
  const countries = useMemo(
    () => [...new Set(data?.schools.map((s) => s.country) || [])].sort(),
    [data],
  );
  const matches = (...values: (string | number)[]) =>
    values.join(" ").toLowerCase().includes(query.toLowerCase());
  const visibleSchools =
    data?.schools.filter(
      (s) =>
        (country === "all" || s.country === country) &&
        (status === "all" || s.status === status) &&
        matches(s.name, s.provider_name, s.region),
    ) || [];
  const visibleMeasurements =
    data?.measurements.filter(
      (m) =>
        (country === "all" || m.country === country) &&
        (status === "all" || m.status === status) &&
        (schoolFilter === "all" || m.school_id === schoolFilter) &&
        matches(m.school_name, m.provider_name, m.id, m.external_id),
    ) || [];
  const visibleCredits =
    data?.credits.filter(
      (c) =>
        (country === "all" || c.country === country) &&
        matches(c.school_name, c.provider_name, c.id, c.period),
    ) || [];
  const visibleEvents =
    data?.events.filter(
      (e) =>
        (status === "all" || e.status === status) &&
        matches(e.school_name, e.id, e.last_error || ""),
    ) || [];
  const measurement = data?.measurements.find((m) => m.id === selected);
  const transport = getTransportInfo(data?.health, !!loadError);
  const publicDemo = data?.health.public_demo === true;
  function exportCSV() {
    if (!data) return;
    const columns = [
      "credit_id",
      "school",
      "country",
      "provider",
      "period",
      "demo_credits",
      "measurement_id",
      "policy_version",
      "issued_at",
    ];
    const quote = (v: unknown) => `"${String(v).replaceAll('"', '""')}"`;
    const rows = visibleCredits.map((c) =>
      [
        c.id,
        c.school_name,
        c.country,
        c.provider_name,
        c.period,
        c.amount,
        c.measurement_id,
        c.rule_version,
        c.created_at,
      ]
        .map(quote)
        .join(","),
    );
    const blob = new Blob(
      ["\uFEFF" + [columns.join(","), ...rows].join("\r\n")],
      { type: "text/csv;charset=utf-8;" },
    );
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `connectivity-demo-credits-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    setToast({
      text: `Exported ${visibleCredits.length} ledger entries with the current filters.`,
    });
  }
  const subtitles: Record<Tab, string> = {
    overview: "A connected school. A measurable difference.",
    schools: "Every school, its service, and the signals that matter.",
    measurements: "Service evidence, evaluated against a shared standard.",
    ledger: "Every demo credit has a verifiable story.",
    events: "Follow each reading from receipt to a durable decision.",
    policy: "Define the service standards behind every demo credit.",
  };
  return (
    <div className="app-shell">
      <aside className={`sidebar ${mobileNav ? "nav-open" : ""}`}>
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            navigate("overview");
          }}
          aria-label="Connectivity Credits home"
        >
          <span className="brand-mark">
            <i />
            <i />
            <i />
            <i />
          </span>
          <span>
            connectivity
            <span>
              credits<span className="brand-period">.</span>
            </span>
          </span>
        </a>
        <div
          className={`workspace-switch ${transport.healthy ? "" : "transport-warning"}`}
        >
          <span className="workspace-avatar">CC</span>
          <div>
            <strong>Operations workspace</strong>
            <span title={transport.detail}>{transport.label}</span>
          </div>
          <span className="workspace-dot" />
        </div>
        <div className="nav-label">WORKSPACE</div>
        <nav aria-label="Main navigation">
          {(
            [
              { id: "overview", icon: "grid" },
              { id: "schools", icon: "school" },
              { id: "measurements", icon: "pulse" },
              { id: "ledger", icon: "coins" },
              { id: "events", icon: "list" },
              { id: "policy", icon: "sliders" },
            ] as const
          ).map((n) => (
            <button
              className={`nav-item ${tab === n.id ? "nav-active" : ""}`}
              aria-current={tab === n.id ? "page" : undefined}
              key={n.id}
              onClick={() => navigate(n.id)}
            >
              <Icon name={n.icon} size={19} />
              <span>{labels[n.id]}</span>
              {n.id === "events" && !!data?.overview.summary.failed_events && (
                <span className="nav-count">
                  {data.overview.summary.failed_events}
                </span>
              )}
              {n.id === "overview" && <span className="nav-active-dot" />}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <div className="sidebar-demo">
            <span className="demo-orbit">
              <Icon name="globe" size={25} />
            </span>
            <strong>
              Big possibilities.
              <br />A small{publicDemo ? ", shared" : ", local"} demo.
            </strong>
            <p>Explore how evidence can power accountable connectivity.</p>
            <span>NO REAL-WORLD TRANSFERS</span>
          </div>
          <button
            className="reset-link"
            onClick={() => setShowReset(true)}
            disabled={!!busy || !data}
          >
            <Icon name="refresh" size={16} />
            Reset demo data
          </button>
          <div className="sidebar-version">
            <span className="live-dot" /> Prototype v1.0 <span>{publicDemo ? "SHARED" : "LOCAL"}</span>
          </div>
        </div>
      </aside>
      {mobileNav && (
        <button
          className="nav-scrim"
          aria-label="Close navigation"
          onClick={() => setMobileNav(false)}
        />
      )}
      <div className="main-shell">
        <header className="topbar">
          <div className="breadcrumb">
            <button
              className="mobile-menu icon-button"
              aria-label="Open navigation"
              onClick={() => setMobileNav(!mobileNav)}
            >
              <Icon name="menu" />
            </button>
            <span className="breadcrumb-workspace">
              Workspace <Icon name="chevron" size={13} />
            </span>
            <span>{labels[tab]}</span>
          </div>
          <div className="topbar-right">
            <span className="prototype-label">
              <span className="live-dot" />
              Independent prototype <span className="label-divider">
                ·
              </span>{" "}
              Synthetic data
            </span>
            <span className="avatar">OP</span>
          </div>
        </header>
        <main>
          {publicDemo && (
            <div className="shared-demo-notice" role="note">
              <Icon name="info" size={19} />
              <div>
                <strong>Application work sample</strong>
                <p>Built by Ankit Pasayat for his application to UNICEF's Connectivity Credits backend consultancy. An independent prototype with simulated school readings and credits.</p>
                <p><a href="https://github.com/ankitpasayat/connectivity-credits-demo/blob/main/docs/prototype-overview.pdf" target="_blank" rel="noreferrer">Start here: illustrated demo guide (PDF)</a></p>
                <p>All data is synthetic. Changes and resets affect every visitor. Do not enter personal data.</p>
              </div>
            </div>
          )}
          <div className="page-heading">
            <div>
              <div className="eyebrow">CONNECTIVITY, ACCOUNTED FOR</div>
              <h1>{tab === "overview" ? "Network overview" : labels[tab]}</h1>
              <p>{subtitles[tab]}</p>
            </div>
            <div className="page-actions">
              <button
                className={`icon-button refresh-button ${refreshing ? "is-spinning" : ""}`}
                onClick={doRefresh}
                disabled={refreshing || loading || !!busy}
                aria-label="Refresh workspace"
                title={
                  lastSync
                    ? `Last refreshed ${lastSync.toLocaleTimeString()}`
                    : "Refresh workspace"
                }
              >
                <Icon name="refresh" size={18} />
              </button>
              {tab === "ledger" ? (
                <button
                  className="button secondary"
                  onClick={exportCSV}
                  disabled={!data || !visibleCredits.length}
                >
                  <Icon name="download" size={16} />
                  Export CSV
                </button>
              ) : tab === "events" ? (
                <button
                  className="button primary"
                  onClick={process}
                  disabled={!!busy || !data}
                >
                  <Icon name="play" size={15} />
                  {busy === "process" ? "Processing…" : "Process queue"}
                </button>
              ) : (
                tab !== "policy" && (
                  <button
                    className="button primary"
                    onClick={() => setShowForm(true)}
                    disabled={!data || !!busy}
                  >
                    <Icon name="plus" size={17} />
                    Add measurement
                  </button>
                )
              )}
            </div>
          </div>
          {loadError && (
            <div className="error-banner" role="alert">
              <Icon name="alert" size={20} />
              <div>
                <strong>Cannot reach the demo data service</strong>
                <p>
                  {loadError}{" "}
                  {data
                    ? "The last successful view is shown; refresh to get current data."
                    : "Please try again shortly."}
                </p>
              </div>
              <button
                className="button secondary"
                onClick={doRefresh}
                disabled={refreshing}
              >
                Try again
              </button>
            </div>
          )}
          {loading ? (
            <div className="loading-state" role="status">
              <span className="loading-spinner" />
              <h2>Connecting the dots…</h2>
              <p>Loading the operations workspace.</p>
            </div>
          ) : (
            data && (
              <>
                {tab === "overview" && (
                  <Overview
                    data={data}
                    transport={transport}
                    busy={busy}
                    onScenario={scenario}
                    onNavigate={navigate}
                    onCountry={(c) => {
                      navigate("schools");
                      setCountry(c);
                    }}
                    onProcess={process}
                  />
                )}
                {["schools", "measurements", "ledger", "events"].includes(
                  tab,
                ) && (
                  <>
                    <div className="table-summary">
                      {tab === "schools" && (
                        <>
                          <span>
                            <strong>{data.schools.length}</strong> fictional
                            schools
                          </span>
                          <span>
                            <i className="teal-dot" />
                            {
                              data.schools.filter(
                                (s) => s.status === "connected",
                              ).length
                            }{" "}
                            meeting policy
                          </span>
                          <span>{countries.length} countries</span>
                        </>
                      )}
                      {tab === "measurements" && (
                        <>
                          <span>
                            <strong>{data.measurements.length}</strong> readings
                            received
                          </span>
                          <span>
                            <i className="teal-dot" />
                            {
                              data.measurements.filter(
                                (m) => m.status === "eligible",
                              ).length
                            }{" "}
                            eligible
                          </span>
                          <span>
                            <i className="amber-dot" />
                            {
                              data.measurements.filter(
                                (m) => m.status === "pending",
                              ).length
                            }{" "}
                            pending evaluation
                          </span>
                        </>
                      )}
                      {tab === "ledger" && (
                        <>
                          <span>
                            <strong>
                              {format(
                                visibleCredits.reduce(
                                  (n, c) => n + c.amount,
                                  0,
                                ),
                              )}
                            </strong>{" "}
                            demo credits in view
                          </span>
                          <span>{visibleCredits.length} daily issuances</span>
                          <span>
                            <Icon name="shield" size={14} /> One per school-day
                          </span>
                        </>
                      )}
                      {tab === "events" && (
                        <>
                          <span>
                            <strong>
                              {
                                data.events.filter((e) => e.status === "queued")
                                  .length
                              }
                            </strong>{" "}
                            queued
                          </span>
                          <span>
                            <i className="amber-dot" />
                            {
                              data.events.filter((e) => e.status === "failed")
                                .length
                            }{" "}
                            failed
                          </span>
                          <span>
                            {
                              data.events.filter(
                                (e) => e.status === "processed",
                              ).length
                            }{" "}
                            processed
                          </span>
                        </>
                      )}
                    </div>
                    <section className="panel data-panel">
                      <div className="table-toolbar">
                        <label className="search-field">
                          <Icon name="search" size={18} />
                          <input
                            placeholder={
                              tab === "schools"
                                ? "Search school, provider or region…"
                                : tab === "events"
                                  ? "Search school, event or error…"
                                  : "Search school, provider or ID…"
                            }
                            aria-label={`Search ${labels[tab].toLowerCase()}`}
                            value={query}
                            onChange={(e) => setQuery(e.target.value)}
                          />
                          {query && (
                            <button
                              aria-label="Clear search"
                              onClick={() => setQuery("")}
                            >
                              <Icon name="close" size={14} />
                            </button>
                          )}
                        </label>
                        <div className="filters">
                          {tab !== "events" && (
                            <select
                              aria-label="Filter by country"
                              value={country}
                              onChange={(e) => {
                                setCountry(e.target.value);
                                setSchoolFilter("all");
                              }}
                            >
                              <option value="all">All countries</option>
                              {countries.map((c) => (
                                <option key={c}>{c}</option>
                              ))}
                            </select>
                          )}
                          {tab === "measurements" && (
                            <select
                              aria-label="Filter by school"
                              value={schoolFilter}
                              onChange={(e) => setSchoolFilter(e.target.value)}
                            >
                              <option value="all">All schools</option>
                              {data.schools
                                .filter(
                                  (s) =>
                                    country === "all" || s.country === country,
                                )
                                .map((s) => (
                                  <option key={s.id} value={s.id}>
                                    {s.name}
                                  </option>
                                ))}
                            </select>
                          )}
                          {tab !== "ledger" && (
                            <select
                              aria-label="Filter by status"
                              value={status}
                              onChange={(e) => setStatus(e.target.value)}
                            >
                              <option value="all">All statuses</option>
                              {(tab === "schools"
                                ? [
                                    "connected",
                                    "needs_attention",
                                    "awaiting_data",
                                  ]
                                : tab === "measurements"
                                  ? ["eligible", "ineligible", "pending"]
                                  : ["queued", "processed", "failed"]
                              ).map((s) => (
                                <option key={s} value={s}>
                                  {s === "connected"
                                    ? "Meeting policy"
                                    : s
                                        .replaceAll("_", " ")
                                        .replace(/^./, (c) => c.toUpperCase())}
                                </option>
                              ))}
                            </select>
                          )}
                        </div>
                      </div>
                      {tab === "schools" &&
                        (visibleSchools.length ? (
                          <div className="table-scroll">
                            <table>
                              <thead>
                                <tr>
                                  <th>School / region</th>
                                  <th>Country</th>
                                  <th>Service provider</th>
                                  <th>Latest download</th>
                                  <th>Uptime</th>
                                  <th>Status</th>
                                  <th>Demo credits</th>
                                  <th>
                                    <span className="sr-only">Actions</span>
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {visibleSchools.map((s) => (
                                  <tr key={s.id}>
                                    <td>
                                      <div className="school-cell">
                                        <span className="school-table-icon">
                                          <Icon name="school" size={17} />
                                        </span>
                                        <div>
                                          <strong>{s.name}</strong>
                                          <small>{s.region}</small>
                                        </div>
                                      </div>
                                    </td>
                                    <td>{s.country}</td>
                                    <td>{s.provider_name}</td>
                                    <td className="numeric">
                                      {s.latest_download_mbps === null
                                        ? "—"
                                        : `${format(s.latest_download_mbps)} Mbps`}
                                    </td>
                                    <td className="numeric">
                                      {s.latest_uptime_pct === null
                                        ? "—"
                                        : `${format(s.latest_uptime_pct)}%`}
                                    </td>
                                    <td>
                                      <Badge status={s.status} />
                                    </td>
                                    <td className="numeric credit-number">
                                      {format(s.credits_earned)}
                                    </td>
                                    <td>
                                      <button
                                        className="row-action"
                                        onClick={() => {
                                          navigate("measurements");
                                          setSchoolFilter(s.id);
                                        }}
                                        aria-label={`View ${s.name} measurements`}
                                      >
                                        <Icon name="arrow" size={17} />
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <Empty />
                        ))}
                      {tab === "measurements" &&
                        (visibleMeasurements.length ? (
                          <div className="table-scroll">
                            <table>
                              <thead>
                                <tr>
                                  <th>School / provider</th>
                                  <th>Service date</th>
                                  <th>Download</th>
                                  <th>Upload</th>
                                  <th>Latency</th>
                                  <th>Uptime</th>
                                  <th>Decision</th>
                                  <th>Credit</th>
                                  <th>
                                    <span className="sr-only">Details</span>
                                  </th>
                                </tr>
                              </thead>
                              <tbody>
                                {visibleMeasurements.map((m) => (
                                  <tr key={m.id}>
                                    <td>
                                      <button
                                        className="school-name-button"
                                        onClick={() => setSelected(m.id)}
                                      >
                                        {m.school_name}
                                      </button>
                                      <small>
                                        {m.country} · {m.provider_name}
                                      </small>
                                    </td>
                                    <td className="nowrap">
                                      {shortDate(m.period)}
                                      <small>{m.period.slice(0, 4)}</small>
                                    </td>
                                    <td className="numeric">
                                      {format(m.download_mbps)}{" "}
                                      <span className="unit">Mbps</span>
                                    </td>
                                    <td className="numeric">
                                      {format(m.upload_mbps)}{" "}
                                      <span className="unit">Mbps</span>
                                    </td>
                                    <td className="numeric">
                                      {format(m.latency_ms)}{" "}
                                      <span className="unit">ms</span>
                                    </td>
                                    <td className="numeric">
                                      {format(m.uptime_pct)}%
                                    </td>
                                    <td>
                                      <Badge status={m.status} />
                                    </td>
                                    <td>
                                      {m.credit_issued ? (
                                        <span className="issued-text">
                                          <Icon name="check" size={14} />
                                          Issued
                                        </span>
                                      ) : (
                                        <span className="muted">
                                          {m.status === "pending"
                                            ? "Pending"
                                            : "—"}
                                        </span>
                                      )}
                                    </td>
                                    <td>
                                      <button
                                        className="row-action"
                                        onClick={() => setSelected(m.id)}
                                        aria-label={`Inspect ${m.school_name} measurement on ${m.period}`}
                                      >
                                        <Icon name="chevron" size={17} />
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <Empty />
                        ))}
                      {tab === "ledger" &&
                        (visibleCredits.length ? (
                          <div className="table-scroll">
                            <table>
                              <thead>
                                <tr>
                                  <th>School / provider</th>
                                  <th>Country</th>
                                  <th>Service date</th>
                                  <th>Demo credits</th>
                                  <th>Policy</th>
                                  <th>Issued at</th>
                                  <th>Evidence</th>
                                </tr>
                              </thead>
                              <tbody>
                                {visibleCredits.map((c) => (
                                  <tr key={c.id}>
                                    <td>
                                      <strong>{c.school_name}</strong>
                                      <small>{c.provider_name}</small>
                                    </td>
                                    <td>{c.country}</td>
                                    <td className="nowrap">{date(c.period)}</td>
                                    <td className="numeric">
                                      <span className="ledger-amount">
                                        +{format(c.amount)}
                                      </span>
                                    </td>
                                    <td>
                                      <span className="version-chip">
                                        v{c.rule_version}
                                      </span>
                                    </td>
                                    <td className="nowrap">
                                      {shortDate(c.created_at)}
                                      <small>{time(c.created_at)}</small>
                                    </td>
                                    <td>
                                      <button
                                        className="text-button"
                                        onClick={() =>
                                          setSelected(c.measurement_id)
                                        }
                                      >
                                        View checks{" "}
                                        <Icon name="arrow" size={14} />
                                      </button>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <Empty
                            title="No credit issuances found"
                            text="Qualifying measurements receive one daily demo credit issuance per school."
                          />
                        ))}
                      {tab === "events" &&
                        (visibleEvents.length ? (
                          <div className="table-scroll">
                            <table>
                              <thead>
                                <tr>
                                  <th>Event / school</th>
                                  <th>Received</th>
                                  <th>Status</th>
                                  <th>Attempts</th>
                                  <th>Processing detail</th>
                                  <th>Action</th>
                                </tr>
                              </thead>
                              <tbody>
                                {visibleEvents.map((e) => (
                                  <tr
                                    key={e.id}
                                    className={
                                      e.status === "failed" ? "failed-row" : ""
                                    }
                                  >
                                    <td>
                                      <strong>{e.school_name}</strong>
                                      <small
                                        className="mono id-text"
                                        title={e.id}
                                      >
                                        {e.id}
                                      </small>
                                    </td>
                                    <td className="nowrap">
                                      {shortDate(e.created_at)}
                                      <small>{time(e.created_at)}</small>
                                    </td>
                                    <td>
                                      <Badge status={e.status} />
                                    </td>
                                    <td className="numeric">{e.attempts}</td>
                                    <td className="event-detail">
                                      {e.last_error ? (
                                        <span className="error-text">
                                          {e.last_error}
                                        </span>
                                      ) : e.status === "queued" ? (
                                        <span className="muted">
                                          Awaiting worker processing
                                        </span>
                                      ) : (
                                        <span>
                                          Decision and audit committed
                                        </span>
                                      )}
                                      <small>{e.type}</small>
                                    </td>
                                    <td>
                                      <div className="event-actions">
                                        {e.status === "failed" && (
                                          <button
                                            className="button retry-button"
                                            onClick={() => retry(e.id)}
                                            disabled={!!busy}
                                          >
                                            <Icon name="refresh" size={14} />
                                            {busy === e.id
                                              ? "Retrying…"
                                              : "Retry"}
                                          </button>
                                        )}
                                        <button
                                          className="row-action"
                                          onClick={() =>
                                            setSelected(e.measurement_id)
                                          }
                                          aria-label={`Inspect ${e.school_name} event measurement`}
                                        >
                                          <Icon name="chevron" size={17} />
                                        </button>
                                      </div>
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        ) : (
                          <Empty
                            title="No matching events"
                            text="Add a measurement or run a scenario to create queue activity."
                          />
                        ))}
                      <div className="table-footer">
                        <span>
                          {tab === "schools"
                            ? visibleSchools.length
                            : tab === "measurements"
                              ? visibleMeasurements.length
                              : tab === "ledger"
                                ? visibleCredits.length
                                : visibleEvents.length}{" "}
                          records shown
                        </span>
                        <span>
                          {tab === "ledger"
                            ? "Demo credits have no monetary value."
                            : tab === "events"
                              ? `${transport.label} · ${transport.mode === "local" ? "Durable event queue" : transport.mode === "kafka" ? "Kafka event transport" : "refresh to verify"}`
                              : "Fictional records · synthetic service data"}
                        </span>
                      </div>
                    </section>
                    {tab === "events" && (
                      <div className="info-callout event-info">
                        <Icon name="shield" size={22} />
                        <p>
                          <strong>Safe to retry.</strong> Credit creation,
                          eligibility decisions and audit completion commit
                          together. A failed transaction rolls back; retries
                          cannot issue a second school-day credit.
                        </p>
                      </div>
                    )}
                    {tab === "measurements" && (
                      <div className="info-callout event-info">
                        <Icon name="info" size={22} />
                        <p>
                          <strong>Pending is not issued.</strong> New
                          measurements wait for the worker. Process the queue in
                          Event log to apply policy checks and create eligible
                          daily demo credits.
                        </p>
                        <button
                          className="text-button"
                          onClick={() => navigate("events")}
                        >
                          Open event log
                          <Icon name="arrow" size={16} />
                        </button>
                      </div>
                    )}
                  </>
                )}
                {tab === "policy" && (
                  <Policy rule={data.rules} busy={!!busy} onSave={savePolicy} />
                )}
              </>
            )
          )}
          <footer className="app-footer">
            <span>
              Connectivity Credits <span>·</span> Independent demonstration
            </span>
            <span>
              Fictional data. Simulated verification. No financial or blockchain
              transactions.
            </span>
          </footer>
        </main>
      </div>
      {toast && (
        <div
          className={`toast ${toast.error ? "toast-error" : ""}`}
          role={toast.error ? "alert" : "status"}
        >
          <span className="toast-icon">
            <Icon name={toast.error ? "alert" : "check"} size={18} />
          </span>
          <p>{toast.text}</p>
          <button
            className="icon-button"
            aria-label="Dismiss notification"
            onClick={() => setToast(null)}
          >
            <Icon name="close" size={17} />
          </button>
        </div>
      )}
      {measurement && (
        <MeasurementDetail m={measurement} onClose={() => setSelected(null)} />
      )}
      {showForm && data && (
        <MeasurementForm
          data={data}
          busy={busy === "submit"}
          onClose={() => setShowForm(false)}
          onSubmit={submitMeasurement}
        />
      )}
      {showReset && (
        <Modal
          title="Restore the demo?"
          eyebrow="RESET SYNTHETIC DATA"
          onClose={() => {
            if (!busy) setShowReset(false);
          }}
        >
          <div className="reset-content">
            <span className="reset-symbol">
              <Icon name="refresh" size={28} />
            </span>
            <p>
              This clears measurements, demo credits, events and policy edits in
              the {publicDemo ? "shared" : "local"} demo database, then restores the original fictional
              seed.
            </p>
            <p>{publicDemo ? "Every visitor's current demo changes will be lost." : "Your current demo changes will be lost."}</p>
          </div>
          <div className="modal-footer">
            <button
              className="button secondary"
              onClick={() => setShowReset(false)}
              disabled={!!busy}
            >
              Keep current data
            </button>
            <button className="button danger" onClick={reset} disabled={!!busy}>
              {busy === "reset" ? "Restoring…" : "Reset demo data"}
            </button>
          </div>
        </Modal>
      )}
    </div>
  );
}
export default App;
