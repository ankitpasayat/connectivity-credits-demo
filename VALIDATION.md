# Validation record

Validated locally on **30 October 2026**. All records/credits are synthetic demonstration data. Latest screenshots and CSV use **Kenya, Nepal and Rwanda** example country labels, with fictional **School A01–C04** and **Provider A/B/C** records. No deployment or measured national performance is represented.

**Current implementation: Node.js / TypeScript / Express, PostgreSQL, React / TypeScript / Vite and actual Kafka.** Earlier development implementations are excluded. The counts below describe the current TypeScript application and PostgreSQL baseline.

## Launch and build

- Runtime discovery selects **Node.js 24.19.0**, skipping the incompatible system Node 18 default. The server/import tooling requires Node **22.16+**; React **18.3.1** and Vite **6.4.5** remain the client stack. Both TypeScript builds pass.
- After the live import, `start-kafka.sh` launched **Express on localhost:3000** and **Vite on localhost:5173**. Health through the actual Vite proxy reports `database_engine:'postgresql', runtime:'node', framework:'express', transport:'kafka', broker:'ok'`. The built UI is also served by Express.
- Native PostgreSQL **16.2** uses the installed arm64-compatible binaries in a task-owned cluster on `127.0.0.1:5433`. SCRAM authentication and generated 0600 credential files are used; the application role is **not a superuser**. Cluster/state directories are 0700. Other PostgreSQL instances are not managed by the demo.
- Lifecycle helpers verify owned PID/data paths before signaling; the launcher retains pre-existing services. Application-child SIGTERM supervision was checked during launcher migration. The launcher manages only its own application processes.
- PostgreSQL persists across fresh Node processes; independent restart/import regressions verify that startup does not reseed over existing work.
- The workspace displays **Kafka connected**, verified against live broker health rather than a static label.
- The frontend owner also checked `npm audit`: zero reported vulnerabilities in the final lockfile at validation time.

## Automated API and business rules

The PostgreSQL/Kafka baseline passed **12/12 native tests**, with no skips. It includes concurrent ingestion, policy versions, rollback, snapshots, earlier numeric JSON replay, transactional import/verification/refusal, invalid-FK rollback, actual Kafka records/offsets and worker-process death/advisory-lock recovery. These run against `server/`. This broader baseline predates the latest country-label change and is preserved as historical evidence.

The independent HTTP/PG/Kafka suite starts real Node subprocesses with disposable `qa_*` PostgreSQL schemas. Command:

```bash
RUN_KAFKA_TESTS=1 \
CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:19092 \
.venv/bin/python -m pytest tests -q \
  --junitxml=artifacts/postgres/pytest.xml
```

Baseline result: **40 independent PostgreSQL/Node HTTP/Kafka tests passed**, zero skipped/failed. [Original JUnit evidence](artifacts/postgres/pytest.xml) is unchanged. Python is only independent QA tooling; no Python app backend is loaded.

For the subsequent country-only label restoration, the backend owner reported **14 passing real-PostgreSQL checks**. QA reran the two focused country/API checks (seed aggregation and identical policy outcomes across Kenya/Nepal/Rwanda) plus the browser flows below. See [focused country JUnit evidence](artifacts/postgres/country-checks.xml). The broader 52-check baseline was not rerun for this label-only change.

The independent suite and native Node suite verify:

- Identical delivery replay, HTTP 409 on a conflicting payload, and concurrent replay without extra measurements or events.
- A separate real Kafka consumer reads the envelope at the exact persisted broker offset. An independent real producer injects a raw duplicate into the broker log; the application retains one inbox receipt, one decision and one credit for the original event, and its committed offset advances past the duplicate.
- A refused broker connection returns HTTP 503, preserves queued/outbox work and creates no credit. Restoring the real broker connection processes that pending work once. Kafka mode never silently uses the local worker path.
- One issuance per school/date across distinct events, policy versions and concurrent worker attempts; SQL uniqueness constraints are present.
- Failure after credit insertion leaves no credit, decision or completion audit; retry commits once, and repeated retry cannot issue again.
- Historical policy versions, check results and credit amounts remain unchanged after a policy edit.
- School/provider association, date and numeric validation, including nonfinite JSON numbers and out-of-range policy values.
- Seeded overview, school, country, trend and ledger totals agree with the underlying records.
- Identical metric/verification values across country groups produce identical checks and outcomes; country is display/filter metadata only.
- An independent SQLite-backup import fixture preserves all 13 application tables exactly in actual PostgreSQL, including credits under two policy versions, pending/failed work and broker receipts. Repeat verification and two Node startups preserve every row. A differing nonempty PostgreSQL target is refused without changes.
- All four scenarios can repeat, reset restores the deterministic seed, data survives a process restart, and missing API routes remain JSON 404 responses while valid frontend routes serve the SPA.

Tests use isolated PostgreSQL schemas and never reset the review `public` schema. The running API uses async `pg` pools; SQLite is only used by the explicit one-off read-only importer. That importer may emit Node's upstream experimental `node:sqlite` warning; it is not part of application request handling.

## Data preservation

The local development dataset was migrated from SQLite to PostgreSQL with exact table fingerprints checked before the later, intentional display-country update. Country restoration preserved all ten business and transport table hashes. Local backups and machine-specific migration reports are excluded from this repository.

## Actual Kafka runtime

The task-local broker is **Apache Kafka 4.4.0**, running single-node KRaft with **Temurin JRE 21.0.13+9** on arm64 macOS. The bootstrap verified the official Apache SHA512 and Adoptium SHA256 before extracting or executing either archive. The artifacts total approximately 183 MB; downloaded runtimes and broker files stay under the ignored `.data` directory and are excluded from the Docker build context.

The [verified download manifest](artifacts/kafka/runtime-downloads.json) records source/checksum URLs, versions, byte sizes and hashes. The pinned Apache archive URL was additionally checked to return HTTP 200 and the identical SHA512, ensuring a stable source when current-release mirrors rotate. Sources: [Apache 4.4.0 archive](https://archive.apache.org/dist/kafka/4.4.0/), [Java support](https://kafka.apache.org/44/operations/java-version/) and [official Temurin release](https://github.com/adoptium/temurin21-binaries/releases/tag/jdk-21.0.13%2B9).

The broker listens at `127.0.0.1:19092`; its controller is localhost-only. Actual Kafka administration reported the demo topic with **one partition, replication factor one and an in-sync leader**. The active backend uses `@confluentinc/kafka-javascript` **1.10.1** native Node clients. Historical seed decisions are precomputed; queued/new Kafka-mode readings and retry/replay deliveries use the broker. The Node worker entry is `node server/dist/worker.js`.

`./scripts/kafka-local.sh stop` targets only the recorded task-owned broker after checking its command line. It retains the task-local runtime and data caches. No Java package, Homebrew tool, Docker daemon or system service was installed.

## Browser checks and screenshots

Command: `BROWSER_TRANSPORT=kafka ./scripts/browser.sh` — **all nine check groups passed against actual Kafka**, with **zero page errors, console errors or failed resource responses**. The test asserts that the live health response is `transport=kafka, broker=ok` and that **Kafka connected** is visible. See the machine-readable [Kafka browser report](artifacts/browser-kafka/report.json).

The harness ran its own **Node/Express process** on port 8766 with a disposable PostgreSQL schema. It asserts `database_engine=postgresql, runtime=node, framework=express`, and independently queried server version16.2 plus the non-superuser app role. It used Playwright **1.62.1**, Node.js **24.19.0** and Chrome **156.0.8078.79**. Its process/schema were removed at completion.

Actual browser controls were exercised for:

1. Seeded overview, live transport status and seven-day chart selection.
2. Country/search filters and the empty school state.
3. Measurement form submission, pending queue state, worker processing, issuance and all five detail checks.
4. Injected worker failure, zero new credits, and a successful UI retry creating one issuance.
5. Policy editing, preservation of a prior decision, and a clean batch evaluated against the new version.
6. Filtered ledger CSV download, with downloaded rows matching the visible country filter.
7. Reset cancellation and confirmed restoration of the seed/default policy.
8. Mobile navigation, contained horizontal table scrolling and a fully visible measurement dialog.
9. Browser page/console/resource errors.

Desktop checks used **1440 × 1000**. Mobile checks used a configured **390 × 844** viewport and assert against that configured width, as well as dialog bounds. The final mobile layout width, visual width and document width are all 390 px. The form spans x=14 to x=376 and y≈65 to y≈779, fully inside the viewport.

Visual review identified and fixed an absolute screen-reader table label that escaped the scroll container and widened the mobile layout viewport. The final screenshots were regenerated after the containment fix. A missing favicon was also fixed; the final console/resource report is clean.

| View | Evidence |
| --- | --- |
| Desktop overview / Kafka status | [Screenshot](artifacts/browser-kafka/desktop-overview.png) |
| Brief overview for the explainer PDF | [1440 × 700 screenshot](artifacts/browser-kafka/desktop-overview-brief.png) |
| Issued measurement and policy checks | [Screenshot](artifacts/browser-kafka/desktop-measurement-checks.png) |
| Clean read-only measurement capture for the PDF | [Screenshot](artifacts/browser-kafka/desktop-measurement-checks-clean.png) |
| Failed worker event and retry action | [Screenshot](artifacts/browser-kafka/desktop-worker-failure.png) |
| Mobile overview | [Screenshot](artifacts/browser-kafka/mobile-overview.png) |
| Mobile school table | [Screenshot](artifacts/browser-kafka/mobile-schools.png) |
| Mobile measurement form | [Screenshot](artifacts/browser-kafka/mobile-form.png) |
| Filtered CSV export | [CSV](artifacts/browser-kafka/connectivity-demo-credits-2026-10-30.csv) |

The latest browser evidence was regenerated against Node/PostgreSQL with Kenya/Nepal/Rwanda example labels and synthetic records. It did not reset/modify the review schema. The prior PostgreSQL neutral-label screenshots/report are preserved unchanged in `.data/evidence-country-neutral-postgres`; older engine captures remain in their `.data` archives. Broker-free mode remains explicitly labeled **Local queue active** and still uses PostgreSQL.

The brief overview and clean measurement capture were taken read-only from the current Vite UI on port5173 for the PDF. They performed no mutation or service restart; their temporary browser processes closed at completion.

## Configuration delivered but not executed here

- Multistage Node24 `Dockerfile` and Compose for **Express + PostgreSQL + Kafka**, private generated credentials, non-superuser app role, persistent volumes, health dependencies and optional worker. Express uses `HOST=0.0.0.0` inside its container; host ports stay localhost-only.
- GitHub Actions configuration includes actual PostgreSQL services, Node/frontend builds, independent HTTP/PG tests, Compose/image validation and real Kafka integration.

These files target the active `server/` implementation and were inspected locally. **No Docker image build/container run or remote GitHub Actions execution is claimed**: no Docker-compatible runtime is installed here. The actual broker tests above used checksum-verified Apache/Temurin archives instead. See the [current stack and scope](README.md#current-stack-and-scope).

## Scope of evidence

These checks demonstrate a local Node/Express/PostgreSQL prototype with real Kafka. They do not establish externally verified connectivity, real school impact, settlement, distributed exactly-once effects, production security or capacity. The consumer uses one partition/manual assignment and PostgreSQL session advisory locks. Verification is simulated and credits have no monetary value. No hosted deployment, external account integration or transfer was performed. There is no UNICEF/Giga affiliation or approval.
