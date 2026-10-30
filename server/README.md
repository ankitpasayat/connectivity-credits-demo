# TypeScript / Node / Express / PostgreSQL backend

This is the active backend implementation. It has no Python runtime dependency.
The React/Vite client
uses the same JSON API, with the Express API on port 3000 by default.

Use Node **22.16 or newer**; development and real PostgreSQL/Kafka tests were run
with Node 24.19.0 and PostgreSQL 16.2. The active runtime uses async `pg` pools.
SQLite is used only by the explicit one-off import tool and migration fixtures.

From this directory:

```sh
npm ci
npm run build
# Set DATABASE_URL privately; the root launcher can load the task-local env file.
npm start
```

`npm run dev` watches TypeScript directly. `HOST` defaults to `127.0.0.1` and `PORT`
to `3000`. Containers can set `HOST=0.0.0.0`. `PORT=0` selects an ephemeral port
for isolated HTTP tests. The server can serve `frontend/dist` when it is built.
Unknown API paths remain JSON 404 responses.

The health endpoint reports `runtime: "node"`, `framework: "express"`, and
`database_engine: "postgresql"`, along
with the existing database, demo mode, transport and verified broker-health
fields. The fictional records use user-selected Kenya, Rwanda and Nepal display
labels and generic school/provider names. These labels are not deployment or
country-level findings. Neither country nor region enters the eligibility function.

| Environment variable | Default |
| --- | --- |
| `DATABASE_URL` | `postgresql://127.0.0.1:5433/connectivity_credits` |
| `CONNECTIVITY_DATABASE_URL` | Alias when `DATABASE_URL` is unset |
| `CONNECTIVITY_DB_SCHEMA` | `public` |
| `CONNECTIVITY_TRANSPORT` | `local` |
| `CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS` | `127.0.0.1:19092` |
| `CONNECTIVITY_KAFKA_TOPIC` | `connectivity-measurements-v1` |
| `CONNECTIVITY_KAFKA_GROUP_ID` | `connectivity-credits-demo` |
| `CONNECTIVITY_KAFKA_TIMEOUT_SECONDS` | `12` |

To use actual Kafka:

```sh
CONNECTIVITY_TRANSPORT=kafka npm start
```

The pinned `@confluentinc/kafka-javascript` client uses real native librdkafka
producers and consumers. This implementation uses the supported callback API
because it exposes explicit partition assignment and bounded consumption; it
does not depend on unsupported KafkaJS manual-assignment internals. Producer
idempotence is enabled and `acks=-1` means all in-sync replicas acknowledge.
This demo creates one partition with replication factor one.

Ingestion atomically persists the measurement, queue event and outbox record.
A worker action relays pending outbox records to Kafka, persists the actual
delivery partition/offset, consumes the real broker record, and stores a durable
inbox receipt. Only that event's required delivery authorizes evaluation. The
credit, immutable decision, measurement projection and completion audit commit
in one PostgreSQL transaction before its Kafka offset is committed explicitly.
Automatic offset commits and automatic offset storage are both disabled.

This gives at-least-once delivery with idempotent SQL effects, rather than a
distributed transaction between Kafka and PostgreSQL. Replaying a broker record
cannot create another decision or school/day credit. A simulated failure throws
after credit INSERT and before transaction commit; the business transaction is
rolled back. Separate durable failure bookkeeping precedes offset acknowledgement.
Explicit Retry creates a fresh Kafka delivery, including already-processed
retries, whose business effects remain unchanged. Duplicate-delivery scenarios
send three actual records while retaining one measurement/event/credit.

Scenario actions process only their own events. Other received records are
durably deferred in the inbox before offsets advance. The imported outbox/inbox,
IDs and persisted namespace retain their original content.
Startup never resets an existing database or reevaluates old decisions. Existing
Python JSON is compared semantically, so numeric formatting such as `1.0` and
`1`, whitespace and key order do not turn a valid replay into a conflict. The
neutral-label migration preserves all business data and removes unused
coordinates. Its original v2 fixtures are frozen. A later country-only startup
migration restores the selected country labels on known fixture IDs while
preserving generic names, regions and every business/transport record. It updates
only exact former `Demo country A/B/C` values, then records its own version;
custom country labels remain unchanged. Reset is a separate explicit endpoint and changes the Kafka
namespace/group suffix so older topic records cannot resurrect deleted work.

The worker holds a PostgreSQL session advisory lock on a dedicated checked-out
connection. Nested scenario processing shares it; competing processes receive a
useful conflict. PostgreSQL releases the lock if the process or connection dies.
The connection is explicitly unlocked before returning to the pool, and a failed
unlock destroys that connection. The old `runtime_locks` table is retained only
for exact import compatibility and does not control PostgreSQL workers.

Each business transaction checks out one pool client and binds every query in
that async context to it. `BEGIN`, mutations and `COMMIT`/`ROLLBACK` never switch
pool clients. Concurrent measurement identity races use SQL `ON CONFLICT` and
semantic payload comparison; policy version allocation uses a transaction
advisory lock. Event rows are locked while evaluating. Float columns use
`DOUBLE PRECISION`; counts and credit sums return safe JSON integers.

For an existing SQLite database, stop the previous API/worker before capturing
and importing it. The new PostgreSQL target schema must be empty. Do this before
starting the new API, which otherwise seeds an empty database:

```sh
# DATABASE_URL and optional CONNECTIVITY_DB_SCHEMA select the PostgreSQL target.
node scripts/import-sqlite.mjs --sqlite-path /absolute/source.sqlite3
node scripts/import-sqlite.mjs --sqlite-path /absolute/source.sqlite3 --verify-only
```

The tool takes a consistent WAL-aware backup through a read-only source
connection and copies all 13 application tables in one PostgreSQL transaction.
IDs, timestamps, raw JSON payload/snapshot strings, decisions, credits, policy
history, activity, queue/outbox/inbox and namespace remain exact. It verifies
content hashes and counts before committing. Re-running an identical import is
safe; a differing nonempty target is refused. Constraint or verification failure
rolls back every inserted row. There is no overwrite/reset flag. Keep the source
backup after handoff. Verify an older neutral-country source before first startup:
the separately versioned country migration intentionally updates its reference
labels during startup, while the import verifier remains exact and unchanged.
The old runtime and PostgreSQL runtime must not process the
same Kafka namespace concurrently.

Optional service worker:

```sh
CONNECTIVITY_TRANSPORT=kafka npm run worker
CONNECTIVITY_TRANSPORT=kafka npm run worker -- --once
```

The Process queue button uses the same bounded worker path. A broker outage
returns HTTP 503 and preserves durable work; there is no silent local fallback.
A transport error can happen after a successful database commit but before the
offset commit; retry remains safe. This single-partition/manual-assignment demo
needs coordinated assignment, replicated infrastructure, security, retention
and operational recovery planning before a distributed deployment.

Tests run a real Express HTTP server and real PostgreSQL schemas. Set
`DATABASE_URL` to a reachable test server; each test creates and removes its own
random schema. The public schema and existing records are not reset:

```sh
npm test
RUN_KAFKA_TESTS=1 npm test
```

The opt-in Kafka tests require the actual broker and independently consume its
wire record, inspect the committed group offset, inject a raw duplicate, and
verify rollback/retry, three replay deliveries, all scenarios, reset isolation
and outage recovery. No producer or consumer is mocked. Other tests cover JSON
validation, overlapping duplicate/conflicting delivery, concurrent policy version
allocation, immutable snapshots, Python-formatted replay, exact transactional
SQLite import/refusal/rollback, country-blind checks and advisory-lock recovery
after an owning process is killed.

References: [node-postgres transactions](https://node-postgres.com/features/transactions),
[pooling](https://node-postgres.com/features/pooling),
[PostgreSQL advisory locks](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS),
[official Confluent client overview](https://docs.confluent.io/kafka-clients/javascript/current/overview.html),
and [native consumer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-javascript/docs/RdKafka.KafkaConsumer.html).
