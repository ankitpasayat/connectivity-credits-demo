"""Actual PostgreSQL persistence, country-blind policy and safe legacy import."""

import os
from pathlib import Path
import sqlite3
import subprocess

from tests.node_server import NodeServer, node_binary, ROOT
from tests.pg_database import PgDatabase


def input_for(school, external_id, period):
    return {"source": "migration-qa", "external_id": external_id,
            "school_id": school["id"], "provider_id": school["provider_id"], "period": period,
            "download_mbps": 50, "upload_mbps": 20, "latency_ms": 20,
            "uptime_pct": 100, "verification_status": "verified"}


def snapshot(database):
    with database.connect() as connection:
        tables = [row[0] for row in connection.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE' ORDER BY table_name", (database.schema,))]
        return {table: connection.execute(f'SELECT * FROM "{table}" ORDER BY 1').fetchall() for table in tables}


def export_legacy_fixture(database, destination):
    """Construct an isolated SQLite backup fixture; active operations use PG."""
    with database.connect() as source, sqlite3.connect(destination) as legacy:
        tables = [row[0] for row in source.execute("SELECT table_name FROM information_schema.tables WHERE table_schema=%s AND table_type='BASE TABLE' ORDER BY table_name", (database.schema,))]
        for table in tables:
            columns = source.execute("SELECT column_name,data_type FROM information_schema.columns WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (database.schema, table)).fetchall()
            def legacy_type(data_type):
                if data_type in ("integer", "bigint", "smallint"):
                    return "INTEGER"
                if data_type in ("double precision", "real", "numeric"):
                    return "REAL"
                return "TEXT"
            declaration = ",".join(f'"{name}" {legacy_type(data_type)}' for name, data_type in columns)
            legacy.execute(f'CREATE TABLE "{table}" ({declaration})')
            rows = source.execute(f'SELECT * FROM "{table}" ORDER BY 1').fetchall()
            placeholders = ",".join("?" for _ in columns)
            legacy.executemany(f'INSERT INTO "{table}" VALUES({placeholders})', rows)


def test_country_groups_do_not_change_eligibility(settled):
    client = settled
    schools = client.get("/api/schools").json()
    selected = [next(school for school in schools if school["country"] == country) for country in ("Kenya", "Nepal", "Rwanda")]
    ids = []
    for index, school in enumerate(selected):
        response = client.post("/api/measurements", json=input_for(school, f"same-policy-{index}", "2020-06-01"))
        assert response.status_code == 201, response.text
        ids.append(response.json()["measurement"]["id"])
    response = client.post("/api/worker/run", json={"limit": 500})
    assert response.status_code == 200, response.text
    assert response.json()["issued"] == 3
    decisions = [client.get(f"/api/measurements/{measurement_id}").json() for measurement_id in ids]
    assert all(row["status"] == "eligible" and row["credit_issued"] for row in decisions)
    assert all(row["checks"] == decisions[0]["checks"] for row in decisions)
    assert {check["key"] for check in decisions[0]["checks"]} == {
        "download_mbps", "upload_mbps", "latency_ms", "uptime_pct", "verification_status"
    }


def test_transactional_sqlite_import_preserves_all_rows_and_refuses_overwrite(tmp_path):
    with PgDatabase() as source:
        with NodeServer(source) as client:
            school = client.get("/api/schools").json()[0]
            first = client.post("/api/measurements", json=input_for(school, "keep-old-policy-credit", "2020-06-02"))
            assert first.status_code == 201, first.text
            assert client.post("/api/worker/run", json={"limit": 500}).status_code == 200
            rule = client.get("/api/rules").json()
            update = {key: value for key, value in rule.items() if key not in ("version", "updated_at")}
            update["credits_per_period"] = 17
            assert client.put("/api/rules", json=update).status_code == 200
            second = client.post("/api/measurements", json=input_for(school, "keep-new-policy-credit", "2020-06-03"))
            assert second.status_code == 201, second.text
            assert client.post("/api/worker/run", json={"limit": 500}).status_code == 200
            assert client.post("/api/measurements", json=input_for(school, "keep-pending", "2020-06-04")).status_code == 201
            assert client.post("/api/demo/scenarios/worker_failure").status_code == 200
            summary = client.get("/api/overview").json()["summary"]
        with source.connect() as connection:
            # A migration receipt fixture, not an assertion about broker I/O.
            delivery_id, event_id = connection.execute("SELECT o.id,o.event_id FROM broker_outbox o JOIN events e ON e.id=o.event_id WHERE e.measurement_id=%s", (first.json()["measurement"]["id"],)).fetchone()
            connection.execute("UPDATE broker_outbox SET published_at='2020-06-04T01:00:00Z',consumed_at='2020-06-04T01:00:01Z',broker_partition=0,broker_offset=42 WHERE id=%s", (delivery_id,))
            connection.execute("INSERT INTO broker_inbox(delivery_id,event_id,topic,broker_partition,broker_offset,received_at) VALUES(%s,%s,%s,0,42,%s)", (delivery_id, event_id, "migration-fixture", "2020-06-04T01:00:01Z"))
        before = snapshot(source)
        backup = tmp_path / "legacy-backup.sqlite3"
        export_legacy_fixture(source, backup)
    with PgDatabase() as target:
        command = [node_binary(), str(ROOT / "server/scripts/import-sqlite.mjs"), "--sqlite-path", str(backup)]
        environment = {**os.environ, **target.environment}
        for arguments in ([], ["--verify-only"]):
            result = subprocess.run(command + arguments, cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30)
            assert result.returncode == 0, result.stderr
        assert snapshot(target) == before
        for _ in range(2):
            with NodeServer(target) as client:
                assert client.get("/api/overview").json()["summary"] == summary
                assert client.get("/api/rules").json()["version"] == 2
            assert snapshot(target) == before
        with target.connect() as connection:
            connection.execute("UPDATE metadata SET value='custom-user-work' WHERE key='seed_version'")
        changed = snapshot(target)
        refused = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True, timeout=30)
        assert refused.returncode != 0
        assert snapshot(target) == changed, "A differing nonempty PostgreSQL target must never be overwritten"
