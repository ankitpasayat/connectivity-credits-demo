"""Independent behavior tests: observable invariants, not implementation mirrors."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
from pathlib import Path

import pytest

from tests.node_server import NodeServer
from tests.pg_database import PgDatabase


ROOT = Path(__file__).resolve().parents[1]


def get(client, path):
    response = client.get(path)
    assert response.status_code == 200, response.text
    return response.json()


def payload(client, external_id="qa-reading", period="2020-01-01", **changes):
    school = get(client, "/api/schools")[0]
    rule = get(client, "/api/rules")
    result = {
        "source": "independent-qa",
        "external_id": external_id,
        "school_id": school["id"],
        "provider_id": school["provider_id"],
        "period": period,
        "download_mbps": rule["min_download_mbps"] + 10,
        "upload_mbps": rule["min_upload_mbps"] + 5,
        "latency_ms": max(0, rule["max_latency_ms"] - 10),
        "uptime_pct": 100,
        "verification_status": "verified",
    }
    result.update(changes)
    return result


def submit(client, reading):
    response = client.post("/api/measurements", json=reading)
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["duplicate"] is False
    return body["measurement"]


def run_worker(client, **changes):
    response = client.post("/api/worker/run", json={"limit": 500, **changes})
    assert response.status_code == 200, response.text
    return response.json()


def counts(client):
    return {
        "measurements": len(get(client, "/api/measurements")),
        "events": len(get(client, "/api/events")),
        "credits": len(get(client, "/api/credits")),
    }


def measurement(client, measurement_id):
    return get(client, f"/api/measurements/{measurement_id}")


def event_for(client, measurement_id):
    matches = [event for event in get(client, "/api/events") if event["measurement_id"] == measurement_id]
    assert len(matches) == 1
    return matches[0]


def test_health_and_missing_api_routes_are_json(client):
    health = get(client, "/api/health")
    assert {key: health[key] for key in ("status", "mode", "database")} == {"status": "ok", "mode": "demo", "database": "ok"}
    assert health["transport"] == "local"
    assert health["broker"] == "not_used"
    assert health["runtime"] == "node" and health["framework"] == "express"
    assert health["database_engine"] == "postgresql"
    for path in ("/api/not-a-route", "/api/measurements/missing", "/api/events/missing/retry"):
        response = client.post(path) if path.endswith("retry") else client.get(path)
        assert response.status_code == 404, response.text
        assert response.headers["content-type"].startswith("application/json")
        assert "detail" in response.json()
    response = client.post("/api/not-a-route", json={})
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")


def test_frontend_routes_and_assets_are_served_without_catching_api(client):
    if not (ROOT / "frontend/dist/index.html").exists():
        pytest.skip("Build frontend first to verify SPA serving")
    for path in ("/", "/schools", "/measurements"):
        response = client.get(path)
        assert response.status_code == 200, response.text
        assert "text/html" in response.headers["content-type"]
        assert '<div id="root">' in response.text
    response = client.get("/api/unknown/deep/route")
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/json")


def test_seed_aggregations_match_actual_records(client):
    overview = get(client, "/api/overview")
    summary = overview["summary"]
    schools = get(client, "/api/schools")
    readings = get(client, "/api/measurements")
    credits = get(client, "/api/credits")
    events = get(client, "/api/events")
    readings_by_id = {row["id"]: row for row in readings}
    eligible = [row for row in readings if row["status"] == "eligible"]
    evaluated = [row for row in readings if row["status"] != "pending"]

    assert len(schools) == 12
    assert len(get(client, "/api/providers")) == 3
    assert {row["country"] for row in schools} == {"Kenya", "Nepal", "Rwanda"}
    assert {row["name"] for row in schools} == {f"School {group}{number:02d}" for group in "ABC" for number in range(1, 5)}
    assert {row["name"] for row in get(client, "/api/providers")} == {"Provider A", "Provider B", "Provider C"}
    assert all("latitude" not in row and "longitude" not in row for row in schools)
    assert summary["schools_total"] == len(schools)
    assert summary["schools_connected"] == sum(row["status"] == "connected" for row in schools)
    assert summary["total_measurements"] == len(readings)
    assert summary["eligible_measurements"] == len(eligible)
    assert summary["credits_issued"] == sum(row["amount"] for row in credits)
    assert summary["pass_rate"] == pytest.approx(100 * len(eligible) / len(evaluated), abs=0.11)
    assert summary["queued_events"] == sum(row["status"] == "queued" for row in events)
    assert summary["failed_events"] == sum(row["status"] == "failed" for row in events)
    assert len({(row["school_id"], row["period"]) for row in credits}) == len(credits)

    issued_measurements = {row["measurement_id"] for row in credits}
    for credit in credits:
        linked = readings_by_id[credit["measurement_id"]]
        assert linked["status"] == "eligible"
        assert linked["credit_issued"] is True
        assert linked["school_id"] == credit["school_id"]
        assert linked["period"] == credit["period"]
        assert linked["rule_version"] == credit["rule_version"]
        assert all(check["passed"] for check in linked["checks"])
    for row in readings:
        assert row["credit_issued"] == (row["id"] in issued_measurements)
        if row["status"] == "pending":
            assert row["evaluated_at"] is None
            assert row["rule_version"] is None
        else:
            assert row["checks"]
            assert row["evaluated_at"]
            assert (all(check["passed"] for check in row["checks"])) == (row["status"] == "eligible")
    for school in schools:
        assert school["credits_earned"] == sum(row["amount"] for row in credits if row["school_id"] == school["id"])

    assert len(overview["trends"]) == 14
    assert sum(row["credits"] for row in overview["trends"]) == summary["credits_issued"]
    assert sum(row["measurements"] for row in overview["trends"]) == len(readings)
    for row in overview["country_breakdown"]:
        country_readings = [item for item in evaluated if item["country"] == row["country"]]
        assert row["schools"] == sum(item["country"] == row["country"] for item in schools)
        assert row["credits"] == sum(item["amount"] for item in credits if item["country"] == row["country"])
        assert row["pass_rate"] == pytest.approx(
            100 * sum(item["status"] == "eligible" for item in country_readings) / len(country_readings), abs=0.11
        )


def test_identical_delivery_replay_creates_no_extra_work(settled):
    client = settled
    before = counts(client)
    reading = payload(client)
    created = submit(client, reading)
    assert created["status"] == "pending"
    for _ in range(2):
        response = client.post("/api/measurements", json=reading)
        assert response.status_code == 200
        assert response.json()["duplicate"] is True
        assert response.json()["measurement"]["id"] == created["id"]
    assert counts(client) == {**before, "measurements": before["measurements"] + 1, "events": before["events"] + 1}
    result = run_worker(client)
    assert result == {"processed": 1, "issued": 1, "rejected": 0, "failed": 0}
    after = counts(client)
    response = client.post("/api/measurements", json=reading)
    assert response.status_code == 200
    assert response.json()["measurement"]["credit_issued"] is True
    assert counts(client) == after
    assert run_worker(client) == {"processed": 0, "issued": 0, "rejected": 0, "failed": 0}


def test_conflicting_replay_is_409_and_does_not_mutate_existing(settled):
    client = settled
    reading = payload(client)
    created = submit(client, reading)
    before = counts(client)
    response = client.post("/api/measurements", json={**reading, "download_mbps": reading["download_mbps"] + 1})
    assert response.status_code == 409, response.text
    assert counts(client) == before
    assert measurement(client, created["id"]) == created


def test_same_school_day_is_issued_once_across_events_and_policy_versions(settled):
    client = settled
    before = counts(client)["credits"]
    first = submit(client, payload(client, external_id="first-event"))
    second = submit(client, payload(client, external_id="second-event"))
    assert run_worker(client)["issued"] == 1
    old_rule = get(client, "/api/rules")
    new_rule = {key: value for key, value in old_rule.items() if key not in ("version", "updated_at")}
    new_rule["credits_per_period"] = 25
    response = client.put("/api/rules", json=new_rule)
    assert response.status_code == 200, response.text
    third = submit(client, payload(client, external_id="third-event"))
    assert run_worker(client)["issued"] == 0
    assert counts(client)["credits"] == before + 1
    decisions = [measurement(client, row["id"]) for row in (first, second, third)]
    assert all(row["status"] == "eligible" for row in decisions)
    assert sum(row["credit_issued"] for row in decisions) == 1
    assert decisions[-1]["rule_version"] == response.json()["version"]


def test_failed_credit_transaction_rolls_back_and_retry_is_repeatable(settled, isolated_app):
    client = settled
    created = submit(client, payload(client, external_id="rollback-proof"))
    before = counts(client)
    before_total = get(client, "/api/overview")["summary"]["credits_issued"]
    _, database = isolated_app
    with database.connect() as connection:
        audit_before = connection.execute("SELECT COUNT(*) FROM activity WHERE type IN ('credit.issued','measurement.rejected','measurement.evaluated')").fetchone()[0]
    outcome = run_worker(client, simulate_failure=True)
    assert outcome == {"processed": 0, "issued": 0, "rejected": 0, "failed": 1}
    failed = event_for(client, created["id"])
    assert failed["status"] == "failed"
    assert failed["attempts"] == 1
    assert failed["last_error"]
    pending = measurement(client, created["id"])
    assert pending["status"] == "pending"
    assert pending["credit_issued"] is False
    assert pending["checks"] == []
    assert pending["rule_version"] is None
    assert pending["evaluated_at"] is None
    assert counts(client) == before
    assert get(client, "/api/overview")["summary"]["credits_issued"] == before_total
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM decisions WHERE measurement_id=%s", (created["id"],)).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM credits WHERE measurement_id=%s", (created["id"],)).fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM activity WHERE type IN ('credit.issued','measurement.rejected','measurement.evaluated')").fetchone()[0] == audit_before

    retry = client.post(f'/api/events/{failed["id"]}/retry')
    assert retry.status_code == 200, retry.text
    assert retry.json()["status"] == "processed"
    assert retry.json()["attempts"] == 2
    assert retry.json()["last_error"] is None
    assert measurement(client, created["id"])["credit_issued"] is True
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM decisions WHERE measurement_id=%s", (created["id"],)).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM credits WHERE measurement_id=%s", (created["id"],)).fetchone()[0] == 1
    after = counts(client)
    assert after["credits"] == before["credits"] + 1
    for _ in range(3):
        again = client.post(f'/api/events/{failed["id"]}/retry')
        assert again.status_code == 200
        assert again.json()["status"] == "processed"
        assert counts(client) == after
    assert run_worker(client)["processed"] == 0


def test_rule_update_preserves_historical_decision_checks_and_amount(settled):
    client = settled
    first = submit(client, payload(client, external_id="old-policy"))
    run_worker(client)
    saved_decision = measurement(client, first["id"])
    saved_credit = next(row for row in get(client, "/api/credits") if row["measurement_id"] == first["id"])
    rule = get(client, "/api/rules")
    updated = {key: value for key, value in rule.items() if key not in ("version", "updated_at")}
    updated.update(min_download_mbps=100, credits_per_period=25)
    response = client.put("/api/rules", json=updated)
    assert response.status_code == 200, response.text
    assert response.json()["version"] == rule["version"] + 1
    assert measurement(client, first["id"]) == saved_decision
    assert next(row for row in get(client, "/api/credits") if row["id"] == saved_credit["id"]) == saved_credit

    rejected = submit(client, payload(client, external_id="fails-new-policy", period="2020-01-02", download_mbps=30))
    valid = submit(client, payload(client, external_id="passes-new-policy", period="2020-01-03"))
    result = run_worker(client)
    assert result == {"processed": 2, "issued": 1, "rejected": 1, "failed": 0}
    failed_decision = measurement(client, rejected["id"])
    assert failed_decision["status"] == "ineligible"
    assert any(not check["passed"] and check["required"] == 100 for check in failed_decision["checks"])
    new_credit = next(row for row in get(client, "/api/credits") if row["measurement_id"] == valid["id"])
    assert new_credit["amount"] == 25
    assert new_credit["rule_version"] == rule["version"] + 1
    assert saved_credit["amount"] == rule["credits_per_period"]


@pytest.mark.parametrize("changes", [
    {"source": ""}, {"external_id": ""}, {"school_id": "missing"}, {"provider_id": "missing"},
    {"download_mbps": -1}, {"upload_mbps": -1}, {"latency_ms": -1},
    {"uptime_pct": -0.1}, {"uptime_pct": 100.1}, {"verification_status": "approved"},
    {"period": "2024-02-30"}, {"period": "not-a-date"}, {"period": "2999-01-01"},
])
def test_invalid_measurements_are_rejected_without_queue_work(client, changes):
    before = counts(client)
    response = client.post("/api/measurements", json=payload(client, **changes))
    assert response.status_code in (400, 422), response.text
    assert counts(client) == before


def test_school_provider_association_is_validated(client):
    reading = payload(client)
    other_provider = next(row for row in get(client, "/api/providers") if row["id"] != reading["provider_id"])
    before = counts(client)
    response = client.post("/api/measurements", json={**reading, "provider_id": other_provider["id"]})
    assert response.status_code in (400, 422), response.text
    assert counts(client) == before


@pytest.mark.parametrize("nonfinite", ["NaN", "Infinity", "-Infinity"])
def test_nonfinite_measurements_are_rejected(client, nonfinite):
    reading = payload(client)
    raw = json.dumps(reading).replace(str(reading["download_mbps"]), nonfinite, 1)
    before = counts(client)
    response = client.post("/api/measurements", content=raw, headers={"content-type": "application/json"})
    assert response.status_code in (400, 422), response.text
    assert counts(client) == before


@pytest.mark.parametrize("changes", [
    {"min_download_mbps": -1}, {"min_upload_mbps": -1}, {"max_latency_ms": -1},
    {"min_uptime_pct": 101}, {"credits_per_period": 0}, {"credits_per_period": 1.5},
])
def test_invalid_policy_does_not_create_a_new_version(client, changes):
    original = get(client, "/api/rules")
    update = {key: value for key, value in original.items() if key not in ("version", "updated_at")}
    response = client.put("/api/rules", json={**update, **changes})
    assert response.status_code == 422, response.text
    assert get(client, "/api/rules") == original


def test_all_scenarios_are_repeatable_and_use_past_dates(settled):
    client = settled
    for _ in range(2):
        for scenario in ("clean_batch", "below_threshold", "duplicate_delivery", "worker_failure"):
            before = counts(client)
            response = client.post(f"/api/demo/scenarios/{scenario}")
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["title"] and result["message"]
            readings = [measurement(client, reading_id) for reading_id in result["measurement_ids"]]
            assert all(date.fromisoformat(row["period"]) <= date.today() for row in readings)
            assert counts(client)["measurements"] - before["measurements"] == len(readings)
            if scenario == "clean_batch":
                assert result["credits_created"] == 3
                assert all(row["credit_issued"] for row in readings)
            elif scenario == "below_threshold":
                assert result["credits_created"] == 0
                assert readings[0]["status"] == "ineligible"
                assert sum(not check["passed"] for check in readings[0]["checks"]) >= 3
            elif scenario == "duplicate_delivery":
                assert result["duplicates_prevented"] == 2
                assert result["credits_created"] == 1
                assert counts(client)["events"] == before["events"] + 1
            else:
                assert result["credits_created"] == 0
                assert readings[0]["status"] == "pending"
                assert event_for(client, readings[0]["id"])["status"] == "failed"
    assert client.post("/api/demo/scenarios/missing").status_code == 404


def test_reset_restores_seed_and_default_policy(client):
    original_summary = get(client, "/api/overview")["summary"]
    original_counts = counts(client)
    original_ids = [row["id"] for row in get(client, "/api/measurements")]
    original_rule = get(client, "/api/rules")
    submit(client, payload(client))
    run_worker(client)
    update = {key: value for key, value in original_rule.items() if key not in ("version", "updated_at")}
    assert client.put("/api/rules", json={**update, "credits_per_period": 77}).status_code == 200
    response = client.post("/api/demo/reset")
    assert response.status_code == 200
    assert response.json() == {"status": "reset"}
    assert counts(client) == original_counts
    assert get(client, "/api/overview")["summary"] == original_summary
    assert [row["id"] for row in get(client, "/api/measurements")] == original_ids
    restored = get(client, "/api/rules")
    for key in ("version", "min_download_mbps", "min_upload_mbps", "max_latency_ms", "min_uptime_pct", "credits_per_period"):
        assert restored[key] == original_rule[key]


def test_postgres_enforces_event_identity_and_daily_issuance_uniqueness(client, isolated_app):
    _, database = isolated_app
    with database.connect() as connection:
        current_schema, engine = connection.execute("SELECT current_schema(),version()").fetchone()
        assert current_schema == database.schema and engine.startswith("PostgreSQL")
        constraints = {row[0] for row in connection.execute("SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname=%s AND c.contype='u'", (database.schema,))}
        assert "UNIQUE (source, external_id)" in constraints
        assert "UNIQUE (school_id, period)" in constraints


def test_concurrent_replays_create_one_measurement_and_one_event(settled):
    client = settled
    reading = payload(client, external_id="concurrent-delivery")
    before = counts(client)
    with ThreadPoolExecutor(max_workers=4) as executor:
        responses = list(executor.map(lambda _: client.post("/api/measurements", json=reading), range(4)))
    assert Counter(response.status_code for response in responses) == {201: 1, 200: 3}
    assert len({response.json()["measurement"]["id"] for response in responses}) == 1
    after = counts(client)
    assert after["measurements"] == before["measurements"] + 1
    assert after["events"] == before["events"] + 1
    assert run_worker(client)["issued"] == 1


def test_database_survives_process_restart_without_reseeding(tmp_path):
    """Two fresh Node processes prove persistence independent of module globals."""
    with PgDatabase() as database:
        with NodeServer(database) as client:
            result = client.post("/api/demo/scenarios/duplicate_delivery")
            assert result.status_code == 200, result.text
            ids = result.json()["measurement_ids"]
            before = get(client, "/api/overview")["summary"]
        with NodeServer(database) as client:
            assert get(client, "/api/overview")["summary"] == before
            assert set(ids).issubset(row["id"] for row in get(client, "/api/measurements"))

