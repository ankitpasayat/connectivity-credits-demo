"""Opt-in tests use a real broker; no producer or consumer is mocked."""

import json
import os
from uuid import uuid4

import pytest

from tests.node_server import NodeServer
from tests.pg_database import PgDatabase
from psycopg.rows import dict_row


pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(os.environ.get("RUN_KAFKA_TESTS") != "1", reason="Set RUN_KAFKA_TESTS=1 to require a real Kafka broker"),
]


@pytest.fixture
def broker_client(tmp_path):
    from confluent_kafka.admin import AdminClient

    bootstrap = os.environ.get("CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092")
    topic = "connectivity-independent-" + uuid4().hex
    group = "independent-qa-" + uuid4().hex
    environment = {"CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                   "CONNECTIVITY_KAFKA_TOPIC": topic, "CONNECTIVITY_KAFKA_GROUP_ID": group,
                   "CONNECTIVITY_KAFKA_TIMEOUT_SECONDS": "15"}
    admin = AdminClient({"bootstrap.servers": bootstrap})
    assert admin.list_topics(timeout=5).brokers, "A real reachable broker is required"
    with PgDatabase() as database:
        node = NodeServer(database, transport="kafka", environment=environment)
        with node as client:
            health = client.get("/api/health").json()
            assert health["transport"] == "kafka" and health["broker"] == "ok"
            assert health["database_engine"] == "postgresql"
            settled = client.post("/api/worker/run", json={"limit": 500})
            assert settled.status_code == 200, settled.text
            yield client, database, bootstrap, topic, group, node
    for future in admin.delete_topics([topic], operation_timeout=5).values():
        future.result(timeout=10)


def reading(client, external_id, period):
    school = client.get("/api/schools").json()[0]
    return {"source": "independent-broker-qa", "external_id": external_id,
            "school_id": school["id"], "provider_id": school["provider_id"], "period": period,
            "download_mbps": 50, "upload_mbps": 20, "latency_ms": 20,
            "uptime_pct": 100, "verification_status": "verified"}


def submit(client, external_id, period):
    response = client.post("/api/measurements", json=reading(client, external_id, period))
    assert response.status_code == 201, response.text
    return response.json()["measurement"]


def process(client):
    response = client.post("/api/worker/run", json={"limit": 500})
    assert response.status_code == 200, response.text
    return response.json()


def test_actual_broker_record_and_raw_duplicate_do_not_reissue(broker_client):
    """Read the wire record independently, then inject a real broker duplicate."""
    from confluent_kafka import Consumer, Producer, TopicPartition

    client, database, bootstrap, topic, group, _ = broker_client
    before = len(client.get("/api/credits").json())
    first = submit(client, "first-wire-reading", "2020-04-01")
    assert process(client)["issued"] == 1
    with database.connect() as connection:
        outbox = connection.cursor(row_factory=dict_row).execute("SELECT o.* FROM broker_outbox o JOIN events e ON e.id=o.event_id WHERE e.measurement_id=%s", (first["id"],)).fetchone()
        instance = connection.execute("SELECT value FROM metadata WHERE key='broker_namespace'").fetchone()[0]
    assert outbox["published_at"] and outbox["consumed_at"]

    inspector = Consumer({"bootstrap.servers": bootstrap, "group.id": "wire-inspector-" + uuid4().hex,
                          "enable.auto.commit": False, "enable.auto.offset.store": False})
    try:
        inspector.assign([TopicPartition(topic, 0, outbox["broker_offset"])])
        message = inspector.poll(10)
        assert message is not None and not message.error()
        assert json.loads(message.value()) == json.loads(outbox["payload_json"])
        assert json.loads(message.value())["measurement_id"] == first["id"]
    finally:
        inspector.close()

    delivered = []
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    producer.produce(topic, partition=0, key=outbox["event_id"], value=outbox["payload_json"],
                     on_delivery=lambda error, message: delivered.append((error, message.offset())))
    assert producer.flush(10) == 0
    assert delivered and delivered[0][0] is None
    duplicate_offset = delivered[0][1]
    assert duplicate_offset > outbox["broker_offset"]

    second = submit(client, "second-wire-reading", "2020-04-02")
    assert process(client)["issued"] == 1
    credits = client.get("/api/credits").json()
    assert len(credits) == before + 2
    assert sum(row["measurement_id"] == first["id"] for row in credits) == 1
    assert sum(row["measurement_id"] == second["id"] for row in credits) == 1
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM broker_inbox WHERE delivery_id=%s", (outbox["id"],)).fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM decisions WHERE measurement_id=%s", (first["id"],)).fetchone()[0] == 1
    committed = Consumer({"bootstrap.servers": bootstrap, "group.id": group + "-" + instance, "enable.auto.commit": False})
    try:
        assert committed.committed([TopicPartition(topic, 0)], timeout=5)[0].offset > duplicate_offset
    finally:
        committed.close()


def test_queued_work_recovers_after_real_broker_connection_is_restored(broker_client):
    client, database, bootstrap, topic, group, node = broker_client
    created = submit(client, "outage-recovery-reading", "2020-04-03")
    before = client.get("/api/credits").json()
    node.__exit__()
    environment = {"CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:1",
                   "CONNECTIVITY_KAFKA_TIMEOUT_SECONDS": "1", "CONNECTIVITY_KAFKA_TOPIC": topic,
                   "CONNECTIVITY_KAFKA_GROUP_ID": group}
    with NodeServer(database, transport="kafka", environment=environment) as unavailable:
        outage = unavailable.post("/api/worker/run", json={"limit": 500})
        assert outage.status_code == 503, outage.text
        assert "no local fallback" in outage.json()["detail"]
        assert unavailable.get("/api/credits").json() == before
        assert unavailable.get(f'/api/measurements/{created["id"]}').json()["status"] == "pending"
    with database.connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM broker_outbox o JOIN events e ON e.id=o.event_id WHERE e.measurement_id=%s AND o.published_at IS NULL", (created["id"],)).fetchone()[0] == 1
    environment.update(CONNECTIVITY_KAFKA_BOOTSTRAP_SERVERS=bootstrap, CONNECTIVITY_KAFKA_TIMEOUT_SECONDS="15")
    with NodeServer(database, transport="kafka", environment=environment) as recovered:
        assert process(recovered) == {"processed": 1, "issued": 1, "rejected": 0, "failed": 0}
        assert recovered.get(f'/api/measurements/{created["id"]}').json()["credit_issued"] is True
        assert process(recovered)["issued"] == 0
