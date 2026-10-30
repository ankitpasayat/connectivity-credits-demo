"""Black-box checks against real Node HTTP and isolated PostgreSQL schemas."""

import pytest

from tests.node_server import NodeServer
from tests.pg_database import PgDatabase


@pytest.fixture(scope="session")
def isolated_app(tmp_path_factory):
    with PgDatabase() as database:
        with NodeServer(database) as connected:
            yield connected, database


@pytest.fixture
def client(isolated_app):
    connected, _ = isolated_app
    response = connected.post("/api/demo/reset")
    assert response.status_code == 200, response.text
    yield connected


@pytest.fixture
def settled(client):
    """Drain the seed's pending queue so tests can attribute each new event."""
    response = client.post("/api/worker/run", json={"limit": 500})
    assert response.status_code == 200, response.text
    assert response.json()["failed"] == 0
    return client

