"""Explicit launcher check; requires free ports 3000/5173 and uses a temp DB."""

import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

import httpx

from tests.node_server import node_binary
from tests.pg_database import PgDatabase


def listening(port):
    with socket.socket() as probe:
        return probe.connect_ex(("127.0.0.1", port)) == 0


def main():
    root = Path(__file__).resolve().parents[1]
    for port in (3000, 5173):
        if listening(port):
            raise RuntimeError(f"Port {port} is in use; refusing to touch an existing server")
    node = node_binary()
    try:
        reference_before = httpx.get("http://127.0.0.1:8765/api/health", timeout=3).json()
    except httpx.HTTPError:
        reference_before = None
    with PgDatabase() as database, tempfile.TemporaryFile(mode="w+t") as log:
        environment = {**os.environ, **database.environment, "CONNECTIVITY_TRANSPORT": "kafka"}
        environment.pop("CONNECTIVITY_LAUNCHER_POSTGRES_PID", None)
        environment.pop("CONNECTIVITY_LAUNCHER_BROKER_PID", None)
        process = subprocess.Popen([node, "scripts/dev.mjs"], cwd=root,
                                   env=environment,
                                   stdout=log, stderr=subprocess.STDOUT)
        report = {"node": subprocess.check_output([node, "--version"], text=True).strip(), "database_isolated": True, "database_isolation": "temporary PostgreSQL schema", "ports": [3000, 5173]}
        try:
            for _ in range(100):
                if process.poll() is not None:
                    log.seek(0)
                    raise RuntimeError(log.read())
                try:
                    api = httpx.get("http://127.0.0.1:3000/api/health", timeout=2)
                    ui = httpx.get("http://127.0.0.1:5173/api/health", timeout=2)
                    if api.status_code == 200 and ui.status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                time.sleep(0.1)
            else:
                raise RuntimeError("Launcher startup timeout")
            assert api.json()["runtime"] == "node" and ui.json()["framework"] == "express"
            assert api.json()["transport"] == "kafka" and api.json()["broker"] == "ok"
            assert api.json()["database_engine"] == "postgresql"
            assert "/@vite/client" in httpx.get("http://127.0.0.1:5173").text
            assert '<div id="root">' in httpx.get("http://127.0.0.1:3000").text
            report["api_health"] = api.json()
            report["vite_proxy_health"] = ui.json()
        finally:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                raise
        assert all(not listening(port) for port in (3000, 5173)), "Owned app ports leaked after shutdown"
        report["owned_ports_closed_after_SIGTERM"] = True
        if reference_before is not None:
            report["reference_8765_health_unchanged"] = httpx.get("http://127.0.0.1:8765/api/health").json() == reference_before
            assert report["reference_8765_health_unchanged"]
        destination = root / "artifacts/postgres/launcher.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report))


if __name__ == "__main__":
    main()
