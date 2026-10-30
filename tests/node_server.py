"""Launch the real Node HTTP server with isolated state for independent tests."""

import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time

import httpx


ROOT = Path(__file__).resolve().parents[1]


def node_binary():
    candidates = [os.environ.get("NODE_BIN"), shutil.which("node"),
                  str(Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node/bin/node")]
    for candidate in candidates:
        if not candidate:
            continue
        if not shutil.which(candidate):
            if candidate == os.environ.get("NODE_BIN"):
                raise RuntimeError("NODE_BIN does not point to an executable")
            continue
        checked = subprocess.run([candidate, "-e", "const [a,b]=process.versions.node.split('.').map(Number);process.exit(a>22||(a===22&&b>=16)?0:1)"], capture_output=True)
        if checked.returncode == 0:
            return candidate
        if candidate == os.environ.get("NODE_BIN"):
            raise RuntimeError("NODE_BIN must be Node 22.16 or newer")
    raise RuntimeError("Node 22.16+ is required for the Express server tests")


class NodeServer:
    def __init__(self, database, *, transport="local", environment=None):
        self.database = database
        self.environment = {**os.environ, **database.environment, "CONNECTIVITY_TRANSPORT": transport,
                            **(environment or {})}
        self.process = None
        self.client = None
        self.log = None

    def __enter__(self):
        entry = ROOT / "server/dist/index.js"
        if not entry.exists():
            raise RuntimeError("Build the active Node backend first: cd server && npm ci && npm run build")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        self.base_url = f"http://127.0.0.1:{port}"
        self.log = tempfile.TemporaryFile(mode="w+t")
        self.process = subprocess.Popen([node_binary(), str(entry)], cwd=ROOT,
                                        env={**self.environment, "PORT": str(port), "HOST": "127.0.0.1"},
                                        stdin=subprocess.DEVNULL, stdout=self.log, stderr=subprocess.STDOUT)
        self.client = httpx.Client(base_url=self.base_url, timeout=60)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                self.log.seek(0)
                detail = self.log.read()
                self.__exit__(None, None, None)
                raise RuntimeError("Node server exited on startup: " + detail)
            try:
                if self.client.get("/api/health", timeout=3).status_code == 200:
                    return self.client
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        self.log.seek(0)
        detail = self.log.read()
        self.__exit__(None, None, None)
        raise RuntimeError("Node server startup timed out: " + detail)

    def __exit__(self, *_):
        if self.client:
            self.client.close()
        if self.process and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.log:
            self.log.close()
