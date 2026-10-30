"""Task-local Apache Kafka + Temurin runtime, with verified official archives.

No system installation or shell profile changes. All mutable state lives in .data.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import tarfile
import time


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".data/runtime"
DOWNLOADS = RUNTIME / "downloads"
STATE = ROOT / ".data/kafka"
KAFKA_VERSION = "4.4.0"
KAFKA_NAME = f"kafka_2.13-{KAFKA_VERSION}"
KAFKA_URL = f"https://archive.apache.org/dist/kafka/{KAFKA_VERSION}/{KAFKA_NAME}.tgz"
KAFKA_SHA512 = "f5ceba8a265ab6bb2d68362b1f6859a95e15d60ef92ba83b43fdf065b557dc18e98d6383c15b9977320e25b3a2c8b6f11decb17b0fa6c0f163423793a0eb95cb"
JAVA_VERSION = "21.0.13+9"
PORT = int(os.environ.get("KAFKA_LOCAL_PORT", "19092"))
CONTROLLER_PORT = int(os.environ.get("KAFKA_LOCAL_CONTROLLER_PORT", "19093"))
CONFIG = STATE / "server.properties"
PID_FILE = STATE / "broker.pid"
BROKER_LOG = STATE / "broker.log"
MANIFEST = RUNTIME / "verified-downloads.json"


def download(url, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["curl", "--fail", "--location", "--silent", "--show-error", "--retry", "3", "--connect-timeout", "15", "--output", str(target), url], check=True)


def digest(path, algorithm):
    hasher = hashlib.new(algorithm)
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def archive(url, filename, algorithm, expected):
    target = DOWNLOADS / filename
    if not target.exists():
        print(f"Downloading {filename} from {url}", flush=True)
        partial = target.with_name(target.name + ".part")
        download(url, partial)
        actual = digest(partial, algorithm)
        if actual != expected.lower():
            raise RuntimeError(f"Checksum mismatch for {filename}; archive was not extracted")
        partial.replace(target)
    actual = digest(target, algorithm)
    if actual != expected.lower():
        raise RuntimeError(f"Cached archive checksum mismatch: {target}. Remove that archive and retry.")
    print(f"Verified {algorithm}: {filename}", flush=True)
    return target


def extract(source, destination):
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(source) as bundle:
        # Python 3.11.8+ includes the data filter; no archive paths may escape.
        bundle.extractall(destination, filter="data")


def java_home():
    location = RUNTIME / f"temurin-{JAVA_VERSION}"
    executables = list(location.glob("**/bin/java"))
    if not executables:
        raise RuntimeError("Task-local Java not set up. Run ./scripts/kafka-local.sh setup")
    return executables[0].parent.parent


def setup():
    RUNTIME.mkdir(parents=True, exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)
    if MANIFEST.exists() and (RUNTIME / KAFKA_NAME / "bin/kafka-server-start.sh").exists():
        try:
            java_home()
            return
        except RuntimeError:
            pass
    architecture = {"arm64": "aarch64", "aarch64": "aarch64", "x86_64": "x64", "amd64": "x64"}.get(platform.machine().lower())
    operating_system = {"Darwin": "mac", "Linux": "linux"}.get(platform.system())
    if not architecture or not operating_system:
        raise RuntimeError("Task-local bootstrap supports macOS/Linux arm64 and x64. Use Docker Compose on other platforms.")
    metadata_url = f"https://api.adoptium.net/v3/assets/release_name/eclipse/jdk-21.0.13%2B9?architecture={architecture}&image_type=jre&jvm_impl=hotspot&os={operating_system}"
    metadata_file = DOWNLOADS / f"temurin-{operating_system}-{architecture}.json"
    download(metadata_url, metadata_file)
    metadata = json.loads(metadata_file.read_text())
    if isinstance(metadata, list):
        metadata = metadata[0]
    package = metadata["binaries"][0]["package"] if "binaries" in metadata else metadata["binary"]["package"]
    # Reuse the first downloaded arm64 mac archive; other platforms get distinct names.
    java_filename = "temurin21-mac-arm64.tar.gz" if operating_system == "mac" and architecture == "aarch64" else f"temurin21-{operating_system}-{architecture}.tar.gz"
    java_archive = archive(package["link"], java_filename, "sha256", package["checksum"])
    kafka_archive = archive(KAFKA_URL, f"{KAFKA_NAME}.tgz", "sha512", KAFKA_SHA512)
    if not (RUNTIME / f"temurin-{JAVA_VERSION}").exists():
        extract(java_archive, RUNTIME / f"temurin-{JAVA_VERSION}")
    if not (RUNTIME / KAFKA_NAME).exists():
        extract(kafka_archive, RUNTIME)
    manifest = {
        "verified_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "kafka": {"version": KAFKA_VERSION, "url": KAFKA_URL, "checksum_url": KAFKA_URL + ".sha512", "sha512": KAFKA_SHA512, "bytes": kafka_archive.stat().st_size},
        "java": {"version": JAVA_VERSION, "os": operating_system, "architecture": architecture, "url": package["link"], "metadata_url": metadata_url, "checksum_url": package["checksum_link"], "sha256": package["checksum"], "bytes": java_archive.stat().st_size},
    }
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n")
    subprocess.run([str(java_home() / "bin/java"), "-version"], check=True)
    print(f"Verified runtime manifest: {MANIFEST}", flush=True)


def environment():
    env = dict(os.environ)
    env["JAVA_HOME"] = str(java_home())
    env["PATH"] = str(java_home() / "bin") + os.pathsep + env.get("PATH", "")
    env.setdefault("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
    # JMX is not needed for this localhost broker.
    env.pop("JMX_PORT", None)
    return env


def tool(name, arguments=(), **kwargs):
    executable = RUNTIME / KAFKA_NAME / "bin" / name
    if executable.parent != RUNTIME / KAFKA_NAME / "bin" or not executable.is_file():
        raise RuntimeError(f"Unknown Kafka tool: {name}")
    return subprocess.run([str(executable), *arguments], env=environment(), **kwargs)


def owned_pid():
    if not PID_FILE.exists():
        return None
    try:
        pid = int(PID_FILE.read_text().strip())
        result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True)
        if result.returncode or not result.stdout.strip():
            return None
        if str(CONFIG) not in result.stdout or "kafka.Kafka" not in result.stdout:
            raise RuntimeError(f"PID {pid} is not this task's broker; refusing to signal it")
        return pid
    except ValueError:
        return None


def listening(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.3):
            return True
    except OSError:
        return False


def start():
    setup()
    existing = owned_pid()
    if existing:
        print(f"Task-local Kafka already running: PID {existing}, 127.0.0.1:{PORT}")
        return
    if listening(PORT) or listening(CONTROLLER_PORT):
        raise RuntimeError(f"Port {PORT} or {CONTROLLER_PORT} is occupied by another process; refusing to reuse it")
    STATE.mkdir(parents=True, exist_ok=True)
    log_directory = STATE / "logs"
    CONFIG.write_text(f"""# Generated only for this local demonstration.
process.roles=broker,controller
node.id=1
controller.quorum.bootstrap.servers=127.0.0.1:{CONTROLLER_PORT}
listeners=PLAINTEXT://127.0.0.1:{PORT},CONTROLLER://127.0.0.1:{CONTROLLER_PORT}
advertised.listeners=PLAINTEXT://127.0.0.1:{PORT}
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={log_directory}
num.partitions=1
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
log.retention.hours=24
auto.create.topics.enable=true
""")
    if not (log_directory / "meta.properties").exists():
        identifier = tool("kafka-storage.sh", ["random-uuid"], check=True, capture_output=True, text=True).stdout.strip().splitlines()[-1]
        tool("kafka-storage.sh", ["format", "--standalone", "-t", identifier, "-c", str(CONFIG)], check=True)
    with BROKER_LOG.open("ab") as log:
        process = subprocess.Popen([str(RUNTIME / KAFKA_NAME / "bin/kafka-server-start.sh"), str(CONFIG)], env=environment(), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    PID_FILE.write_text(str(process.pid) + "\n")
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError(f"Kafka exited; see {BROKER_LOG}")
        if listening(PORT):
            result = tool("kafka-broker-api-versions.sh", ["--bootstrap-server", f"127.0.0.1:{PORT}"], capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                print(f"Task-local Apache Kafka {KAFKA_VERSION} ready at 127.0.0.1:{PORT}; PID {process.pid}")
                return
        time.sleep(0.25)
    raise RuntimeError(f"Kafka readiness timed out; see {BROKER_LOG}")


def stop():
    pid = owned_pid()
    if not pid:
        print("Task-local broker is stopped")
        PID_FILE.unlink(missing_ok=True)
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(120):
        if not owned_pid():
            PID_FILE.unlink(missing_ok=True)
            print("Task-local Kafka stopped; data and verified runtimes retained in .data")
            return
        time.sleep(0.25)
    raise RuntimeError(f"Broker PID {pid} is still shutting down; inspect {BROKER_LOG}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("setup", "start", "stop", "status", "tool"))
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    arguments = parser.parse_args()
    if arguments.command == "setup":
        setup()
    elif arguments.command == "start":
        start()
    elif arguments.command == "stop":
        stop()
    elif arguments.command == "status":
        print(json.dumps({"pid": owned_pid(), "listening": listening(PORT), "bootstrap_servers": f"127.0.0.1:{PORT}", "log": str(BROKER_LOG)}))
    elif arguments.command == "tool":
        if not arguments.arguments:
            parser.error("tool requires a Kafka script name and optional arguments")
        setup()
        result = tool(arguments.arguments[0], arguments.arguments[1:])
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error)) from error

