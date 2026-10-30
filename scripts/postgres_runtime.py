"""Password-authenticated task-owned PostgreSQL; never use the existing 5432 cluster."""

import argparse
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
from urllib.parse import quote


ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".data/postgres"
DATA = STATE / "cluster"
CONFIG = STATE / "credentials.json"
ENV_FILE = STATE / "app.env"
PORT = int(os.environ.get("PG_LOCAL_PORT", "5433"))
ADMIN = "connectivity_admin"
APP_USER = "connectivity_app"
DATABASE = "connectivity_credits"


def binary(name):
    directory = os.environ.get("PG_BIN")
    candidates = ([Path(directory) / name] if directory else []) + [Path("/Library/PostgreSQL/16/bin") / name]
    available = shutil.which(name)
    if available:
        candidates.append(Path(available))
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    raise RuntimeError("PostgreSQL binaries are required. Set PG_BIN to their directory; no system install is performed.")


def private_write(path, text):
    temporary = path.with_name("." + path.name + "." + secrets.token_hex(4) + ".tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(text)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def credentials():
    if not CONFIG.exists():
        raise RuntimeError("Run ./scripts/postgres-local.sh setup first")
    return json.loads(CONFIG.read_text())


def listening():
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=0.3):
            return True
    except OSError:
        return False


def owned_pid():
    marker = DATA / "postmaster.pid"
    if not marker.exists():
        return None
    lines = marker.read_text().splitlines()
    if len(lines) < 4 or Path(lines[1]).resolve() != DATA.resolve() or int(lines[3]) != PORT:
        raise RuntimeError("PostgreSQL ownership/port marker does not match this task; refusing to signal any process")
    pid = int(lines[0])
    result = subprocess.run(["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True)
    if result.returncode or not result.stdout.strip():
        return None
    if str(DATA) not in result.stdout or "postgres" not in result.stdout:
        raise RuntimeError("Recorded PID is not this task's PostgreSQL process; refusing to signal it")
    return pid


def prepare_credentials():
    if PORT == 5432:
        raise RuntimeError("Port 5432 belongs to the unrelated existing service; use the task-local 5433 default")
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    STATE.chmod(0o700)
    if not CONFIG.exists():
        private_write(CONFIG, json.dumps({"admin_password": secrets.token_urlsafe(32), "app_password": secrets.token_urlsafe(32)}, indent=2) + "\n")
    config = credentials()
    password_file = STATE / "admin.pw"
    private_write(password_file, config["admin_password"] + "\n")
    private_write(STATE / "app.pw", config["app_password"] + "\n")
    url = f"postgresql://{APP_USER}:{quote(config['app_password'], safe='')}@127.0.0.1:{PORT}/{DATABASE}"
    private_write(ENV_FILE, f"export DATABASE_URL='{url}'\nexport CONNECTIVITY_DB_SCHEMA='public'\n")
    private_write(STATE / "compose.env", f"CONNECTIVITY_POSTGRES_PASSWORD={config['app_password']}\nCONNECTIVITY_POSTGRES_ADMIN_PASSWORD={config['admin_password']}\n")
    return config


def setup():
    prepare_credentials()
    password_file = STATE / "admin.pw"
    if not (DATA / "PG_VERSION").exists():
        if listening():
            raise RuntimeError(f"Port {PORT} is occupied; refusing to initialize/reuse another server")
        subprocess.run([binary("initdb"), "-D", str(DATA), "--username", ADMIN,
                        "--pwfile", str(password_file), "--auth-local=scram-sha-256", "--auth-host=scram-sha-256",
                        "--encoding=UTF8", "--locale=C"], check=True)
        socket_directory = STATE / "socket"
        socket_directory.mkdir(mode=0o700, exist_ok=True)
        escaped_socket = str(socket_directory).replace("'", "''")
        with (DATA / "postgresql.conf").open("a") as stream:
            stream.write(f"\n# Task-local settings; never the installed system cluster.\nlisten_addresses='127.0.0.1'\nport={PORT}\nunix_socket_directories='{escaped_socket}'\npassword_encryption='scram-sha-256'\nlog_statement='none'\n")
    print(f"Task-local PostgreSQL configured; private connection environment: {ENV_FILE}")


def psql(query, database="postgres", *, app=False):
    config = credentials()
    env = {**os.environ, "PGPASSWORD": config["app_password" if app else "admin_password"]}
    result = subprocess.run([binary("psql"), "-X", "-w", "-h", "127.0.0.1", "-p", str(PORT),
                             "-U", APP_USER if app else ADMIN, "-d", database, "-At", "-v", "ON_ERROR_STOP=1"],
                            input=query, capture_output=True, text=True, env=env)
    if result.returncode:
        # Do not print SQL/connection strings: role setup contains a password.
        raise RuntimeError("Task-local PostgreSQL command failed; connection secrets were not logged")
    return result.stdout.strip()


def start():
    setup()
    pid = owned_pid()
    if not pid:
        if listening():
            raise RuntimeError(f"Port {PORT} is occupied by another server; refusing to reuse it")
        subprocess.run([binary("pg_ctl"), "-D", str(DATA), "-l", str(STATE / "server.log"), "-w", "-t", "30", "start"], check=True)
    if Path(psql("SHOW data_directory")).resolve() != DATA.resolve():
        raise RuntimeError("Connected data directory is not task-owned; refusing setup changes")
    config = credentials()
    # Both names and passwords are generated/constant, never arbitrary SQL text.
    if psql(f"SELECT 1 FROM pg_roles WHERE rolname='{APP_USER}'") != "1":
        psql(f"CREATE ROLE {APP_USER} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD '{config['app_password']}';")
    if psql(f"SELECT 1 FROM pg_database WHERE datname='{DATABASE}'") != "1":
        psql(f"CREATE DATABASE {DATABASE} OWNER {APP_USER};")
    assert psql("SELECT 1", DATABASE, app=True) == "1"
    print(f"Task-owned PostgreSQL ready at 127.0.0.1:{PORT}/{DATABASE}; role {APP_USER}; PID {owned_pid()}")


def stop():
    pid = owned_pid()
    if not pid:
        print("Task-owned PostgreSQL is stopped")
        return
    subprocess.run([binary("pg_ctl"), "-D", str(DATA), "-m", "fast", "-w", "-t", "30", "stop"], check=True)
    print("Task-owned PostgreSQL stopped; data and credentials retained")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", nargs="?", default="status", choices=("setup", "start", "stop", "status", "env-file", "credentials"))
    command = parser.parse_args().command
    if command == "setup": setup()
    elif command == "start": start()
    elif command == "stop": stop()
    elif command == "env-file": print(ENV_FILE)
    elif command == "credentials":
        prepare_credentials()
        print(f"Private credentials prepared in {STATE}; no PostgreSQL server was initialized or changed")
    else: print(json.dumps({"pid": owned_pid(), "listening": listening(), "host": "127.0.0.1", "port": PORT, "database": DATABASE, "role": APP_USER, "credentials": "private 0600 files", "environment_file": str(ENV_FILE)}))


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.SubprocessError) as error:
        raise SystemExit(str(error)) from error
