"""Independent real PostgreSQL inspector and disposable schema ownership."""

import os
from pathlib import Path
import shlex
from uuid import uuid4

import psycopg
from psycopg import sql


ROOT = Path(__file__).resolve().parents[1]


def database_url():
    provided = os.environ.get("DATABASE_URL") or os.environ.get("CONNECTIVITY_DATABASE_URL")
    if provided:
        return provided
    private = ROOT / ".data/postgres/app.env"
    if private.exists():
        for line in private.read_text().splitlines():
            if line.startswith("export DATABASE_URL="):
                return shlex.split(line.split("=", 1)[1])[0]
    raise RuntimeError("Start task-owned PostgreSQL or provide DATABASE_URL; no SQLite fallback is used")


class PgDatabase:
    def __init__(self):
        self.url = database_url()
        self.schema = "qa_" + uuid4().hex
        with psycopg.connect(self.url, autocommit=True) as connection:
            connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(self.schema)))

    @property
    def environment(self):
        return {"DATABASE_URL": self.url, "CONNECTIVITY_DB_SCHEMA": self.schema}

    def connect(self, **kwargs):
        return psycopg.connect(self.url, options=f"-c search_path={self.schema}", **kwargs)

    def close(self):
        if not self.schema.startswith("qa_"):
            raise RuntimeError("Refusing to drop a schema not owned by the QA harness")
        with psycopg.connect(self.url, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(self.schema)))

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
