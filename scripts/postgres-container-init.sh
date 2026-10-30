#!/usr/bin/env bash
set -euo pipefail
# Runs only inside the new Compose PostgreSQL volume, never a host cluster.
export PGPASSWORD="$POSTGRES_PASSWORD"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<'SQL'
\getenv app_password CONNECTIVITY_POSTGRES_PASSWORD
CREATE ROLE connectivity_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE PASSWORD :'app_password';
CREATE DATABASE connectivity_credits OWNER connectivity_app;
SQL
