# Monitoring mode (`--monitoring`)

When running DataSync with `--monitoring` (e.g. from the DataSync-UI backend), the binary reads **one JSON request from stdin** and writes the response to stdout, then exits.

## Do you need to install extra software?

**No.** The monitoring pages (tracing, APM, bottlenecks, resources, costs, log-aggregation, alerting) do **not** require Jaeger, Prometheus, Elasticsearch, or any other third-party service. They use only:

| What        | Role |
|------------|------|
| **PostgreSQL** | Same DB as your app (e.g. DataLake). Holds the `metadata` schema and monitoring tables. |
| **DataSync binary** | Built from this repo. The UI backend spawns it with `--monitoring` for tracing, APM, bottlenecks, resources, etc. |
| **config.json** | In the DataSync directory. Tells the binary how to connect to PostgreSQL. |

So you only need: **PostgreSQL** (already in use), **DataSync** built, and **config.json** in place. The C++ code creates the monitoring tables automatically on first use, as long as the schema `metadata` exists.

## Requirements

- **Working directory**: The process is run with `cwd` = the DataSync project directory (where the `DataSync` executable lives).
- **Configuration**: The binary loads DB config from `config.json` in that directory, or from environment variables (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`).
- **PostgreSQL**: The monitoring commands connect to the same PostgreSQL instance. The **schema `metadata` must exist**; the C++ code then creates the monitoring tables (e.g. `metadata.distributed_traces`, `metadata.apm_metrics`, `metadata.cost_tracking`) on first use. If the schema is missing, create it once.

## One-time: create the metadata schema

If your database does not yet have the `metadata` schema, run once (e.g. with `psql` or any PostgreSQL client connected to your DB, e.g. DataLake):

```sql
CREATE SCHEMA IF NOT EXISTS metadata;
```

After that, the first time each monitoring feature is used, DataSync will create the needed tables (e.g. `metadata.distributed_traces`, `metadata.trace_spans`, `metadata.apm_metrics`, `metadata.cost_tracking`, `metadata.cost_budgets`, etc.).

## Quick setup for dev

1. Create `config.json` in the DataSync directory (same folder as the `DataSync` executable), for example:

```json
{
  "database": {
    "postgres": {
      "host": "localhost",
      "port": "5432",
      "database": "your_metadata_db",
      "user": "your_user",
      "password": "your_password"
    }
  }
}
```

2. Or set `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` in the environment before starting the DataSync-UI backend (so spawned DataSync processes inherit them).

If config is missing or the DB is unreachable, the UI will still respond with HTTP 200 and empty data so the app remains usable.
