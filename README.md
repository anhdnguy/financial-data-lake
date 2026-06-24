# Financial Data Lake

> End-of-day OHLCV data pipeline for the Russell 3000 universe — built on Apache Airflow,
> Delta Lake, and PostgreSQL.

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-TaskFlow-017CEE?logo=apacheairflow&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-delta--rs-00ADD8)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![LocalStack](https://img.shields.io/badge/LocalStack-S3-7B42BC)
![Status](https://img.shields.io/badge/status-active%20development-yellow)

A production-shaped data engineering project: it ingests daily price data for the top
US equities, validates it through four layers, stores price history in **Delta Lake on
S3** and operational metadata in **PostgreSQL**, and orchestrates everything with
**Apache Airflow**. It is Phase 1 of a longer quant-developer roadmap, with an emphasis
on doing data engineering *correctly* — clean layering, idempotent writes, and explicit
data-quality gates.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Model](#data-model)
- [Pipelines](#pipelines)
- [OHLCV Validation](#ohlcv-validation)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Running the Pipelines](#running-the-pipelines)
- [Storage Layout](#storage-layout)
- [Design Decisions](#design-decisions)
- [Roadmap](#roadmap)
- [Testing](#testing)
- [License](#license)

---

## Features

- **Daily EOD OHLCV ingestion** from the Schwab `/pricehistory` API for the Russell 3000.
- **Four-layer data validation** — HTTP, field presence, internal consistency, and a
  statistical 5-sigma outlier gate using per-ticker rolling volatility.
- **Delta Lake price store** (ACID, year-partitioned) on S3 via `delta-rs` — no Spark.
- **PostgreSQL metadata store** — universe membership, rolling volatility, a dead-letter
  queue, and an operational audit log.
- **Survivorship-bias-aware universe maintenance** — delisted symbols are soft-deleted
  (an `exit_date` is set), never hard-deleted.
- **Two-tier Schwab token management** — short-lived access token cached in Redis,
  durable refresh token in PostgreSQL; a single-flight `SET NX` lock collapses the
  thundering herd of ~3000 concurrent cache misses into one refresh.
- **Resilient external calls** — failed symbols routed to a dead-letter queue for retry,
  not dropped; terminal vs. transient refresh failures are classified distinctly.
- **Strict three-layer architecture** — orchestration, business logic, and I/O are
  cleanly separated, with pure functions for all transforms.

---

## Architecture

**Layered design** — each layer has one job and never reaches past the next:

```
Airflow DAG          orchestration only (task wiring, scheduling, retries)
     │
     ▼
Service layer        business logic; owns the DB connection + transaction lifecycle
     │
     ▼
Client layer         external-system boundaries — no business logic, no commits
                     DBClient · SchwabClient · S3DeltaClient

Transform layer      pure functions (no I/O) — imported directly by tasks and services
```

**Storage split** — the right tool for each kind of data:

| Data | Store | Why |
|------|-------|-----|
| OHLCV price history (large, append-only) | **Delta Lake on S3** | Cheap columnar storage, ACID appends, scales |
| Membership, volatility, DLQ, audit log (small, relational) | **PostgreSQL** | Transactions, joins, point-in-time queries |

Derived facts flow from the price store back to metadata: rolling volatility is
*computed* from the last 21 closes read out of Delta Lake, and only the resulting scalar
is written to PostgreSQL.

**Runtime topology** — services communicate over an external Docker network,
`shared-network`:

```
┌─────────────────────────  shared-network  ─────────────────────────┐
│                                                                     │
│   Airflow stack            ohlcv-db          ohlcv-redis            │
│   (separate compose)       (PostgreSQL)      (token cache + lock)   │
│                                                                     │
│   LocalStack (S3)                                                   │
└─────────────────────────────────────────────────────────────────────┘
```

This repo provisions `ohlcv-db` and `ohlcv-redis`. The Airflow and LocalStack stacks run
as separate Compose projects attached to the same `shared-network`.

---

## Tech Stack

| Concern | Technology |
|---------|-----------|
| Orchestration | Apache Airflow (TaskFlow API, dynamic task mapping) |
| Language | Python 3.11 |
| Metadata DB | PostgreSQL 16 (`psycopg2`, `execute_values` for bulk ops) |
| Price store | Delta Lake via `deltalake` (delta-rs) + PyArrow |
| Object storage | LocalStack S3 in dev (AWS S3 in prod) |
| Token cache / lock | Redis (`SET NX` distributed lock) |
| Data source | Schwab API (`/pricehistory`) + CSV universe seed |
| Data processing | pandas, NumPy |
| Infra | Docker Compose |

---

## Project Structure

```
financial-data-lake/
├── airflow/
│   └── dags/
│       ├── universe_maintenance.py    # weekly: maintain Russell 3000 membership
│       └── market_data_pipeline.py    # daily: ingest + validate + store EOD OHLCV
├── src/
│   ├── clients/                       # external-system boundaries (no business logic)
│   │   ├── db_client.py               #   PostgreSQL (psycopg2, no internal commits)
│   │   ├── schwab_client.py           #   Schwab API /pricehistory + validation 1–3
│   │   ├── schwab_token.py            #   two-tier token: Redis cache + Postgres seed, SET NX
│   │   ├── s3_client.py               #   Delta Lake on S3 (write / read_recent_closes)
│   │   └── *_exception.py             #   db / schwab / s3 client error types
│   ├── services/                      # business logic; own DB transaction lifecycle
│   │   ├── universe_service.py
│   │   ├── ohlcv_service.py
│   │   └── pipeline_exception.py
│   ├── transform/                     # pure functions (no I/O)
│   │   ├── universe_transform.py
│   │   └── ohlcv_transform.py         #   chunk, build_dataframe, dedup, rolling vol
│   ├── config/config.py               # AppConfig — env-var loading
│   └── utilities/bootstrap.py         # wires AppConfig + clients + service
├── scripts/
│   └── create_bucket.sh               # one-time LocalStack bucket provisioning
├── stock_csv/tickers.csv              # universe seed (manual; iShares IWV planned)
├── database_init.sql                  # schema: 7 tables + indexes + triggers
├── docker-compose.yaml                # ohlcv-db + ohlcv-redis on shared-network
├── setup.yaml                         # conda environment definition
└── .env.example                       # configuration template
```

---

## Data Model

PostgreSQL schema (`database_init.sql`), database `ohlcv`:

| Table | Purpose |
|-------|---------|
| `universe` | Registry of universes (Russell 3000, S&P 500, …) |
| `membership` | Symbol registry — **never deleted** |
| `universe_membership` | Symbol ↔ universe with `enter_date` / `exit_date`, delisted reason |
| `volatility_rolling` | Per-ticker rolling volatility (one row per symbol) used by validation |
| `failed_ingestion` | Dead-letter queue — `failure_mode` (`HTTP_ERROR` \| `VALIDATION`), `retry_after`, `review_required` |
| `pipeline_run` | Operational audit log — status `RUNNING` \| `SUCCESS` \| `FAILED` \| `PARTIAL` |
| `schwab_token` | Durable Schwab refresh token — one row per provider (`UNIQUE`), `issued_at` anchors the 7-day expiry wall |

Highlights: a partial unique index enforces one active membership per symbol per universe
(`exit_date IS NULL`); `updated_at` triggers on mutable tables; an index on
`universe_membership(enter_date, exit_date)` supports point-in-time queries.

---

## Pipelines

### `universe_maintenance` (weekly)
Keeps Russell 3000 membership current. Reads the universe CSV, normalizes symbols to dot
notation (`BRK/B → BRK.B`), diffs against active DB membership, upserts new symbols, and
**soft-deletes** delistings by setting `exit_date` — preserving history for survivorship-
bias correctness.

### `market_data_pipeline` (daily, weekdays)
Nine tasks, with `fetch_ohlcv` fanned out via dynamic task mapping for concurrent chunks:

```
log_pipeline_start → query_active_symbols → build_chunks
        → fetch_ohlcv (mapped per chunk)        # Schwab fetch + validation layers 1–3
        → aggregate_results                      # flatten, build DataFrame, dedup
        → statistical_validation                 # layer 4: 5-sigma outlier gate
        → write_to_delta_lake                    # year-partitioned append
        → update_volatility                      # recompute rolling vol → PostgreSQL
        → log_pipeline_end                       # SUCCESS / PARTIAL / FAILED
```

A DAG-level `on_failure_callback` marks the run `FAILED`. Symbols that fail fetch or
validation are written to the `failed_ingestion` dead-letter queue and retried on a later
run rather than silently dropped.

---

## OHLCV Validation

Every candle passes four layers before it is persisted:

| Layer | Check | On failure |
|-------|-------|-----------|
| 1 — HTTP | Status 200, non-empty body, candle date == today (UTC) | Raise / DLQ |
| 2 — Fields | `open/high/low/close/volume` present and positive | DLQ (`VALIDATION`) |
| 3 — Consistency | `high ≥ open,close,low` and `low ≤ open,close` | DLQ (`VALIDATION`) |
| 4 — Statistical | 5σ log-return filter, volume spike, open-vs-prev-close gap | DLQ (`VALIDATION`) |

Layer 4 uses each ticker's **daily** log-return standard deviation (`rolling_sd`, sample
`ddof=1`) as the threshold on a single-day log return — so the stored volatility is
deliberately *not* annualized.

---

## Getting Started

### Prerequisites

- [Conda](https://docs.conda.io/) (Miniconda/Anaconda)
- Docker + Docker Compose
- The external `shared-network` Docker network, with the **Airflow** and **LocalStack**
  stacks running on it:
  ```bash
  docker network create shared-network   # if it does not already exist
  ```

### 1. Environment

```bash
conda env create -f setup.yaml
conda activate financial_data_lake
```

### 2. Configuration

```bash
cp .env.example .env
# then edit .env — set DB_PASSWORD and your Schwab credentials
```

### 3. Infrastructure

```bash
docker-compose up -d                    # start ohlcv-db + ohlcv-redis
```

### 4. Database schema

```bash
docker exec -i financial-data-lake-ohlcv-db-1 \
  psql -U "$DB_USER" -d "$DB_DB" < database_init.sql
```

### 5. Delta Lake bucket (one-time)

```bash
bash scripts/create_bucket.sh           # provisions s3://ohlcv in LocalStack
```

---

## Configuration

All configuration is loaded from `.env` by `AppConfig` (`src/config/config.py`). See
[`.env.example`](.env.example) for the full template.

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_USER` / `DB_PASSWORD` / `DB_DB` | PostgreSQL credentials + database | — |
| `DB_HOSTNAME` | DB host (`ohlcv-db` in-network, `localhost` from host) | `localhost` |
| `REDIS_HOST` / `REDIS_PORT` | Redis token cache + lock | `localhost` / `6379` |
| `SCHWAB_CLIENT_ID` / `SCHWAB_CLIENT_SECRET` | Schwab OAuth client credentials | — |
| `SCHWAB_BASE_URL` | Schwab API base URL | `https://api.schwabapi.com` |
| `S3_ENDPOINT_URL` | S3 endpoint (`localstack:4566` in-network) | `http://localstack:4566` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials (`test`/`test` for LocalStack) | `test` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `DELTA_TABLE_URI` | Delta table path | `s3://ohlcv/prices` |

> **Dual addressing:** services are reachable by container alias on `shared-network`
> (e.g. `ohlcv-db`, `localstack:4566`) and by `localhost` from the host via published
> ports. Set the host-style values when running anything outside the Docker network.

> **The Schwab refresh token is _not_ an env var.** It is durable state, not config — it
> lives in the PostgreSQL `schwab_token` table (one row per provider, anchored by
> `issued_at` for the 7-day expiry wall) and is provisioned/rotated by the human re-auth
> path. Only the OAuth client credentials (`SCHWAB_CLIENT_ID` / `SCHWAB_CLIENT_SECRET`)
> come from `.env`. See [Design Decisions](#design-decisions).

---

## Running the Pipelines

Airflow runs as a **separate Compose stack** on `shared-network`. Mount this repo's DAGs
into it and ensure the repo root is on `PYTHONPATH` (the DAGs import `src.*`):

- DAGs folder → `airflow/dags/`
- `PYTHONPATH` → repo root

For lightweight local development you can instead run Airflow standalone on the host
(using the `localhost` config overrides):

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd)
airflow standalone
```

Then trigger `universe_maintenance` once to seed membership, followed by
`market_data_pipeline`.

---

## Storage Layout

Delta Lake table at `s3://ohlcv/prices/`, **partitioned by year**:

```
s3://ohlcv/prices/
├── _delta_log/                 # JSON transaction log (the source of truth)
│   └── 00000000000000000000.json
└── year=2026/
    └── part-*.snappy.parquet   # columnar OHLCV data
```

Year partitioning (rather than daily) keeps file sizes healthy and avoids the
small-files problem. Writes are append-only after in-pipeline deduplication;
`OPTIMIZE` + `VACUUM` compaction is planned for a separate weekly maintenance DAG.

---

## Design Decisions

- **Delta Lake via `delta-rs`, not Spark** — the data volume doesn't justify Spark/JVM
  overhead; a pure-Python library is enough.
- **Two-tier Schwab token model** — the access token (~30 min) and the refresh token
  (~7 days) have different lifetimes and different homes, and conflating them was the bug
  this design removed. The access token is cached in **Redis** under its own TTL; the
  refresh token is durable in **PostgreSQL** (`schwab_token`, anchored by `issued_at` for
  the hard 7-day wall). On a cache miss the refresh runs **single-flight** under a Redis
  `SET NX` lock: the winner re-reads the cache, reads the refresh seed from Postgres
  read-only, calls Schwab, and re-caches; losers poll for the fresh token or a short-TTL
  `RedisErrorMarker` so a terminal failure fails them fast instead of spinning to a lock
  timeout. The lock is released with a compare-and-delete (only the owner deletes), and
  refresh failures are classified **terminal** (HTTP 400/401 / `invalid_grant` → human
  re-auth) vs. **transient** (network / 5xx / 429 → retry next run).
- **Dedicated `ohlcv-redis`, not Airflow's broker** — the token-refresh `SET NX` lock is
  a correctness primitive. A shared broker's `maxmemory`/LRU eviction could drop the lock
  key before its TTL and silently break mutual exclusion. A dedicated Redis with no cap
  guarantees the lock lives its full life.
- **Clients never commit** — transaction control belongs to the service layer's
  `__exit__` (commit on success, rollback on exception).
- **Soft deletes only** — delistings set `exit_date`; price points are never zeroed.
- **Parameterized SQL only** — no f-string interpolation, ever.

---

## Roadmap

- [x] PostgreSQL schema, DBClient, UniverseService, universe-maintenance DAG
- [x] Schwab API client with Redis token management
- [x] `market_data_pipeline` DAG (dynamic task mapping, failure handling)
- [x] Delta Lake write layer + rolling-volatility computation
- [ ] First end-to-end run with live Schwab credentials
- [ ] `tests/` suite — transforms, clients (mocked), service
- [ ] DLQ retry merge verified end-to-end
- [ ] Weekly `OPTIMIZE` + `VACUUM` maintenance DAG
- [ ] Point-in-time correctness / historical constituent snapshots (later phase)

---

## Testing

A `tests/` suite is planned (not yet implemented): unit tests for the pure transforms,
client tests with mocked Redis / `requests` / Delta Lake, and service-level tests with a
mocked `SchwabClient`. The intended approach is unit tests first (mock at each boundary),
then integration, then end-to-end.

---

## License

This is a personal learning project (Phase 1 of a multi-month quant-developer roadmap).
No formal license is attached and it is not intended for redistribution or production use.
