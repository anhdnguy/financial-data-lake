# Financial Data Lake

> End-of-day OHLCV data pipeline for the Russell 3000 universe — built on Apache Airflow,
> Delta Lake, and PostgreSQL.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-3.1-017CEE?logo=apacheairflow&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.4.9-red?logo=redis&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-delta--rs-00ADD8)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![LocalStack](https://img.shields.io/badge/LocalStack-S3-7B42BC)
![Status](https://img.shields.io/badge/status-active-green)

A production-shaped data engineering project: it ingests daily price data for the top
US equities, validates it through four layers, stores price history in **Delta Lake on
S3** and operational metadata in **PostgreSQL**, and orchestrates everything with
**Apache Airflow**. The emphasis is on doing data engineering *correctly* — clean
layering, idempotent writes, and explicit data-quality gates.

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
- [Known Limitations](#known-limitations)
- [Roadmap](#roadmap)
- [Testing](#testing)
- [License](#license)

---

## Features

- **Daily EOD OHLCV ingestion** from the Schwab `/pricehistory` API for the Russell 3000.
- **Four-layer data validation** — three structural gates (HTTP + session date, field
  presence, internal consistency) that reject malformed bars, plus a statistical layer
  that **annotates rather than rejects**: abnormal bars are stored and flagged in
  `ohlcv_flag` with the metrics and the limits applied that day.
- **Delta Lake price store** (ACID, year-partitioned) on S3 via `delta-rs` — no Spark.
- **PostgreSQL metadata store** — universe membership, rolling volatility, a dead-letter
  queue, Layer-4 flags, the Schwab refresh token, and an operational audit log.
- **Survivorship-bias-aware universe maintenance** — delisted symbols are soft-deleted
  (an `exit_date` is set), never hard-deleted.
- **Two-tier Schwab token management** — short-lived access token cached in Redis,
  durable refresh token in PostgreSQL; a single-flight `SET NX` lock collapses the
  thundering herd of ~3000 concurrent cache misses into one refresh.
- **Distributed rate limiting** — an atomic Redis token bucket meters every Schwab call
  across all concurrent workers (~109 calls/min, burst of 1), with exponential-backoff
  429 handling that honors `Retry-After`. The limiter **fails closed**: no token, no call.
- **Delta Lake maintenance DAG** — periodic `OPTIMIZE` (small-file compaction) +
  `VACUUM` (tombstone cleanup) keeps the table healthy as daily merges accumulate.
- **Resilient external calls** — failed symbols routed to a dead-letter queue, not
  dropped, and cleared from it once they land in the price store; terminal vs. transient
  refresh failures are classified distinctly.
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
| OHLCV price history (large, one row per symbol per session) | **Delta Lake on S3** | Cheap columnar storage, ACID merges, scales |
| Membership, volatility, DLQ, flags, audit log, refresh token (small, relational) | **PostgreSQL** | Transactions, joins, point-in-time queries |

Derived facts flow from the price store back to metadata: the rolling stats are
*computed* from the last 21 sessions read out of Delta Lake, and only the resulting two
numbers per symbol (`rolling_sd`, `rolling_avg_volume`) are written to PostgreSQL.

**Runtime topology** — services communicate over an external Docker network,
`shared-network`:

```
┌───────────────────────  shared-network (external)  ─────────────────────┐
│                                                                          │
│   ohlcv-db            ohlcv-redis               LocalStack (S3)          │
│   (PostgreSQL)        (cache · lock · limiter)  (separate compose)       │
│                                                                          │
│   Airflow — apiserver · scheduler · dag-processor · triggerer            │
│      │      (LocalExecutor; also on this repo's private airflow-net)     │
│      └── airflow-db (Airflow metadata — airflow-net only)                │
└──────────────────────────────────────────────────────────────────────────┘
```

This repo provisions everything except LocalStack: the data plane (`ohlcv-db`,
`ohlcv-redis`) and a **dedicated single-project Airflow stack** (LocalExecutor, custom
image with the project's runtime deps baked in). Airflow's own components talk over a
private `airflow-net`, so its metadata DB never appears on `shared-network`. LocalStack
runs as a separate Compose project attached to the same `shared-network`.

---

## Tech Stack

| Concern | Technology |
|---------|-----------|
| Orchestration | Apache Airflow 3.1 (TaskFlow API, dynamic task mapping, LocalExecutor) |
| Language | Python 3.12 |
| Metadata DB | PostgreSQL 16 (`psycopg2`, `execute_values` for bulk ops) |
| Price store | Delta Lake via `deltalake` (delta-rs) + PyArrow |
| Object storage | LocalStack S3 in dev (AWS S3 in prod) |
| Token cache / lock / rate limiter | Redis (`SET NX` lock, atomic Lua token bucket) |
| Data source | Schwab API (`/pricehistory`) + CSV universe seed |
| Data processing | pandas, NumPy, `pandas_market_calendars` (NYSE session calendar) |
| Infra | Docker Compose |

---

## Project Structure

```
financial-data-lake/
├── airflow/
│   ├── dags/
│   │   ├── universe_maintenance.py    # weekly: maintain Russell 3000 membership
│   │   ├── market_data_pipeline.py    # daily: ingest + validate + store EOD OHLCV
│   │   ├── delta_maintenance.py       # periodic: Delta Lake OPTIMIZE + VACUUM
│   │   └── xcom_cleanup.py            # weekly: purge XComs >30 days from metadata DB
│   ├── Dockerfile                     # Airflow image + project runtime deps baked in
│   └── requirements.txt               # deps the DAGs pull in via src.*
├── src/
│   ├── clients/                       # external-system boundaries (no business logic)
│   │   ├── db_client.py               #   PostgreSQL (psycopg2, no internal commits)
│   │   ├── schwab_client.py           #   Schwab API /pricehistory + validation 1–3
│   │   ├── schwab_token.py            #   two-tier token: Redis cache + Postgres seed, SET NX
│   │   ├── schwab_limiter.py          #   RedisTokenBucket — distributed rate limiter (Lua)
│   │   ├── s3_client.py               #   Delta Lake on S3 (write / read / optimize / vacuum)
│   │   └── *_exception.py             #   db / schwab / s3 client error types
│   ├── services/                      # business logic; own DB transaction lifecycle
│   │   ├── universe_service.py
│   │   ├── ohlcv_service.py
│   │   ├── delta_maintenance_service.py  # storage-only: OPTIMIZE + VACUUM (no DB)
│   │   └── pipeline_exception.py
│   ├── transform/                     # pure functions (no I/O)
│   │   ├── universe_transform.py
│   │   └── ohlcv_transform.py         #   chunk, build_dataframe, dedup, rolling stats,
│   │                                  #   last_trading_date (NYSE calendar)
│   ├── config/config.py               # AppConfig — env-var loading
│   └── utilities/bootstrap.py         # wires AppConfig + clients + service
├── scripts/
│   ├── create_bucket.sh               # one-time LocalStack bucket provisioning
│   └── setup_python.sh                # local interpreter/env helper
├── stock_csv/tickers_sample.csv       # universe seed sample (real tickers.csv gitignored)
├── database_init.sql                  # schema: 8 tables + indexes + triggers
├── docker-compose.yaml                # data plane (ohlcv-db, ohlcv-redis) + Airflow stack
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
| `volatility_rolling` | Per-ticker rolling stats — `rolling_sd` (daily log-return std) and `rolling_avg_volume`, one row per symbol, both used by Layer 4 |
| `failed_ingestion` | Dead-letter queue — symbols for which **no usable bar** could be obtained (Layers 1–3 only). `failure` (enum `failure_mode`: `HTTP_ERROR` \| `VALIDATION`), `failure_error`, `attempts`, `review_required` (set at 3 attempts). **Current-state, one row per symbol** (unique on `symbol_id`); the row is deleted once the symbol reaches the price store |
| `pipeline_run` | Operational audit log — status `RUNNING` \| `SUCCESS` \| `FAILED` \| `PARTIAL`. Written by `universe_maintenance` and `market_data_pipeline` only; the two maintenance DAGs log nothing here |
| `ohlcv_flag` | Layer-4 annotations — one row per flagged (symbol, session): which rules tripped, the signed log return / open gap and volume, and the **limit each was compared against that day**. The bar itself is always in the price store |
| `schwab_token` | Durable Schwab refresh token — one row per provider (`UNIQUE`), `issued_at` anchors the 7-day expiry wall |

Highlights: a partial unique index enforces one active membership per symbol per universe
(`exit_date IS NULL`); `updated_at` triggers on mutable tables; an index on
`universe_membership(enter_date, exit_date)` supports point-in-time queries.

---

## Pipelines

### `universe_maintenance` (weekly, Sundays 00:00 UTC)
Keeps Russell 3000 membership current. Reads `stock_csv/tickers.csv` (columns
`ticker,universe`, where `universe` is the `universe.id` UUID), normalizes symbols to
slash notation (`BRK.B → BRK/B` — `_sanitize_symbol` replaces every character that is
not a letter, digit or whitespace with `/`), diffs against active DB membership, upserts
new symbols, and **soft-deletes** delistings by setting `exit_date` — preserving history
for survivorship-bias correctness. The CSV is the source of truth: an active symbol that
is missing from it is treated as delisted on the next run.

### `market_data_pipeline` (06:30 UTC, Tue–Sat — after each US trading day)
Nine tasks, with `fetch_ohlcv` fanned out via dynamic task mapping over chunks of 100
symbols:

```
log_pipeline_start → query_active_symbols → build_chunks
        → fetch_ohlcv (mapped per chunk)        # rate-limited Schwab fetch + validation 1–3
        → aggregate_results                      # flatten the mapped chunk results
        → statistical_validation                 # layer 4: flag abnormal bars, keep them all
        → write_to_delta_lake                    # dedup, year-partitioned merge, clear DLQ rows
        → update_volatility                      # recompute rolling stats → PostgreSQL
        → log_pipeline_end                       # SUCCESS / PARTIAL / FAILED
```

Each fetch asks `/pricehistory` for one year of daily candles and keeps only the newest
one. The schedule sits well after the close because Schwab publishes a session's daily
bar some hours later: a run at ~01:20 UTC still saw the previous session as newest and
failed Layer 1 for every symbol. `update_volatility` does nothing when no rows were
written, and `log_pipeline_end` runs even when upstream tasks fail (`all_done`), so
every run is closed out in `pipeline_run`.

That close-out is what normally records a failed run: with nothing written,
`log_pipeline_end` sets `FAILED` itself and leaves `notes` empty. The DAG-level
`on_failure_callback` is a backstop — it sets `FAILED` and stores the error in `notes`,
but only fires when Airflow marks the DAG run itself as failed, which the `all_done`
close-out usually prevents.

Symbols that fail fetch or
validation are written to the `failed_ingestion` dead-letter queue rather than silently
dropped, and their rows are deleted once the symbol next reaches the price store.
Failures that are not about one symbol — an expired refresh token, a failed token
refresh, or an unreachable rate limiter — abort the whole chunk task instead, since
retrying the next symbol cannot succeed either.

There is deliberately **no separate retry list**: the pipeline fetches the whole active
universe every run, so a symbol that fails today is refetched tomorrow as a matter of
course — today's run *is* the retry. `failed_ingestion` is therefore a monitoring
surface ("what is broken right now, and for how many runs"), not an input to the fetch
list. `log_pipeline_end` derives run status from symbols *attempted* versus rows
*written*: `SUCCESS` only when every attempted symbol was written, `PARTIAL` when any is
missing (or the DLQ logged a failure), `FAILED` when nothing was written. The DLQ count
can downgrade a run but never vouch for one — a DLQ that cannot accept writes reads as
zero failures, and would otherwise report a clean run.

Every Schwab call (retries included) first acquires a token from a shared Redis token
bucket, so all mapped tasks together stay on one metered drip (~109 calls/min → ~27 min
for the full universe). A 429 triggers exponential backoff with jitter, honoring the
`Retry-After` header when present; a symbol still throttled after 4 attempts goes to the
DLQ instead of being hammered further.

### `delta_maintenance` (every 25 days)
Two tasks, strictly ordered: `OPTIMIZE` (bin-packing compaction of the small files the
daily merges create) then `VACUUM` (physical deletion of tombstoned files older than the
168-hour retention window). Retention is a time-travel / in-flight-reader safety window,
not a business data-retention knob — `VACUUM` never touches the current table version,
so the 20-day volatility lookback is unaffected.

### `xcom_cleanup` (weekly, Sundays 01:00 UTC)
XComs are ephemeral inter-task plumbing, and the daily pipeline pushes sizeable
aggregate/validation payloads through them — left alone, the `xcom` table grows without
bound. This DAG purges rows older than 30 days through the sanctioned
`airflow db clean --tables xcom` CLI (Airflow 3's Task SDK forbids direct ORM access to
the metadata DB from task code), with `--skip-archive` so the space is actually reclaimed
rather than copied into archive tables. Scope is deliberately XCom-only: task and run
history stays for the UI, and the durable audit trail is `pipeline_run` in the project
DB, never XCom.

---

## OHLCV Validation

Layers 1–3 decide whether a response **is a bar at all**; Layer 4 records whether a bar
**looks unusual**. Only the first kind of question can reject data.

| Layer | Check | On failure |
|-------|-------|-----------|
| 1 — HTTP | Status 200, non-empty candles, newest candle date == last NYSE session before today (UTC) | DLQ (`HTTP_ERROR`; wrong date → `VALIDATION`) |
| 2 — Fields | `open/high/low/close/volume` present and positive | DLQ (`VALIDATION`) |
| 3 — Consistency | `high ≥ open,close,low` and `low ≤ open,close` | DLQ (`VALIDATION`) |
| 4 — Statistical | 5σ log return, 3σ open-vs-prev-close gap, 10× volume spike | **Bar kept**; row in `ohlcv_flag` |

**Why Layer 4 annotates instead of rejecting.** Rejecting on a statistical judgment
destroyed data irreversibly — the rejected bar was never stored — and forced one
definition of "outlier" on every future consumer (a momentum model *wants* the 40%
move; a risk model wants it flagged). It also trapped real moves: once a genuine jump
was rejected, the stored previous close stayed stale and every later day was rejected
against it. The price store now records what the source said; cleaning is left to
downstream consumers, who get an honest note of what looked abnormal.

The flag is computed **only from sessions before the bar**, so it is point-in-time safe
for backtests. Cleaning the full history after the fact often isn't: centred windows, or
spotting a bad tick because "it reverted the next day", quietly use future data
(look-ahead bias).

Layer 4 uses each ticker's **daily** log-return standard deviation (`rolling_sd`, sample
`ddof=1`) as the threshold on a single-day log return — so the stored volatility is
deliberately *not* annualized. Both the return and the gap are measured as **log ratios**,
matching `rolling_sd`'s units: comparing a dollar difference against a dimensionless
standard deviation is a units error that would flag most of the universe (when Layer 4
still rejected, it silently dropped about 85% of it).

The volume multiple (10×) is deliberately loose — this flags suspected bad ticks, and earnings
or index rebalances routinely run 3–5× without the price data being wrong.

A symbol with no rolling baseline yet (a recent listing, or the first run for that ticker)
has **no** statistical check applied. Those records are written, but counted separately as
`unvalidated_count` so a run cannot report a clean bill of health it never earned. The
return and gap checks also need a stored previous close; the volume check does not, so a
symbol with rolling stats but no previous close still gets that one.

---

## Getting Started

### Prerequisites

- [Conda](https://docs.conda.io/) (Miniconda/Anaconda)
- Docker + Docker Compose
- The external `shared-network` Docker network, with the **LocalStack** stack running
  on it (Airflow ships in this repo's own Compose stack):
  ```bash
  docker network create shared-network   # if it does not already exist
  ```

### 1. Environment (host-side runs only)

```bash
conda env create -f setup.yaml
conda activate financial_data_lake
```

The Compose stack does not use this environment — the Airflow image carries its own
copy of the dependencies (`airflow/requirements.txt`). You need it only to run project
code on the host.

### 2. Configuration

```bash
cp .env.example .env
# then edit .env — set DB_PASSWORD, your Schwab credentials, and generate
# AIRFLOW__CORE__FERNET_KEY + AIRFLOW__API_AUTH__JWT_SECRET (one-liners are
# in the .env.example comments)
```

### 3. Infrastructure

```bash
docker-compose up -d    # data plane + Airflow (first boot: airflow-init migrates
                        # the metadata DB and creates the web UI admin user)
```

### 4. Database schema (first boot only)

```bash
set -a; source .env; set +a             # DB_USER / DB_DB into this shell
docker exec -i financial-data-lake-ohlcv-db-1 \
  psql -U "$DB_USER" -d "$DB_DB" < database_init.sql
```

The script is for an empty database: `CREATE TYPE`, the triggers and some indexes have
no `IF NOT EXISTS`, so a second run errors. There is no migration tool yet — later schema
changes are applied by hand.

### 5. Delta Lake bucket (one-time)

```bash
bash scripts/create_bucket.sh           # provisions s3://ohlcv in LocalStack
```

The script runs `awslocal` inside a container named `localstack`. `DELTA_BUCKET`
overrides the bucket name — keep it in step with `DELTA_TABLE_URI`.

### 6. Universe seed

`database_init.sql` creates no universe rows, and the real ticker list is gitignored.
Register the universe, then build `stock_csv/tickers.csv` in the same shape as
[`stock_csv/tickers_sample.csv`](stock_csv/tickers_sample.csv), with the returned id in
the `universe` column:

```sql
INSERT INTO universe (name) VALUES ('Russell 3000') RETURNING id;
```

```
ticker,universe
AAPL,<universe id from above>
BRK.B,<universe id from above>
```

### 7. Schwab refresh token

The pipeline reads the refresh token from the `schwab_token` table; it never writes it.
The browser login that produces a token (Schwab's OAuth authorization-code flow) is
**not part of this repo**. Once you have a refresh token, store it — in an interactive
`psql` session, so the token lands in neither a file nor your shell history:

```sql
INSERT INTO schwab_token (refresh_token, issued_at)
VALUES ('<refresh token>', NOW())
ON CONFLICT (provider) DO UPDATE
SET refresh_token = EXCLUDED.refresh_token, issued_at = EXCLUDED.issued_at;
```

`issued_at` starts the 7-day clock. Repeat this every week; once the token passes its
wall, every chunk aborts with `SchwabAuthExpiredError` until you do.

---

## Configuration

`AppConfig` (`src/config/config.py`) reads plain environment variables — it does not
open `.env` itself. Inside the stack, Compose injects `.env` into every Airflow container
(`env_file`); on the host, export it first (`set -a; source .env; set +a`). See
[`.env.example`](.env.example) for the full template.

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_USER` / `DB_PASSWORD` / `DB_DB` | PostgreSQL credentials + database | — |
| `DB_HOSTNAME` | DB host (`ohlcv-db` in-network, `localhost` from host); port is fixed at 5432 | `localhost` |
| `REDIS_HOST` / `REDIS_PORT` | Redis token cache, refresh lock and rate limiter | `localhost` / `6379` |
| `SCHWAB_CLIENT_ID` / `SCHWAB_CLIENT_SECRET` | Schwab OAuth client credentials | — |
| `SCHWAB_BASE_URL` | Schwab API base URL | `https://api.schwabapi.com` |
| `SCHWAB_CALLS_PER_MIN` | Rate limiter: long-run **average** call rate | `109` |
| `SCHWAB_LIMITER_CAPACITY` | Rate limiter: max **burst** (banked tokens) | `1` |
| `S3_ENDPOINT_URL` | S3 endpoint (`localstack:4566` in-network) | `http://localstack:4566` |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | S3 credentials (`test`/`test` for LocalStack) | `test` |
| `AWS_REGION` | AWS region | `us-east-1` |
| `DELTA_TABLE_URI` | Delta table path | `s3://ohlcv/prices` |
| `AIRFLOW_UID` | Host UID for bind-mounted log ownership (Linux: `id -u`) | `50000` |
| `AIRFLOW__CORE__FERNET_KEY` | Encrypts Airflow Connections/Variables at rest | — |
| `AIRFLOW__API_AUTH__JWT_SECRET` | Signs tokens between Airflow components | — |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Web UI admin login (created by `airflow-init`) | `airflow` |

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

Airflow ships in this repo's Compose stack: `docker-compose up -d` starts it with the
project mounted at `/opt/project`, `PYTHONPATH` preset (the DAGs import `src.*`), and
DAGs loading from `airflow/dags/`.

Open the web UI at **http://localhost:8080** (login: `_AIRFLOW_WWW_USER_*` from `.env`).
DAGs are **paused at creation** — unpause them, then trigger `universe_maintenance` once
to seed membership (it needs step 6 of Getting Started), followed by
`market_data_pipeline` (it needs step 7). Avoid triggering a DAG while a run of it is still in flight — see
[Known Limitations](#known-limitations).

Code changes need no rebuild: the project is bind-mounted, every task runs in a fresh
process, and the DAG processor re-parses within about two minutes. Rebuild the image
(`docker-compose build`) only when `airflow/requirements.txt` changes.

For lightweight local iteration you can instead run Airflow standalone on the host
(using the `localhost` config overrides). `setup.yaml` does **not** install Airflow, so
install `apache-airflow==3.1.8` into the environment first (with Airflow's constraints
file):

```bash
set -a; source .env; set +a          # AppConfig reads the environment, not .env
export AIRFLOW_HOME=$(pwd)/airflow
export PYTHONPATH=$(pwd)
airflow standalone
```

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

Columns: `symbol` (string), `date` (timestamp, the session date), `open` / `high` /
`low` / `close` (float64), `volume` (int64), and `year` (int32, the partition key).

Year partitioning (rather than daily) keeps file sizes healthy and avoids the
small-files problem. Writes are an **idempotent `MERGE` on (symbol, date)**, so a task
retry or a re-triggered run overwrites the matching rows instead of double-writing a day
(only the very first write, which has no table to merge into, is a plain append). The
source frame is de-duplicated before the merge, since delta-rs raises if two source rows
match one target row. The `delta_maintenance` DAG periodically runs `OPTIMIZE` + `VACUUM`
to compact the daily files and reap the resulting tombstones.

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
- **Distributed token-bucket rate limiter** (`RedisTokenBucket`) — Schwab's GET rate
  limit is undocumented (observed: ~50 unthrottled pulls → 429s, sustained hammering →
  temporary 403 ban), so every call is metered through one shared bucket in Redis. The
  whole read → refill → check → spend sequence is a single atomic Lua script, and the
  clock is Redis's own `TIME` — N workers carry N skewed clocks, and a fast local clock
  stamping shared state would mint phantom tokens. The two dials are independent: the
  refill period caps the long-run **average** (109/min leaves headroom under the
  community-consensus ~120/min ceiling) while capacity caps the **burst** (1 = a
  perfectly smooth drip; a spike is impossible by construction). Blocked workers sleep
  the script-returned wait plus jitter so they don't wake in lockstep. The limiter
  **fails closed** — Redis down or acquire timeout aborts the chunk rather than calling
  unmetered, because an unthrottled retry herd is exactly what escalates 429s into a ban.
- **Delta maintenance is storage-only** — `DeltaMaintenanceService` holds no `DBClient`
  and no context manager: `OPTIMIZE`/`VACUUM` touch only the Delta-on-S3 boundary, so
  there is no PostgreSQL transaction lifecycle to own.
- **Clients never commit _or roll back_** — transaction control belongs to the service
  layer's `__exit__` (commit on success, rollback on exception). A client-side rollback
  discards work the service has already staged, so `DBClient` exposes a `savepoint()`
  context manager instead: Postgres aborts the whole transaction on any statement error,
  and a savepoint is what lets one failed per-symbol write be caught without poisoning
  the rest of the batch.
- **Soft deletes only** — delistings set `exit_date`; price points are never zeroed.
- **Parameterized SQL only** — no f-string interpolation, ever.

---

## Known Limitations

- **No backfill.** Each fetch keeps only the newest candle, and Layer 1 compares it with
  the last NYSE session before the *wall-clock* date, not the Airflow logical date.
  Re-triggering a missed day's run therefore fetches whatever session is newest at that
  moment; a missed session cannot be recovered by re-running it.
- **Rolling stats hold only the latest value.** `volatility_rolling` is overwritten
  nightly. `ohlcv_flag` stores the limit applied on the day, but re-evaluating an old
  session would compare it with today's stats — information from after that session.
- **One shared run id.** Tasks read the current run's id from a single Airflow Variable,
  so two overlapping runs of the same DAG overwrite each other's id. Safe while runs never
  overlap (`catchup=False`, no manual trigger during a run).
- **Hand-maintained universe.** `tickers.csv` is updated by hand; an automated import
  from the iShares IWV holdings file is planned.
- **Local development only.** Redis has no password and publishes port 6379 on every
  interface, and Airflow's metadata DB uses `airflow`/`airflow`. Do not run this stack on
  a shared or public network as-is.

---

## Roadmap

- [x] PostgreSQL schema, DBClient, UniverseService, universe-maintenance DAG
- [x] Schwab API client with Redis token management
- [x] `market_data_pipeline` DAG (dynamic task mapping, failure handling)
- [x] Delta Lake write layer + rolling-volatility computation
- [x] Distributed Schwab rate limiter (Redis token bucket + 429 backoff)
- [x] `delta_maintenance` DAG — periodic `OPTIMIZE` + `VACUUM`
- [x] `xcom_cleanup` DAG — weekly XCom purge via `airflow db clean`
- [x] First end-to-end run with live Schwab credentials
- [x] Layer 4 as annotation (`ohlcv_flag`) instead of rejection
- [ ] Automated test suite (framework not yet chosen — see [Testing](#testing))
- [ ] Backfill of missed sessions (see [Known Limitations](#known-limitations))
- [ ] Automated universe refresh from the iShares IWV holdings file
- [ ] Point-in-time correctness / historical constituent snapshots

---

## Testing

There is **no automated test suite yet** — `setup.yaml` pins no test framework and the
tooling choice is deliberately still open. This is the project's largest known gap.

Verification today is manual and evidence-based: changes are exercised against the live
LocalStack / PostgreSQL stack using read-only probes, or inside a transaction that is
rolled back afterwards, and pipeline health is read back from `pipeline_run` and the DAG
logs.

Two bugs found in September 2026 are the argument for the suite — each would have been
caught in seconds by a single unit test:

- **`open_gap` units** — the open-vs-previous-close gap was compared as a dollar
  difference against a dimensionless log-return standard deviation, flagging 2455 of
  2902 symbols on an ordinary trading day.
- **`pipeline_end` status** — `SUCCESS` was derived from a dead-letter-queue count that
  read zero precisely when the DLQ writes themselves were failing, so five consecutive
  runs reported success while the daily write shrank from 3010 rows to 614.

First tests to write, in priority order: `run_statistical_validation` (threshold units,
and the no-baseline path), `pipeline_end` (the full status matrix),
`compute_rolling_volatility` (insufficient-history skip), then the transform functions —
pure, no mocks required.

---

## License

No formal license is attached. This project is not intended for redistribution or
production use.
