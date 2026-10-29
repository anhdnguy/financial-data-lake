# Financial Data Lake

End-of-day prices (OHLCV: open, high, low, close, volume) for the Russell 3000, pulled
from the Schwab API and run by Airflow. Prices go to Delta Lake on S3 (LocalStack);
everything else — the symbol list, rolling stats, failures, run log — goes to PostgreSQL.

## DAGs

### `universe_maintenance` — weekly, Sun 00:00 UTC
Keeps the list of symbols to fetch.
- reads `stock_csv/tickers.csv` and rewrites symbols to slash form (`BRK.B` → `BRK/B`)
- adds new symbols
- a symbol missing from the CSV is marked delisted (its `exit_date` is set) — never deleted

### `market_data_pipeline` — daily, Tue–Sat 06:30 UTC
Fetches the last session's bar for every active symbol and stores it.
- symbols are split into chunks of 100 and fetched in parallel; all chunks share one
  Redis rate limiter (~109 calls/min, ~27 min for the whole universe)
- each bar must pass three checks: right session date, all fields present and positive,
  high/low consistent. A bar that fails goes to `failed_ingestion` (the dead-letter
  queue) and is simply fetched again on the next run
- unusual bars (5σ return, 3σ open gap, 10× volume) are still written, and noted in
  `ohlcv_flag`
- bars are merged into Delta Lake on (symbol, date), so a re-run never writes a day twice
- rolling stats (`rolling_sd`, `rolling_avg_volume`) are recomputed from the last 21
  closes and saved to PostgreSQL — the next run's checks use them
- every run ends with a row in `pipeline_run`: `SUCCESS`, `PARTIAL` or `FAILED`

The Schwab access token (~30 min) is cached in Redis. The refresh token (7 days) lives
in the `schwab_token` table and must be renewed by hand every week.

### `delta_maintenance` — every 25 days
`OPTIMIZE` (compacts the small daily files), then `VACUUM` (deletes files no longer used).

### `xcom_cleanup` — weekly, Sun 01:00 UTC
Deletes Airflow XCom rows older than 30 days, so Airflow's own database doesn't grow
forever.

## Run it
Prerequisites:
- Docker
- the external network: `docker network create shared-network`
- LocalStack running on that network
- a `.env` copied from `.env.example`, with `DB_PASSWORD`, `SCHWAB_CLIENT_ID`,
  `SCHWAB_CLIENT_SECRET`, `AIRFLOW__CORE__FERNET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET`
  filled in

```bash
docker-compose up -d
```
Starts PostgreSQL, Redis and Airflow.

First time only:
```bash
set -a; source .env; set +a
docker exec -i financial-data-lake-ohlcv-db-1 \
  psql -U "$DB_USER" -d "$DB_DB" < database_init.sql   # create the tables
bash scripts/create_bucket.sh                          # create s3://ohlcv in LocalStack
```
Then, in `psql`:
- register the universe with `INSERT INTO universe (name) VALUES ('Russell 3000') RETURNING id;`
  and build `stock_csv/tickers.csv` like `stock_csv/tickers_sample.csv`, using that id
- store the Schwab refresh token (the OAuth login that produces it is not in this repo).
  Repeat weekly:
  ```sql
  INSERT INTO schwab_token (refresh_token, issued_at) VALUES ('<refresh token>', NOW())
  ON CONFLICT (provider) DO UPDATE
  SET refresh_token = EXCLUDED.refresh_token, issued_at = EXCLUDED.issued_at;
  ```

Open Airflow at http://localhost:8080 (login from `_AIRFLOW_WWW_USER_*` in `.env`),
unpause the DAGs, run `universe_maintenance` once, then `market_data_pipeline`.

### Check it works
Latest runs and their outcome:
```sql
SELECT dag_id, run_date, status, records_processed, records_failed
FROM pipeline_run ORDER BY started_at DESC LIMIT 5;
```
Symbols failing right now: `SELECT * FROM failed_ingestion;`

## Caveats
- **Local development only.** Redis has no password and Airflow's database uses
  `airflow`/`airflow`. Don't run this on a shared network as-is.
- **No backfill.** Each run keeps only the newest bar, so a missed session can't be
  recovered by re-running it.
- **Don't trigger a DAG while it's already running** — overlapping runs share one run id.
