# My Quant Journey Handbook

A plain-English log of what I learned each coding session. No jargon without
explanation. Future me reads this to remember *why*, not just *what*.

---

## 2026-06-05 — Redis ownership & first look at Delta Lake

### Lesson 1: Run your own Redis. Don't borrow Airflow's.

**What Redis does for us:** Redis is a fast in-memory store (think: a notebook
the program keeps in RAM). We use it for two things:
1. Cache the Schwab login token so we don't re-login every call.
2. A **lock** — a "do not disturb" sign so two workers don't try to refresh the
   token at the exact same moment.

**The tempting shortcut:** Airflow already runs its own Redis (it uses it as a
task queue). Why not just reuse it and save a container?

**Why that's a trap:** Airflow's Redis is tuned for Airflow's needs. When it
gets full, it's allowed to **throw away old data to make room** — this is called
*eviction*. It could delete *our* token, or worse, *our lock*, at any time,
without telling us.

- Losing the token = no big deal. We just log in again.
- Losing the **lock** = real bug. The lock is the *only* thing stopping two
  workers from hammering the API at once. If it silently vanishes mid-job, both
  workers think they have permission. That kind of bug only shows up under load
  and is miserable to track down.

**The rule I learned:** If something is a *correctness* tool (a lock, a counter
that must be exact), give it its own home where *you* control the rules. Don't
put it somewhere a neighbor might wipe it.

**What we did:** Made our own `ohlcv-redis` container with eviction turned off.
Now our lock always lives its full life. We also gave it a unique name so it
can't get confused with Airflow's Redis on the shared network.

---

### Lesson 2: First time writing Delta Lake code

**The problem:** We need to store millions of daily price rows somewhere cheap
and reliable. We picked **Delta Lake**.

**What Delta Lake actually is (the thing that clicked):**
It is **not** a database server. It's just a folder of files plus a logbook.

- The data lives in **Parquet** files (Parquet = a compact file format that
  stores data by column — great for big analytics data).
- On its own, a pile of files is messy: which files are current? Did a write
  finish or crash halfway?
- Delta Lake adds a **transaction log** — a little folder called `_delta_log`
  that records every change: "added these files," "removed those." It's like a
  bank ledger for your data files.

**Why the log matters:** It gives you **ACID** — safe, all-or-nothing writes. If
a write crashes halfway, the log never records it, so readers never see broken,
half-written data. The log is the single source of truth for "what's really in
the table right now."

**The library:** `deltalake` (also called *delta-rs*). It's written in Rust, so
**no Spark, no Java** needed — just a Python import. The whole thing was simpler
than I expected. The core is basically two calls:

```python
from deltalake import write_deltalake, DeltaTable

# WRITE: add today's rows. Creates the table on first write.
write_deltalake(
    "s3://ohlcv/prices",      # where the folder of files lives
    df,                        # a pandas DataFrame
    mode="append",             # add to what's there, don't overwrite
    partition_by=["year"],     # group files into year=2026/ folders
    storage_options={...},     # how to reach S3 (endpoint + keys)
)

# READ: get it back as a DataFrame
df = DeltaTable("s3://ohlcv/prices", storage_options={...}).to_pandas()
```

**Partitioning by year:** Instead of one giant pile, files are grouped into
folders like `year=2026/`. When we only need recent data, we read just that
folder instead of scanning everything. Faster and cheaper.

**Where it's stored:** On S3 (we use LocalStack to fake S3 on the laptop). Delta
Lake talks to S3 directly — that's the whole point: a "table" that lives on
plain cheap object storage, no database server running.

**One subtle but important bit:** the data store (Delta Lake) holds the price
*history*. PostgreSQL only holds small *summary* numbers we calculate from it
(like a stock's recent volatility). So to compute volatility, we *read* the last
21 days of prices back out of Delta Lake, do the math, and save just the one
result number to Postgres. Big raw data → Delta Lake. Small derived facts →
Postgres.

---

### One-line takeaways
- **Locks belong in a Redis you control** — a borrowed cache can delete them.
- **A Delta Lake table = Parquet files + a transaction log**, living on S3. No
  server, no Spark.
- **Raw history → Delta Lake; small computed numbers → Postgres.**
