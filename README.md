# Project Setup
This project is a local analytics warehouse built on **DuckDB + dbt (dbt-core + dbt-duckdb)**. Raw CSV drops are loaded append-only into a `raw` schema (with `ingested_at` + `source_file`), then dbt builds incremental `stg` and `int` layers, runs **dbt snapshots** for SCD2 dimensions, and produces `mart` facts/marts for MRR reporting. To run it, create a Python `venv`, `pip install dbt-core dbt-duckdb` (and `dbt deps` if packages are used), configure `~/.dbt/profiles.yml` with a DuckDB `path` (e.g. `warehouse.duckdb`), then execute `dbt build` from the repo root; the resulting DuckDB file can be queried via DuckDB CLI or DBeaver.

# DuckDB + dbt MRR Warehouse

A small, finance-focused local analytics warehouse built on **DuckDB + dbt (dbt-core + dbt-duckdb)** from messy CSV drops.

Raw CSV drops are loaded **append-only** into a `raw` schema (batch lineage preserved with `ingested_at` + `source_file`). Then dbt builds:

- `stg` - incremental append / arrival log
- `int` - latest-row “current truth”
- **dbt snapshots** - SCD2 history for dimensions
- `mart` - facts/marts for MRR reporting (Power BI–ready star + CFO-style waterfall)

---

## What it builds

**Star schema for MRR reporting**

- `dim_date` - conformed calendar
- `dim_account` - SCD2 (versioned)
- `dim_subscription` - SCD2 (versioned)
- `fct_subscription_month` - subscription-grain monthly MRR snapshot (EOM)
- `fct_account_month` - account-level monthly movements (new/reactivation/upgrade/downgrade/churn)
- `mart_mrr_waterfall_month` - month-level CFO waterfall rollup

---

## Data sources

Used for finance marts:

- `raw.accounts`
- `raw.subscriptions`

Present but not used (currently):

- `raw.feature_usage`
- `raw.support_tickets`
- `raw.churn_events`

---

## Layer meanings (mental model)

- `stg_subscriptions` / `stg_accounts`: **arrival log** (append-only by `ingested_at`, includes `record_hash`)
- `int_*_current`: **current truth** (latest record per natural key by `ingested_at`)
- `snap_*` / `dim_*`: **SCD2 history** of warehouse-observed changes

---

## Business assumptions

- **MRR definition:** `mrr_amount` is monthly recurring run-rate while active (not cash / not refunds).
- **Snapshot semantics:** MRR is measured as an **end-of-month (EOM) snapshot** (active on the last day of the month).
- **Trials:** trial subscriptions contribute **0 MRR**.
- **Source of truth for movements:** movements are derived from **subscriptions**, not churn events or support tickets.
- **No proration:** if active at EOM → full `mrr_amount`, otherwise 0.
- **Account movements (month-over-month):**
  - New: 0 → >0 and no prior paid history
  - Reactivation: 0 → >0 with prior paid history
  - Churn: >0 → 0 (can happen multiple times)
  - Expansion: >0 → >0 and Δ > 0
  - Contraction: >0 → >0 and Δ < 0
- **Warehouse-observed history:** SCD2 reflects what was ingested and when; late corrections may restate history.

---

## Incremental + finance safety

Monthly facts are built with `delete+insert` and a rolling restatement window (`reprocess_months`, default `2`).

I tried settip up “tripwire” audit/test fails if the latest ingestion batch introduces changes that would affect months older than the window, forcing a wider restatement run (not working yet, so not part of the first commit).

---

## Setup

### 1) Create a Python virtual environment and install dbt

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install dbt-core dbt-duckdb
````

### 2) Configure `~/.dbt/profiles.yml`

```yml
oxylabs_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: warehouse.duckdb
      threads: 1
```

### 3) Install packages (if `packages.yml` exists)

```bash
dbt deps
```

---

## Running

### First time (clean build)

```bash
dbt build --full-refresh
```

### Normal run

```bash
dbt build
```

### If the audit fails (need deeper restatement)

If the “restatement check” fails, it means the latest data that was ingested changes older months than the ur normal rebuild window covers. In that case, rerun dbt and tell it to recompute more months of history (e.g. the last 6 months instead of the default 2) so the MRR numbers and movements get corrected:

Increase the window:

```bash
dbt build --vars '{reprocess_months: 6}'
```

---

## Raw CSV loading

CSV ingestion is handled by:

```bash
python scripts/load_csvs_to_duckdb.py --mode replace
# or
python scripts/load_csvs_to_duckdb.py --mode append
```