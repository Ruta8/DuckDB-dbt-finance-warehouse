import argparse
from pathlib import Path
from datetime import datetime, timezone

import duckdb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default="db/warehouse.duckdb")
    p.add_argument("--csv-dir", default="data")
    p.add_argument("--schema", default="raw")
    # allow either overwrite (replace) or preserve history (append)
    p.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="replace = overwrite raw tables each run; append = keep all batches by inserting rows",
    )
    args = p.parse_args()

    db_path = Path(args.db)
    csv_dir = Path(args.csv_dir)
    schema = args.schema
    mode = args.mode

    csvs = sorted(csv_dir.glob("*.csv"))
    if not csvs:
        raise SystemExit(f"No .csv files found in: {csv_dir}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(db_path.as_posix())
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    batch_ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


    for csv in csvs:
        table = csv.stem
        full_table = f"{schema}.{table}"

        if mode == "replace":
            con.execute(
                f"""
                CREATE OR REPLACE TABLE {full_table} AS
                SELECT
                    *,
                    CAST('{batch_ingested_at}' AS TIMESTAMP) AS ingested_at,  -- CHANGED
                    '{csv.name}' AS source_file                               -- CHANGED
                FROM read_csv_auto('{csv.as_posix()}', header=true);
                """
            )
        else:
            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {full_table} AS
                SELECT
                    *,
                    CAST('{batch_ingested_at}' AS TIMESTAMP) AS ingested_at,  -- CHANGED
                    '{csv.name}' AS source_file                               -- CHANGED
                FROM read_csv_auto('{csv.as_posix()}', header=true)
                LIMIT 0;
                """
            )

            # Insert this  rows as a new batch
            con.execute(
                f"""
                INSERT INTO {full_table}
                SELECT
                    *,
                    CAST('{batch_ingested_at}' AS TIMESTAMP) AS ingested_at,  -- CHANGED
                    '{csv.name}' AS source_file                               -- CHANGED
                FROM read_csv_auto('{csv.as_posix()}', header=true);
                """
            )

        rows = con.execute(f"SELECT COUNT(*) FROM {full_table}").fetchone()[0]
        print(f"Loaded {csv.name} -> {full_table} ({rows} rows total, mode={mode})")

    con.close()
    print(f"\nDone. DuckDB at: {db_path.resolve()}")


if __name__ == "__main__":
    main()
