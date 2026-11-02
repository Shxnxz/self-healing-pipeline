import sys
import os
# pyrefly: ignore [missing-import]
import duckdb

SOURCES = ["crash1", "crash2", "crash3", "crash4"]  # spark-jobs/sources.py

# Each data folder has its own Bronze / Silver 1 / Silver 2 table (bronze_crash1, ...);
# Gold is the UNION ALL of the Silver 2 tables. "bronze", "silver1" and "silver2" read
# every source's table at once. Note: globbing part files can include files Delta has
# logically removed (after OPTIMIZE / overwrite); read_parquet_v3.py reads only the
# current snapshot.
LAYERS = {
    **{
        f"{layer}_{source}": f"./delta/{layer}_{source}/part-*.parquet"
        for layer in ("bronze", "silver1", "silver2")
        for source in SOURCES
    },
    "bronze": "./delta/bronze_crash*/part-*.parquet",
    "silver1": "./delta/silver1_crash*/part-*.parquet",
    "silver2": "./delta/silver2_crash*/part-*.parquet",
    "gold": "./delta/gold/part-*.parquet",
    "quarantine": "./delta/silver2_quarantine/part-*.parquet",
    "dead_letter": "./delta/silver2_dead_letter/part-*.parquet",
    "audit": "./delta/silver2_fuzzy_audit/part-*.parquet",
    "ref": "./delta/ref_canonical_mappings/part-*.parquet",
    # Legacy alias
    "silver": "./delta/silver1_crash*/part-*.parquet",
}

def main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "gold"

    # Check if the user passed a direct path to a file (e.g. delta/gold_car_overview/part-....parquet)
    if os.path.exists(arg) or arg.endswith(".parquet"):
        parquet_pattern = arg
        target_name = arg
    elif arg in LAYERS:
        parquet_pattern = LAYERS[arg]
        target_name = arg
    else:
        print(f"Unknown argument '{arg}'. Provide a table ({', '.join(LAYERS)}) or a direct path to a .parquet file.")
        sys.exit(1)

    print(f"\n--- Reading: {target_name} ---")

    try:
        # union_by_name: quarantine / audit were written with mergeSchema, and
        # "bronze" etc. read several tables at once
        source = f"read_parquet('{parquet_pattern}', union_by_name = true)"
        query = f"SELECT * FROM {source}"

        # Check if user wants all rows or a custom limit
        show_all = "--all" in sys.argv or "-a" in sys.argv or "all" in sys.argv

        # Determine row limit
        limit = None
        for a in sys.argv[2:]:
            if a.isdigit():
                limit = int(a)
                break

        # 1. Preview data
        if show_all:
            print("\n=== Full Data (All Rows & Columns) ===")
            duckdb.sql(query).show(max_rows=100000, max_width=10000)
        elif limit:
            print(f"\n=== Data Preview (Top {limit} rows) ===")
            duckdb.sql(query).limit(limit).show(max_rows=limit, max_width=10000)
        else:
            print("\n=== Data Preview (Top 10 rows) ===")
            duckdb.sql(query).limit(10).show(max_rows=10, max_width=10000)
            print("(Tip: Run with '--all' to view all rows, or specify a number: python3 read_parquet.py gold 20)")
            print("(Tables: bronze_crash1 .. silver2_crash4, bronze/silver1/silver2 for all sources, gold, quarantine.)")

        # 2. Get total row count
        count = duckdb.sql(f"SELECT COUNT(*) FROM {source}").fetchall()[0][0]
        print(f"Total rows in {target_name}: {count}")

        # 3. Convert to Pandas DataFrame for further Python processing
        df = duckdb.sql(query).df()
        return df

    except duckdb.IOException:
        print(f"\n[!] No Parquet files found at: {parquet_pattern}")
        print("Note: If the pipeline hasn't run yet, start it with `docker compose up` first to generate the data.")
    except Exception as e:
        print(f"\nError reading parquet files: {e}")

if __name__ == "__main__":
    main()
