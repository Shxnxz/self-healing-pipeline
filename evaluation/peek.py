"""Print a few rows of a pipeline table (runs in the `tools` service, for the demo).

  docker compose -f docker-compose.yml -f docker-compose.eval.yml run --rm tools \
      python -m evaluation.peek gold --where "crash_record_id LIKE 'FAULT-%'" --columns crash_record_id,crash_month
"""
import argparse
import sys

from tools import session


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m evaluation.peek")
    parser.add_argument("table", help="a table under DELTA_ROOT, e.g. gold, gold_daily_summary, silver2_quarantine")
    parser.add_argument("--where", help="a Spark SQL filter")
    parser.add_argument("--columns", help="comma separated")
    parser.add_argument("--order", help="a Spark SQL ORDER BY expression")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--count", action="store_true", help="only count the matching rows")
    args = parser.parse_args(argv)

    spark = session.spark("Peek")
    from sources import shared_path

    path = shared_path(args.table)
    if not session.is_delta(path):
        print(f"no table at {path}", file=sys.stderr)
        return 2
    df = spark.read.format("delta").load(path)
    if args.where:
        df = df.filter(args.where)
    if args.count:
        print(f"{args.table}: {df.count()} row(s)" + (f" where {args.where}" if args.where else ""))
        return 0
    if args.columns:
        df = df.select(*[c.strip() for c in args.columns.split(",") if c.strip()])
    if args.order:
        df = df.orderBy(*[c.strip() for c in args.order.split(",")])
    df.show(args.limit, truncate=40)
    return 0


if __name__ == "__main__":
    sys.exit(main())
