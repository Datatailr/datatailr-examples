"""CLI: list Parquet keys in blob or run DuckDB SQL (Blob ls/get under the hood)."""

from __future__ import annotations

import argparse
import os
import sys

from stock_price_processing.lake_query.reader import (
    default_blob_prefix,
    iter_dataset_parquet_keys,
    load_parquet_keys_arrow,
    normalize_key_path,
    query_lake_sql,
)


def _blob():
    try:
        from datatailr import Blob

        return Blob()
    except ImportError as e:
        raise SystemExit(
            "datatailr is required for blob access (install on the platform or dev env)."
        ) from e


def main() -> None:
    p = argparse.ArgumentParser(description="Query stock_price_lake Parquet via Blob (dt blob ls/get).")
    p.add_argument(
        "--prefix",
        default="",
        help="Blob prefix (default: COLLECTOR_BLOB_PREFIX or stock_price_lake)",
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    ls_p = sub.add_parser("ls", help="List .parquet keys under prefix/dataset with optional partition filters")
    ls_p.add_argument(
        "dataset",
        nargs="?",
        default="analytics",
        help="Dataset under prefix (analytics or market_events)",
    )
    ls_p.add_argument("--dt", default="", help="Partition date, e.g. 2026-03-20")
    ls_p.add_argument("--hour", default="", help="Partition hour, e.g. 15")
    ls_p.add_argument("--max", type=int, default=500, help="Max keys to print")

    head_p = sub.add_parser("head", help="Load first N parquet files into Arrow and print row count + schema")
    head_p.add_argument(
        "dataset",
        nargs="?",
        default="analytics",
        help="Dataset under prefix (default: analytics)",
    )
    head_p.add_argument("--dt", default="", help="Partition date, e.g. 2026-03-20")
    head_p.add_argument("--hour", default="", help="Partition hour, e.g. 15")
    head_p.add_argument("--files", type=int, default=3, help="Number of files to load")
    head_p.add_argument("--rows-per-file", type=int, default=5, help="Max rows per file")

    sql_p = sub.add_parser("sql", help="Run SQL against lake (DuckDB); data available as view `lake`")
    sql_p.add_argument(
        "-d",
        "--dataset",
        default="analytics",
        help="Dataset under prefix (default: analytics)",
    )
    sql_p.add_argument("--dt", default="", help="Partition date, e.g. 2026-03-20")
    sql_p.add_argument("--hour", default="", help="Partition hour, e.g. 15")
    sql_p.add_argument("query", help="SQL, e.g. SELECT * FROM lake LIMIT 10")
    sql_p.add_argument("--max-files", type=int, default=64, help="Max parquet files to download")
    sql_p.add_argument("--no-hive", action="store_true", help="Disable hive_partitioning in read_parquet")

    args = p.parse_args()
    base = normalize_key_path(args.prefix or default_blob_prefix())
    blob = _blob()

    if args.cmd == "ls":
        keys = list(
            iter_dataset_parquet_keys(
                blob,
                base,
                args.dataset,
                dt=args.dt or None,
                hour=args.hour or None,
                max_files=args.max,
            )
        )
        for k in keys:
            print(k)
        print(f"# {len(keys)} keys", file=sys.stderr)

    elif args.cmd == "head":
        keys = list(
            iter_dataset_parquet_keys(
                blob,
                base,
                args.dataset,
                dt=args.dt or None,
                hour=args.hour or None,
                max_files=args.files,
            )
        )
        if not keys:
            print("No parquet keys found.", file=sys.stderr)
            sys.exit(1)
        t = load_parquet_keys_arrow(blob, keys, max_rows_per_file=args.rows_per_file)
        print(t.schema)
        print(t.to_pandas())

    elif args.cmd == "sql":
        t = query_lake_sql(
            blob,
            base,
            args.query,
            dataset=args.dataset,
            dt=args.dt or None,
            hour=args.hour or None,
            max_files=args.max_files,
            hive_partitioning=not args.no_hive,
        )
        df = t.to_pandas()
        if os.environ.get("LAKE_QUERY_JSON"):
            print(df.to_json(orient="records", date_format="iso"))
        else:
            print(df.to_string())


if __name__ == "__main__":
    main()
