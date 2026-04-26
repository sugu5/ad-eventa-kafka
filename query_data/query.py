"""
Pandas + SQLite Script to Read Iceberg Parquet Tables
"""
import argparse
import os
import re
import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read local Iceberg Parquet data, run SQL, and save query output."
    )
    parser.add_argument(
        "--table-name",
        action="append",
        dest="table_names",
        help=(
            "Iceberg table name to load into SQLite. Can be used multiple times. "
            "Defaults to all tables in output/iceberg_warehouse."
        ),
    )
    parser.add_argument(
        "--sql-file",
        type=Path,
        default=PROJECT_ROOT / "query_data" / "queries.sql",
        help="SQL file to execute against the in-memory SQLite table.",
    )
    parser.add_argument(
        "--output-file",
        type=Path,
        help="CSV output file. Defaults to <sql-file-folder>/<first-table-name>_output.csv.",
    )
    return parser.parse_args()


def resolve_project_path(path):
    return path if path.is_absolute() else PROJECT_ROOT / path


def load_parquet_table(table_name):
    data_dir = PROJECT_ROOT / "output" / "iceberg_warehouse" / table_name / "data"
    parquet_files = sorted(
        path
        for path in data_dir.rglob("*.parquet")
        if path.is_file() and not path.name.startswith(".")
    )

    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {data_dir}")

    df = pd.read_parquet(parquet_files)
    print(
        f"Loaded {len(df):,} rows from {len(parquet_files)} parquet files "
        f"with columns: {list(df.columns)}"
    )
    return df


def discover_table_names():
    warehouse_dir = PROJECT_ROOT / "output" / "iceberg_warehouse"
    if not warehouse_dir.exists():
        raise FileNotFoundError(f"Iceberg warehouse not found: {warehouse_dir}")

    table_names = sorted(
        path.name
        for path in warehouse_dir.iterdir()
        if path.is_dir() and (path / "data").is_dir()
    )
    if not table_names:
        raise FileNotFoundError(f"No Iceberg tables found under {warehouse_dir}")

    return table_names


def get_output_table_name(sql, table_names):
    matches = re.findall(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", sql, re.IGNORECASE)
    for match in matches:
        if match in table_names:
            return match
    return table_names[0]


def main():
    args = parse_args()
    os.chdir(PROJECT_ROOT)

    try:
        table_names = args.table_names or discover_table_names()
    except Exception as e:
        print(f"Error finding tables: {e}")
        exit(1)

    sql_file = resolve_project_path(args.sql_file)
    print("=" * 90)
    print("Reading Iceberg Parquet Files with Pandas + SQLite")
    print("=" * 90)

    print("\nLoading parquet files...")
    loaded_tables = {}
    for table_name in table_names:
        try:
            loaded_tables[table_name] = load_parquet_table(table_name)
        except Exception as e:
            print(f"Error loading table '{table_name}': {e}")
            exit(1)

    if not sql_file.exists():
        print(f"SQL file not found: {sql_file}")
        exit(1)

    sql = sql_file.read_text(encoding="utf-8").strip()
    if not sql:
        print(f"SQL file is empty: {sql_file}")
        exit(1)

    output_table_name = get_output_table_name(sql, table_names)
    output_file = (
        resolve_project_path(args.output_file)
        if args.output_file
        else sql_file.parent / f"{output_table_name}_output.csv"
    )

    print(f"\nRunning SQL from: {sql_file}")
    print("-" * 90)

    conn = sqlite3.connect(":memory:")
    try:
        for table_name, table_df in loaded_tables.items():
            table_df.to_sql(table_name, conn, index=False, if_exists="replace")
            print(f"Registered SQLite table: {table_name}")

        result_df = pd.read_sql_query(sql, conn)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_file, index=False)

        print(result_df.to_string(index=False))
        print(f"\nSaved {len(result_df)} rows to: {output_file}")
    except Exception as e:
        print(f"Error running SQL: {e}")
        exit(1)
    finally:
        conn.close()

    print("\n" + "=" * 90)


if __name__ == "__main__":
    main()
