"""
main.py
Runs the whole pipeline from start to finish:
fetch -> clean -> analyse -> visualise.
(Steps 1 and 2 are in place so far; the others are added as they're built.)

Usage:
    python main.py            # uses the saved raw data if it exists
    python main.py --refresh  # fetches fresh data from the API
"""

import sys

import pandas as pd

from src import fetch, clean
from src.config import NUMERIC_COLUMNS
from src.spark_session import get_spark


def print_section(title):
    """Print a title so the output is easy to read."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main():
    # Show all columns when printing Pandas tables in the terminal
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)

    refresh = "--refresh" in sys.argv
    spark = get_spark()

    # Step 1: Fetch the data
    print_section("Step 1: Fetching data")
    raw_df = fetch.load_or_fetch(spark, refresh=refresh)
    print(f"Raw data: {raw_df.count()} rows, {len(raw_df.columns)} columns")

    # Step 2: Clean the data
    print_section("Step 2: Cleaning data")
    # cache() keeps the clean data in memory, because every later step reuses it.
    # Without it, Spark would redo all the cleaning steps for each one.
    df = clean.clean_data(raw_df).cache()
    clean.save_clean_data(df)

    print("\nBefore vs. after cleaning:")
    clean.compare_before_after(raw_df, df).show(truncate=False)
    print("Missing values per column after cleaning:")
    clean.missing_values(df).show(truncate=False)
    print("Summary statistics of the clean data:")
    print(clean.summary_statistics(df, NUMERIC_COLUMNS))

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()