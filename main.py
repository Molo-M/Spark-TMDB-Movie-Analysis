"""
main.py
Runs the whole pipeline from start to finish:
fetch -> clean -> analyse -> visualise.
(Only Step 1 is in place so far; the other steps are added as they're built.)

Usage:
    python main.py            # uses the saved raw data if it exists
    python main.py --refresh  # fetches fresh data from the API
"""

import sys

from src import fetch
from src.spark_session import get_spark


def print_section(title):
    """Print a title so the output is easy to read."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def main():
    refresh = "--refresh" in sys.argv
    spark = get_spark()

    # Step 1: Fetch the data
    print_section("Step 1: Fetching data")
    raw_df = fetch.load_or_fetch(spark, refresh=refresh)
    print(f"\nRaw data: {raw_df.count()} rows, {len(raw_df.columns)} columns")
    raw_df.select("id", "title", "release_date", "budget", "revenue").show(truncate=False)

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()
