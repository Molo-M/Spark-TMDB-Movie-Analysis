"""
main.py
Runs the whole pipeline from start to finish:
fetch -> clean -> analyse -> visualise.
(Steps 1 to 3 are in place so far; the others are added as they're built.)

Usage:
    python main.py            # uses the saved raw data if it exists
    python main.py --refresh  # fetches fresh data from the API
"""

import sys

import pandas as pd

from src import fetch, clean, analysis
from src.config import NUMERIC_COLUMNS
from src.spark_session import get_spark


def print_section(title):
    """Print a title so the output is easy to read."""
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def show_results(df, columns):
    """Show the chosen columns, or a message if no movies matched."""
    if df.isEmpty():
        print("No matching movies found.")
    else:
        df.select(columns).show(truncate=False)


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

    # Step 3: KPIs and analysis
    df = analysis.add_profit_and_roi(df)

    print_section("Step 3: Best / Worst Performing Movies")
    budget_10m = analysis.filter_min_budget(df)
    votes_10 = analysis.filter_min_votes(df)

    rankings = [
        ("Highest Revenue", df, "revenue_musd", False),
        ("Highest Budget", df, "budget_musd", False),
        ("Highest Profit", df, "profit_musd", False),
        ("Lowest Profit", df, "profit_musd", True),
        ("Highest ROI (budget >= 10M)", budget_10m, "roi", False),
        ("Lowest ROI (budget >= 10M)", budget_10m, "roi", True),
        ("Most Voted", df, "vote_count", False),
        ("Highest Rated (>= 10 votes)", votes_10, "vote_average", False),
        ("Lowest Rated (>= 10 votes)", votes_10, "vote_average", True),
        ("Most Popular", df, "popularity", False),
    ]
    for title, data, column, ascending in rankings:
        print(f"\n{title}:")
        analysis.rank_movies(data, column, ascending=ascending, top_n=5).show(truncate=False)

    print_section("Search Queries")
    print("\nSci-Fi Action movies starring Bruce Willis:")
    show_results(analysis.search_bruce_willis(df), ["title", "vote_average"])
    print("\nUma Thurman movies directed by Quentin Tarantino:")
    show_results(analysis.search_uma_thurman_tarantino(df), ["title", "runtime"])

    print_section("Franchise vs. Standalone")
    analysis.franchise_vs_standalone(df).show(truncate=False)

    print_section("Most Successful Franchises")
    analysis.franchise_summary(df).show(truncate=False)

    print_section("Most Successful Directors")
    analysis.director_summary(df).show(truncate=False)

    spark.stop()
    print("\nDone!")


if __name__ == "__main__":
    main()