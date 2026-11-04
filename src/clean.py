"""
clean.py
Step 2: Clean and preprocess the raw movie data.
Each function does one cleaning step: it takes a Spark DataFrame and returns
a new one. clean_data() runs all of them in order.

Every step uses Spark's built-in column functions instead of Python functions,
so the work runs inside Spark (in parallel) rather than one row at a time in Python.
"""

from pyspark.sql import functions as F

from src.config import (COLUMNS_TO_DROP, NAME_LIST_COLUMNS,
                        FINAL_COLUMN_ORDER, CLEAN_DATA_PATH)


# ----------------------------------------------------------------------
# Helper (builds a column expression, used by several steps)
# ----------------------------------------------------------------------

def join_names(names):
    """Join an array of names with "|".
    Returns null for an empty or missing array, so it counts as missing data."""
    return F.when(F.size(names) > 0, F.concat_ws("|", names))


# ----------------------------------------------------------------------
# Cleaning steps (each takes a DataFrame and returns a DataFrame)
# ----------------------------------------------------------------------

def drop_irrelevant_columns(df):
    """Drop the columns we don't need. Names that don't exist are ignored."""
    return df.drop(*COLUMNS_TO_DROP)


def extract_collection(df):
    """Replace the belongs_to_collection struct with just the collection name.
    Movies that aren't in a collection stay null."""
    return df.withColumn("belongs_to_collection", F.col("belongs_to_collection.name"))


def extract_credits(df):
    """Split the credits column into cast, cast_size, director and crew_size."""
    cast = F.col("credits.cast")
    crew = F.col("credits.crew")

    # Keep only the crew members whose job is "Director"
    directors = F.filter(crew, lambda member: member["job"] == "Director")

    return (
        df
        # Count the cast and crew before turning them into strings
        .withColumn("cast_size", F.size(cast))
        .withColumn("crew_size", F.size(crew))
        # Keep only the names (cast.name gives an array with every cast member's name)
        .withColumn("cast", join_names(cast["name"]))
        .withColumn("director", join_names(directors["name"]))
        # Drop the uncleaned column now that the new clean columns have been added
        .drop("credits")
    )


def extract_names(df):
    """Clean genres, spoken languages, production countries and companies."""
    for col in NAME_LIST_COLUMNS:
        df = df.withColumn(col, join_names(F.col(col)["name"]))
    return df


def convert_types(df):
    """Convert columns to the right data types (invalid values become null).
    try_cast and try_to_date return null for bad values; plain cast and
    to_date would stop the whole job with an error instead."""
    return (
        df
        .withColumn("budget", F.col("budget").try_cast("long"))
        .withColumn("id", F.col("id").try_cast("long"))
        .withColumn("popularity", F.col("popularity").try_cast("double"))
        .withColumn("release_date", F.try_to_date("release_date"))
    )


def replace_unrealistic_values(df):
    """Replace values that don't make sense with null."""
    # A budget, revenue or runtime of 0 means the value is unknown
    for col in ["budget", "revenue", "runtime"]:
        df = df.withColumn(col, F.when(F.col(col) != 0, F.col(col)))

    # A movie with no votes can't have a meaningful rating
    df = df.withColumn(
        "vote_average",
        F.when(F.col("vote_count") == 0, None).otherwise(F.col("vote_average")),
    )

    # Replace placeholder text with null
    for col in ["overview", "tagline"]:
        df = df.withColumn(
            col,
            F.when(F.col(col).isin("No Data", ""), None).otherwise(F.col(col)),
        )

    return df


def convert_to_millions(df):
    """Convert budget and revenue to million USD and rename the columns."""
    return (
        df
        .withColumn("budget_musd", F.round(F.col("budget") / 1_000_000, 2))
        .withColumn("revenue_musd", F.round(F.col("revenue") / 1_000_000, 2))
        .drop("budget", "revenue")
    )


def remove_bad_rows(df):
    """Remove duplicates, rows without id/title, and rows with too much missing data."""
    df = df.dropDuplicates(["id"])
    df = df.dropna(subset=["id", "title"])
    # Keep only rows where at least 10 columns have non-null values
    df = df.dropna(thresh=10)
    return df


def filter_released(df):
    """Keep only movies that have been released, then drop the status column."""
    return df.filter(F.col("status") == "Released").drop("status")


def reorder_columns(df):
    """Put the columns in the final order.
    select() raises an error if a column name doesn't exist, instead of
    silently creating an empty column. There is no index to reset:
    Spark DataFrames don't have a row index like Pandas does."""
    return df.select(FINAL_COLUMN_ORDER)


# ----------------------------------------------------------------------
# Run every step
# ----------------------------------------------------------------------

def clean_data(raw_df):
    """Run all the cleaning steps in order and return the clean DataFrame.
    Nothing is computed yet: Spark only builds a plan of the steps, and runs
    it when an action (like count() or show()) needs the result."""
    return (
        raw_df
        .transform(drop_irrelevant_columns)
        .transform(extract_collection)
        .transform(extract_credits)
        .transform(extract_names)
        .transform(convert_types)
        .transform(replace_unrealistic_values)
        .transform(convert_to_millions)
        .transform(remove_bad_rows)
        .transform(filter_released)
        .transform(reorder_columns)
    )


# ----------------------------------------------------------------------
# Inspection and summary helpers
# ----------------------------------------------------------------------

def value_counts(df, column, split_pipes=False):
    """Count how often each value appears in a column, most common first
    (Spark's version of Pandas' value_counts()).
    With split_pipes=True, "Action|Adventure" is counted as Action and Adventure."""
    values = F.col(column)
    if split_pipes:
        values = F.explode(F.split(values, r"\|"))
    return (
        df.select(values.alias(column))
        .groupBy(column)
        .count()
        .orderBy(F.desc("count"), column)
    )


def count_missing(df):
    """Count the missing values in every column, in a single pass over the data."""
    counts = df.select([
        F.sum(F.col(col).isNull().cast("int")).alias(col) for col in df.columns
    ])
    return counts.first().asDict()


def missing_values(df):
    """Missing value count per column (only columns with at least one)."""
    rows = [(col, count) for col, count in count_missing(df).items() if count > 0]
    return df.sparkSession.createDataFrame(rows, "column string, missing long")


def compare_before_after(raw_df, clean_df):
    """Build a small table comparing the raw data and the clean data."""
    def summarise(df):
        return {
            "rows": df.count(),
            "columns": len(df.columns),
            "missing": sum(count_missing(df).values()),
            "duplicate_ids": df.count() - df.select("id").distinct().count(),
        }

    before, after = summarise(raw_df), summarise(clean_df)
    rows = [
        ("Rows", before["rows"], after["rows"]),
        ("Columns", before["columns"], after["columns"]),
        ("Missing values (total)", before["missing"], after["missing"]),
        ("Duplicate ids", before["duplicate_ids"], after["duplicate_ids"]),
    ]
    return clean_df.sparkSession.createDataFrame(rows, "Metric string, Before long, After long")


def summary_statistics(df, columns):
    """Count, mean, standard deviation, min and max of numeric columns,
    rounded to 2 decimals. Spark's describe() returns its results as text,
    so the small result table is converted to Pandas to round it."""
    stats = df.select(columns).describe().toPandas().set_index("summary")
    return stats.astype(float).round(2)


def save_clean_data(df, path=CLEAN_DATA_PATH):
    """Save the clean DataFrame as Parquet, which keeps the column types.
    coalesce(1) writes a single file, since the data is small."""
    df.coalesce(1).write.mode("overwrite").parquet(str(path))
    print(f"Saved clean data...")
