"""
analysis.py
Step 3: KPIs and analysis.
Ranking movies, searching for movies, and comparing franchises and directors.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window


def add_profit_and_roi(df):
    """Add a profit column (revenue - budget) and an ROI column (revenue / budget).
    try_divide returns null instead of failing if a budget is ever 0."""
    return (
        df
        .withColumn("profit_musd", F.round(F.col("revenue_musd") - F.col("budget_musd"), 2))
        .withColumn("roi", F.round(F.try_divide(F.col("revenue_musd"), F.col("budget_musd")), 2))
    )


# ----------------------------------------------------------------------
# User-Defined Function (UDF)
# ----------------------------------------------------------------------

@F.udf(returnType=StringType())
def format_musd(value):
    """UDF: turn an amount in million USD into a readable label,
    e.g. 2942.19 -> "$2.94B" and 356.0 -> "$356.0M".

    A Python UDF runs row by row in a separate Python process, which is much
    slower than Spark's built-in functions, so the rest of this module uses
    built-in functions only. It is fine here because it only formats the few
    rows that a ranking returns."""
    if value is None:
        return None
    if abs(value) >= 1000:
        return f"${value / 1000:.2f}B"
    return f"${value:.1f}M"


# Columns measured in million USD, which get a readable label in rankings
MONEY_COLUMNS = {"budget_musd", "revenue_musd", "profit_musd"}


# ----------------------------------------------------------------------
# Best / worst performing movies
# ----------------------------------------------------------------------

def rank_movies(df, column, ascending=False, top_n=None):
    """Rank movies by a column and return title, value and rank.
    ascending=False gives the highest first, ascending=True gives the lowest first.
    Movies with a missing value are left out, and tied movies share a rank
    (so a top 5 can contain more than 5 movies if there is a tie for 5th place)."""
    order = F.col(column).asc() if ascending else F.col(column).desc()
    # A window with no partitionBy ranks all movies together. Spark warns that
    # this moves all the data into one partition, which is fine for 18 movies.
    ranking = Window.orderBy(order)

    ranked = (
        df.filter(F.col(column).isNotNull())
        .select("title", column)
        .withColumn("rank", F.rank().over(ranking))
    )
    if column in MONEY_COLUMNS:
        ranked = ranked.withColumn("label", format_musd(F.col(column)))
    if top_n is not None:
        ranked = ranked.filter(F.col("rank") <= top_n)
    # Sorting by title too keeps tied movies in the same order on every run
    return ranked.orderBy("rank", "title")


def filter_min_budget(df, min_budget=10):
    """Keep only movies with a budget of at least min_budget million USD (used for ROI)."""
    return df.filter(F.col("budget_musd") >= min_budget)


def filter_min_votes(df, min_votes=10):
    """Keep only movies with at least min_votes votes (used for ratings)."""
    return df.filter(F.col("vote_count") >= min_votes)


# ----------------------------------------------------------------------
# Search queries
# ----------------------------------------------------------------------

def has_value(column, value):
    """True if a "|"-separated column contains value as one of its items.
    Matching whole items avoids false matches that a substring search allows
    (e.g. a search for "Action" would also match "Action Comedy")."""
    return F.array_contains(F.split(F.col(column), r"\|"), value)


def search_bruce_willis(df):
    """Search 1: Science Fiction Action movies starring Bruce Willis, best rated first."""
    return (
        df.filter(
            has_value("cast", "Bruce Willis")
            & has_value("genres", "Science Fiction")
            & has_value("genres", "Action")
        )
        .orderBy(F.desc("vote_average"), "title")
    )


def search_uma_thurman_tarantino(df):
    """Search 2: movies starring Uma Thurman directed by Quentin Tarantino, shortest first."""
    return (
        df.filter(
            has_value("cast", "Uma Thurman")
            & has_value("director", "Quentin Tarantino")
        )
        .orderBy(F.asc("runtime"), "title")
    )


# ----------------------------------------------------------------------
# Franchise vs. standalone
# ----------------------------------------------------------------------

def franchise_vs_standalone(df):
    """Compare franchise and standalone movies on every metric, in one groupBy."""
    movie_type = (
        F.when(F.col("belongs_to_collection").isNotNull(), "Franchise")
        .otherwise("Standalone")
    )
    return (
        df.groupBy(movie_type.alias("movie_type"))
        .agg(
            F.count("id").alias("movie_count"),
            F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
            # median() averages the two middle values, like Pandas does.
            # percentile_approx() would return one of them instead, which
            # gives a different answer for small groups (e.g. 2 standalone movies).
            F.round(F.median("roi"), 2).alias("median_roi"),
            F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
            F.round(F.mean("popularity"), 2).alias("mean_popularity"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy("movie_type")
    )


# ----------------------------------------------------------------------
# Most successful franchises and directors
# ----------------------------------------------------------------------

def franchise_summary(df):
    """Summarise each franchise: number of movies, budget, revenue and rating."""
    return (
        df.filter(F.col("belongs_to_collection").isNotNull())
        .groupBy("belongs_to_collection")
        .agg(
            F.count("id").alias("movie_count"),
            F.round(F.sum("budget_musd"), 2).alias("total_budget_musd"),
            F.round(F.mean("budget_musd"), 2).alias("mean_budget_musd"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.mean("revenue_musd"), 2).alias("mean_revenue_musd"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue_musd"), "belongs_to_collection")
    )


def director_summary(df):
    """Summarise each director: number of movies, total revenue and mean rating.
    Movies with several directors count once for each of them."""
    return (
        df.withColumn("director", F.explode(F.split(F.col("director"), r"\|")))
        .groupBy("director")
        .agg(
            F.count("id").alias("movie_count"),
            F.round(F.sum("revenue_musd"), 2).alias("total_revenue_musd"),
            F.round(F.mean("vote_average"), 2).alias("mean_rating"),
        )
        .orderBy(F.desc("total_revenue_musd"), "director")
    )
