"""
visualize.py
EDA charts and Step 4 charts.
One function per chart. Each function can show the chart, save it, or both.

Spark does the heavy work (grouping, counting, averaging, correlations), and
only the small result is converted to Pandas with toPandas() for plotting.
Matplotlib can only draw data that is in memory on one machine, so with a
large dataset the aggregation has to happen in Spark first.
"""

import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import functions as F

from src.analysis import franchise_vs_standalone
from src.clean import value_counts
from src.config import NUMERIC_COLUMNS

BAR_STYLE = {"color": "skyblue", "edgecolor": "black"}


def finish_plot(save_path=None, show=True):
    """Save the current chart (if a path is given) and/or show it."""
    if save_path is not None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(save_path, bbox_inches="tight")
        print(f"Saved figure to {save_path}")
    if show:
        plt.show()
    plt.close()


def scatter_with_trend(x, y, line_color):
    """Scatter plot with a straight trend line fitted through the points."""
    plt.scatter(x, y, alpha=0.8)
    slope, intercept = np.polyfit(x, y, 1)
    x_line = np.linspace(x.min(), x.max(), 100)
    plt.plot(x_line, slope * x_line + intercept, color=line_color, linestyle="--", linewidth=2)


# ----------------------------------------------------------------------
# Exploratory Data Analysis (EDA) charts
# ----------------------------------------------------------------------

def plot_distributions(df, save_path=None, show=True):
    """Histograms of budget, revenue, rating and runtime side by side.
    A histogram needs every value, so only the 4 columns are collected.
    With a large dataset you would take a sample() first."""
    columns = ["budget_musd", "revenue_musd", "vote_average", "runtime"]
    titles = ["Budget (million USD)", "Revenue (million USD)", "Rating", "Runtime (minutes)"]
    data = df.select(columns).toPandas()

    fig, axes = plt.subplots(1, 4, figsize=(16, 4))
    for ax, column, title in zip(axes, columns, titles):
        ax.hist(data[column].dropna(), bins=8, **BAR_STYLE)
        ax.set_title(title)
        ax.set_ylabel("Number of movies")
    plt.suptitle("Distributions of Key Numeric Columns")
    plt.tight_layout()
    finish_plot(save_path, show)


def plot_genre_counts(df, save_path=None, show=True):
    """Horizontal bar chart of how many movies belong to each genre."""
    # Counted in Spark: one row per genre after splitting "Action|Adventure"
    counts = value_counts(df, "genres", split_pipes=True).toPandas()
    counts = counts.sort_values("count")

    plt.figure()
    plt.barh(counts["genres"], counts["count"], **BAR_STYLE)
    plt.title("Number of Movies per Genre")
    plt.xlabel("Number of movies")
    plt.ylabel("Genre")
    finish_plot(save_path, show)


def correlation_matrix(df, columns=NUMERIC_COLUMNS):
    """Correlations between numeric columns, computed by Spark in one pass.
    Spark's Correlation works on a single "vector" column, so VectorAssembler
    first packs the numeric columns into one. Rows with a missing value are
    skipped (Pandas' corr() instead skips them pair by pair)."""
    assembler = VectorAssembler(inputCols=columns, outputCol="features", handleInvalid="skip")
    vectors = assembler.transform(df.select(columns))
    matrix = Correlation.corr(vectors, "features").head()[0].toArray()
    return matrix


def plot_correlations(df, save_path=None, show=True):
    """Heatmap of the correlations between the numeric columns."""
    matrix = correlation_matrix(df)
    labels = NUMERIC_COLUMNS

    fig, ax = plt.subplots(figsize=(7, 5))
    image = ax.imshow(matrix, cmap="coolwarm", vmin=-1, vmax=1)
    fig.colorbar(image)
    ax.set_xticks(range(len(labels)), labels, rotation=90)
    ax.set_yticks(range(len(labels)), labels)
    # Write each correlation value in its square
    for i in range(len(labels)):
        for j in range(len(labels)):
            color = "white" if abs(matrix[i, j]) > 0.6 else "black"
            ax.text(j, i, f"{matrix[i, j]:.2f}", ha="center", va="center", color=color)
    plt.title("Correlation Between Numeric Columns")
    finish_plot(save_path, show)


# ----------------------------------------------------------------------
# Step 4 charts
# ----------------------------------------------------------------------

def plot_revenue_vs_budget(df, save_path=None, show=True):
    """Scatter plot of revenue against budget with a trend line."""
    data = df.select("budget_musd", "revenue_musd").dropna().toPandas()

    plt.figure()
    scatter_with_trend(data["budget_musd"], data["revenue_musd"], line_color="red")
    plt.title("Revenue vs. Budget Trend")
    plt.xlabel("Budget (million USD)")
    plt.ylabel("Revenue (million USD)")
    finish_plot(save_path, show)


def plot_roi_by_genre(df, save_path=None, show=True):
    """Horizontal bar chart of the average ROI for each genre.
    Each bar is labelled with its number of movies, because an average
    over 1 or 2 movies is much less reliable than one over 10."""
    genre_roi = (
        df.withColumn("genre", F.explode(F.split(F.col("genres"), r"\|")))
        .groupBy("genre")
        .agg(F.mean("roi").alias("mean_roi"), F.count("id").alias("movie_count"))
        .orderBy("mean_roi")
        .toPandas()
    )

    plt.figure()
    bars = plt.barh(genre_roi["genre"], genre_roi["mean_roi"], **BAR_STYLE)
    plt.bar_label(bars, labels=[f"n={n}" for n in genre_roi["movie_count"]], padding=3, fontsize=8)
    plt.xlim(0, genre_roi["mean_roi"].max() * 1.15)
    plt.title("ROI Distribution by Genre")
    plt.xlabel("Average ROI")
    plt.ylabel("Genre")
    finish_plot(save_path, show)


def plot_popularity_vs_rating(df, save_path=None, show=True):
    """Scatter plot of rating against popularity with a trend line."""
    data = df.select("popularity", "vote_average").dropna().toPandas()

    plt.figure()
    scatter_with_trend(data["popularity"], data["vote_average"], line_color="purple")
    plt.title("Popularity vs. Rating")
    plt.xlabel("Popularity")
    plt.ylabel("Vote Average (Rating)")
    finish_plot(save_path, show)


def plot_yearly_revenue(df, save_path=None, show=True):
    """Line chart of total revenue per release year.
    Each point is labelled with its number of movies, since a year with
    more movies in the dataset will naturally have a higher total."""
    yearly = (
        df.groupBy(F.year("release_date").alias("release_year"))
        .agg(F.sum("revenue_musd").alias("total_revenue_musd"), F.count("id").alias("movie_count"))
        .orderBy("release_year")
        .toPandas()
    )

    plt.figure()
    plt.plot(yearly["release_year"], yearly["total_revenue_musd"], marker="o")
    for year, revenue, count in zip(yearly["release_year"], yearly["total_revenue_musd"],
                                    yearly["movie_count"]):
        plt.annotate(f"n={count}", (year, revenue),
                     textcoords="offset points", xytext=(0, 7), ha="center", fontsize=8)
    plt.margins(y=0.1)  # leave room for the labels above the highest points
    plt.title("Yearly Trends in Box Office Performance")
    plt.xlabel("Release Year")
    plt.ylabel("Revenue (million USD)")
    finish_plot(save_path, show)


def plot_franchise_vs_standalone(df, save_path=None, show=True):
    """Side-by-side bars comparing franchise and standalone movies on four metrics.
    Reuses the table from analysis.franchise_vs_standalone(), so the chart
    always shows the same numbers as the analysis."""
    summary = franchise_vs_standalone(df).toPandas()
    metrics = [
        ("mean_revenue_musd", "Mean Revenue (M USD)"),
        ("median_roi", "Median ROI"),
        ("mean_budget_musd", "Mean Budget (M USD)"),
        ("mean_rating", "Mean Rating"),
    ]
    labels = [f"{t}\n(n={n})" for t, n in zip(summary["movie_type"], summary["movie_count"])]

    fig, axes = plt.subplots(1, 4, figsize=(14, 4))
    for ax, (column, title) in zip(axes, metrics):
        bars = ax.bar(labels, summary[column], color=["tab:blue", "tab:orange"], edgecolor="black")
        ax.bar_label(bars, fmt="%.2f", padding=2)
        ax.set_title(title)
        ax.margins(y=0.15)
    plt.suptitle("Franchise vs. Standalone Success")
    plt.tight_layout()
    finish_plot(save_path, show)
