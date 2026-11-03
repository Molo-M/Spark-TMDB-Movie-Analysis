"""
config.py
Settings used across the whole project: the API token, the movie IDs,
Spark settings, file paths, and the column lists used during cleaning.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ----------------------------------------------------------------------
# API settings
# ----------------------------------------------------------------------

# Load the variables stored in the .env file
load_dotenv()
TOKEN = os.getenv("API_READ_ACCESS_TOKEN")

BASE_URL = "https://api.themoviedb.org/3/movie"

# How long to wait for the API to respond (in seconds) before giving up
REQUEST_TIMEOUT = 10

# How many times to try fetching a movie, and how long to wait between tries (in seconds)
MAX_RETRIES = 3
RETRY_WAIT = 2

# Collection of movie ids whose data we will use for the project
MOVIE_IDS = [
    0, 299534, 19995, 140607, 299536, 597, 135397,
    420818, 24428, 168259, 99861, 284054, 12445,
    181808, 330457, 351286, 109445, 321612, 260513
]

# ----------------------------------------------------------------------
# Spark settings
# ----------------------------------------------------------------------

SPARK_APP_NAME = "TMDB-Movie-Analysis"

# Run Spark on this machine, using all CPU cores
SPARK_MASTER = "local[*]"

# After every groupBy, join or sort, Spark splits the data into 200 partitions
# by default. That suits large clusters, but for 18 rows it just creates
# 200 tiny tasks, most of them empty, which makes every step slower.
SHUFFLE_PARTITIONS = 4

# ----------------------------------------------------------------------
# File paths
# ----------------------------------------------------------------------

# The project folder (one level above this src/ folder)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# JSON Lines: one movie per line, the format Spark reads natively
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw" / "movies_raw.jsonl"

# Spark writes a folder of part files, not a single file
CLEAN_DATA_PATH = PROJECT_ROOT / "data" / "processed" / "movies_clean.parquet"

FIGURES_DIR = PROJECT_ROOT / "reports" / "figures"

# ----------------------------------------------------------------------
# Column lists used in cleaning
# ----------------------------------------------------------------------

# Columns we don't need for the analysis
COLUMNS_TO_DROP = [
    "adult", "imdb_id", "original_title", "video", "homepage"
]

# Columns that contain a list of dictionaries with a "name" key
NAME_LIST_COLUMNS = [
    "genres", "spoken_languages", "production_countries", "production_companies"
]

# Final order of the columns (from the instructions)
FINAL_COLUMN_ORDER = [
    "id", "title", "tagline", "release_date", "genres", "belongs_to_collection",
    "original_language", "budget_musd", "revenue_musd", "production_companies",
    "production_countries", "vote_count", "vote_average", "popularity", "runtime",
    "overview", "spoken_languages", "poster_path", "cast", "cast_size", "director", "crew_size"
]
