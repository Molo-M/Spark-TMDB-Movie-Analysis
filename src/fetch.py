"""
fetch.py
Step 1: Fetch the movie data from the TMDB API.
The raw data is saved to a JSON Lines file so we don't have to call the API
every time we rerun the analysis. Spark then reads that file into a DataFrame.
"""

import json
import time
import requests

from src.config import (TOKEN, BASE_URL, MOVIE_IDS, RAW_DATA_PATH,
                        REQUEST_TIMEOUT, MAX_RETRIES, RETRY_WAIT)
from src.schemas import RAW_MOVIE_SCHEMA


# ----------------------------------------------------------------------
# Calling the API (plain Python, runs on the Spark driver)
# ----------------------------------------------------------------------

def fetch_movie(movie_id):
    """Fetch one movie (with its credits) from the API.
    Tries up to MAX_RETRIES times if the request fails because of a network
    problem or a server error. Returns the movie as a dictionary, or None."""
    url = f"{BASE_URL}/{movie_id}?language=en-US&append_to_response=credits"

    # The header where the token is applied
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            # Raise an error if the API returned a bad status (e.g. 404 for movie id 0)
            response.raise_for_status()
            movie_data = response.json()
            print(f"Fetched: {movie_data.get('title', movie_id)}")
            return movie_data

        except requests.exceptions.HTTPError as error:
            # 429 (too many requests) and 5xx (server errors) are usually temporary,
            # so they are worth trying again. Anything else (like 404) is not.
            status = response.status_code
            if status != 429 and status < 500:
                print(f"Could not fetch movie ID {movie_id}: {error}")
                return None
            print(f"Attempt {attempt} failed for movie ID {movie_id}: {error}")

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as error:
            print(f"Attempt {attempt} failed for movie ID {movie_id}: {error}")

        # Wait a little before the next attempt
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_WAIT)

    print(f"Giving up on movie ID {movie_id} after {MAX_RETRIES} attempts")
    return None


def fetch_movies(movie_ids=MOVIE_IDS):
    """Fetch every movie in the list and return them as a list of dictionaries.
    Movies that failed to fetch are skipped."""
    if not TOKEN:
        raise ValueError("API_READ_ACCESS_TOKEN is missing from your .env file.")

    movies = []
    for movie_id in movie_ids:
        movie_data = fetch_movie(movie_id)
        if movie_data is not None:
            movies.append(movie_data)

    print(f"Fetched {len(movies)} of {len(movie_ids)} movies")
    return movies


# ----------------------------------------------------------------------
# Saving and loading the raw data
# ----------------------------------------------------------------------

def save_raw_data(movies, path=RAW_DATA_PATH):
    """Save the movies as JSON Lines: one movie (one JSON object) per line."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        for movie in movies:
            file.write(json.dumps(movie, ensure_ascii=False) + "\n")
    print(f"Saved raw data to {path}")


def load_raw_data(spark, path=RAW_DATA_PATH):
    """Read the JSON Lines file into a Spark DataFrame using the explicit schema.
    Values that don't match their type in the schema are read as null, and
    a warning is printed for every movie that had such a value."""
    df = (
        spark.read
        .schema(RAW_MOVIE_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(path))
    )

    # Spark refuses to run a query on a raw JSON file that only looks at the
    # corrupt record column, so the DataFrame is cached first. Caching also
    # means the file isn't re-read for every step that follows.
    df = df.cache()

    bad_rows = df.filter(df["_corrupt_record"].isNotNull())
    bad_count = bad_rows.count()
    if bad_count > 0:
        print(f"Warning: {bad_count} movie(s) had values that didn't match the schema "
              "(those values were read as null):")
        bad_rows.select("id", "title").show(truncate=False)

    return df.drop("_corrupt_record")


def load_or_fetch(spark, path=RAW_DATA_PATH, refresh=False):
    """Load the raw data from the JSON Lines file if it exists.
    Otherwise (or if refresh=True), fetch it from the API and save it first.
    Returns a Spark DataFrame."""
    if refresh or not path.exists():
        movies = fetch_movies()
        save_raw_data(movies, path)
    else:
        print("Loading saved raw data...")

    return load_raw_data(spark, path)
