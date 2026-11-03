"""
schemas.py
The structure of the raw movie data returned by the TMDB API
(the /movie/{id} endpoint with append_to_response=credits).

Spark reads the raw JSON with this schema instead of guessing the types itself.
Fields that aren't listed here are skipped when the data is read, and fields
that are listed but missing from a movie are read as null.
"""

from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, IntegerType,
    LongType, StringType, StructField, StructType,
)

# ----------------------------------------------------------------------
# Nested structures
# ----------------------------------------------------------------------

COLLECTION = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("poster_path", StringType()),
    StructField("backdrop_path", StringType()),
])

GENRE = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
])

PRODUCTION_COMPANY = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("logo_path", StringType()),
    StructField("origin_country", StringType()),
])

PRODUCTION_COUNTRY = StructType([
    StructField("iso_3166_1", StringType()),
    StructField("name", StringType()),
])

SPOKEN_LANGUAGE = StructType([
    StructField("english_name", StringType()),
    StructField("iso_639_1", StringType()),
    StructField("name", StringType()),
])

# Only the cast and crew fields the analysis needs. The full records
# are still kept in the raw JSON file.
CAST_MEMBER = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("character", StringType()),
    StructField("order", IntegerType()),
])

CREW_MEMBER = StructType([
    StructField("id", LongType()),
    StructField("name", StringType()),
    StructField("department", StringType()),
    StructField("job", StringType()),
])

CREDITS = StructType([
    StructField("cast", ArrayType(CAST_MEMBER)),
    StructField("crew", ArrayType(CREW_MEMBER)),
])

# ----------------------------------------------------------------------
# One movie
# ----------------------------------------------------------------------

RAW_MOVIE_SCHEMA = StructType([
    StructField("adult", BooleanType()),
    StructField("backdrop_path", StringType()),
    StructField("belongs_to_collection", COLLECTION),
    # Revenue goes above 2.1 billion, the limit of IntegerType, so use LongType
    StructField("budget", LongType()),
    StructField("genres", ArrayType(GENRE)),
    StructField("homepage", StringType()),
    StructField("id", LongType()),
    StructField("imdb_id", StringType()),
    StructField("origin_country", ArrayType(StringType())),
    StructField("original_language", StringType()),
    StructField("original_title", StringType()),
    StructField("overview", StringType()),
    StructField("popularity", DoubleType()),
    StructField("poster_path", StringType()),
    StructField("production_companies", ArrayType(PRODUCTION_COMPANY)),
    StructField("production_countries", ArrayType(PRODUCTION_COUNTRY)),
    # Kept as text here; it's converted to a date during cleaning (Step 2)
    StructField("release_date", StringType()),
    StructField("revenue", LongType()),
    StructField("runtime", IntegerType()),
    StructField("spoken_languages", ArrayType(SPOKEN_LANGUAGE)),
    StructField("status", StringType()),
    StructField("tagline", StringType()),
    StructField("title", StringType()),
    StructField("video", BooleanType()),
    StructField("vote_average", DoubleType()),
    StructField("vote_count", IntegerType()),
    StructField("credits", CREDITS),
    # Filled in by Spark with the original line if a movie doesn't match the schema
    StructField("_corrupt_record", StringType()),
])
