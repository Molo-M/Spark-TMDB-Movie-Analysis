"""
spark_session.py
Creates the SparkSession used by main.py and the notebook,
so both run Spark with the same settings.
"""

import os
import sys
import pyspark

from pyspark.sql import SparkSession

from src.config import SPARK_APP_NAME, SPARK_MASTER, SHUFFLE_PARTITIONS


def get_spark():
    """Create the SparkSession, or return the one that is already running."""
    # Spark runs Python code (e.g. UDFs) in separate worker processes. On Windows
    # those workers can start a different Python than the one running this code,
    # such as a system install instead of the virtual environment, which causes
    # "Python worker failed to connect back" errors. This points them at the
    # Python that is running right now.
    os.environ["PYSPARK_PYTHON"] = sys.executable

    # Use the Spark that pip installed in this environment, so the version
    # always matches the pyspark package
    os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName(SPARK_APP_NAME)
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Use Apache Arrow to make toPandas() (used for the charts) faster
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    # Only show warnings and errors, not Spark's routine INFO messages
    spark.sparkContext.setLogLevel("WARN")
    return spark
