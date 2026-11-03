"""
check_setup.py
Quick check that Spark works on this machine before running the project.
Tests the four things the project relies on: starting Spark, running Python
workers, converting to Pandas, and writing/reading files.

Usage:
    python check_setup.py
"""

import shutil

from src.config import PROJECT_ROOT
from src.spark_session import get_spark


def main():
    spark = get_spark()
    print(f"Spark {spark.version} started")

    df = spark.createDataFrame(
        [("Avatar", 2923.71), ("Titanic", 2264.16)],
        ["title", "revenue_musd"],
    )
    df.show()

    # 1. Python workers (used by UDFs). Fails if Spark can't find the right Python.
    titles = df.rdd.map(lambda row: row.title.upper()).collect()
    print("Python workers OK:", titles)

    # 2. Converting to Pandas (used for the charts)
    print("toPandas OK:\n", df.toPandas())

    # 3. Writing and reading files (needs winutils.exe / HADOOP_HOME on Windows)
    test_path = PROJECT_ROOT / "data" / "_spark_write_test"
    df.write.mode("overwrite").parquet(str(test_path))
    print("File write/read OK:", spark.read.parquet(str(test_path)).count(), "rows")
    shutil.rmtree(test_path, ignore_errors=True)

    spark.stop()
    print("\nAll checks passed!")


if __name__ == "__main__":
    main()
