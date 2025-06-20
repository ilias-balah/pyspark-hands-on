"""
Usage Instructions
------------------
To execute this script, open PowerShell as Administrator and run one of the following:

    spark-submit src\\spark-setup\\users-rating-histogram.py
    python src\\spark-setup\\users-rating-histogram.py

NOTE:
- If using `python` directly, ensure that your virtual environment is activated.
- PySpark must be installed in the environment and its version should match the installed Spark version (e.g., 3.4.4).
"""
from src.internal.spark.proxies import SparkContextProxy


if __name__ == '__main__':

    spark_configs = dict({
        # Configure for a local setup with 2 cores
        "spark.master": "local[2]"
    })

    # Initialize and manage the Spark context using a context manager
    with SparkContextProxy('MoviesRatingsHistogram', **spark_configs) as spark:

        # Load and parse the dataset file
        lines = spark.context.textFile("file:///Users/balah/Desktop/Spark/data/ml-100k/u.data")

        # Extract the rating field (3rd column) from each line
        ratings = lines.map(lambda line: line.split()[2])

        # Count the frequency of each rating value
        rating_counts = ratings.countByValue()

        # Output the sorted result to the console
        for rating, count in sorted(rating_counts.items()):
            print(f"{rating}: {count}")
