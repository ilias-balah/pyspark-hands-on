"""
To run this script, we need to open PowerShell as Administrator and
execute one of the following commands :

  `spark-submit src\spark-setup\users-rating-histogram.py`
  `python src\spark-setup\users-rating-histogram.py`

NOTE: If using Python directly, make sure to activate the environment
where PySpark is installed. The PySpark version must match the Spark
version used (3.4.4).
"""

from pyspark import SparkConf, SparkContext


if __name__ == '__main__':

    # Use the spark context in a context manager to ensure the closure.
    with SparkContext('local', 'MoviesRatingsHistogram') as spark_context:

        # Set the logging level to error.
        spark_context.setLogLevel('ERROR')

        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/ml-100k/u.data")
        print('#'*100)
        
        ratings = lines.map(lambda s: s.split()[2])
        results = ratings.countByValue()

        for rate, n_rate in sorted(results.items()):
            print(f"{rate}: {n_rate}")