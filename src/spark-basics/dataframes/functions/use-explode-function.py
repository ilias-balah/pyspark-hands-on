"""
Practice using the explode, and other functions from the
pyspark.sql.functions module to manipulate DataFrames.
"""
from pyspark.sql import functions as funcs
from src.internal.proxy_spark_session import ProxySparkSession


if __name__ == '__main__':

    with ProxySparkSession("Explode Function") as spark:

        # Read the fake friends CSV file into a DataFrame.
        dataframe = spark.read.text("file:///Users/balah/Desktop/Spark/data/text/self-employment-book.txt")

        # Print the schema of the DataFrame.
        # dataframe.printSchema()

        # Register the DataFrame as a temporary view.
        dataframe.createTempView("self_employment_book")

        # Transform the DataFrame by exploding the lines into individual
        # lowercased words :
        # 1 - Lowercase the 'value' column to ensure uniformity.
        transformed_column = funcs.lower('value')
        # 2 - Split the 'value' column into words using a regex pattern that
        # matches non-word characters.
        # NOTE : Using r'\W+' (and not r'\w+') ensures we split on punctuation and whitespace,
        # effectively isolating words and ignoring punctuation.
        transformed_column = funcs.split(transformed_column, r'\W+')
        # 3 - Explode the resulting array of words into individual rows.
        transformed_column = funcs.explode(transformed_column)
        # 4 - Finally, rename the column to 'word'.
        transformed_column = transformed_column.alias("word")

        # Count up the occurrences of each word in the exploded DataFrame.
        count_results = dataframe.select(transformed_column).groupBy('word') \
                                 .count() \
                                 .withColumnRenamed('count', 'occurrence')

        # Sort the results by count in descending order.
        count_results.orderBy('occurrence', ascending=False).show(10, False)