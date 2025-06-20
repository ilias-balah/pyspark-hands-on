"""
Practice using the explode, and other functions from the
pyspark.sql.functions module to manipulate DataFrames.
"""
from src.internal.spark.proxies import SparkSessionProxy
from src.utils.spark_functions import spark_funcs


if __name__ == '__main__':

    with SparkSessionProxy("Explode Function") as spark:

        # Read the fake friends CSV file into a DataFrame.
        dataframe = spark.session.read.text("file:///Users/balah/Desktop/Spark/data/text/self-employment-book.txt")

        # Print the schema of the DataFrame.
        # dataframe.printSchema()

        # Register the DataFrame as a temporary view.
        dataframe.createTempView("self_employment_book")

        # Transform the DataFrame by exploding the lines into individual
        # lowercased words :
        # 1 - Lowercase the 'value' column to ensure uniformity.
        transformed_column = spark_funcs.lower('value')
        # 2 - Split the 'value' column into words using a regex pattern that
        # matches non-word characters.
        # NOTE : Using r'\W+' (and not r'\w+') ensures we split on punctuation and whitespace,
        # effectively isolating words and ignoring punctuation.
        transformed_column = spark_funcs.split(transformed_column, r'\W+')
        # 3 - Explode the resulting array of words into individual rows.
        transformed_column = spark_funcs.explode(transformed_column)
        # 4 - Finally, rename the column to 'word'.
        transformed_column = transformed_column.alias("word")

        # Count up the occurrences of each word in the exploded DataFrame.
        count_results = dataframe.select(transformed_column).groupBy('word') \
                                 .count() \
                                 .withColumnRenamed('count', 'occurrence')

        # Sort the results by count in descending order.
        count_results.orderBy('occurrence', ascending=False).show(10, False)