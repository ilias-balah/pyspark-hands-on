"""
Practice using DataFrames instead of RDDs, and some basic DataFrame operations.
"""
from pyspark.sql import functions as funcs
from src.internal.proxy_spark_session import ProxySparkSession


if __name__ == '__main__':

    with ProxySparkSession("Test Dataframes instead of RDDs") as spark:

        # Read the fake friends CSV file into a DataFrame.
        dataframe = spark.read.csv("file:///Users/balah/Desktop/Spark/data/csv/fake-friends-with-header.csv", header=True, inferSchema=True)

        # Print the schema of the DataFrame.
        # dataframe.printSchema()

        # Register the DataFrame as a temporary view.
        dataframe.createTempView("fake_friends")

        # Define the aggregation function to calculate the average number of friends,
        # rounding it to 2 decimal places.
        agg_column = funcs.avg("number_of_friends")
        agg_column = funcs.round(agg_column, 2)
        agg_column = agg_column.alias("average_number_of_friends")

        # Print the aggregation column to see its structure.
        # This will show the expression used for aggregation :
        # Column<'round(avg(number_of_friends), 2) AS average_number_of_friends'>
        print(agg_column)

        # Group by age and calculate the average number of friends.
        agg = dataframe.groupby("age").agg(agg_column).orderBy('age', ).show()
        #  +-----+---------------------------+
        #  | age | average_number_of_friends |
        #  +-----+---------------------------+
        #  |  18 |                    343.38 |
        #  |  19 |                    213.27 |
        #  |  20 |                     165.0 |
        #  |  21 |                    350.88 |
        #  | ... |                       ... |
        #  +-----+---------------------------+