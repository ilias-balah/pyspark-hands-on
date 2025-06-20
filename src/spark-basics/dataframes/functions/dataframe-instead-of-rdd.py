"""
Practice using DataFrames instead of RDDs, and some basic DataFrame operations.
"""
from src.internal.spark.proxies import SparkSessionProxy
from src.utils.spark_functions import spark_funcs


if __name__ == '__main__':

    with SparkSessionProxy("Test Dataframes instead of RDDs") as spark:

        # Read the fake friends CSV file into a DataFrame.
        dataframe = spark.session.read.csv("file:///Users/balah/Desktop/Spark/data/csv/fake-friends-with-header.csv", header=True, inferSchema=True)

        # Print the schema of the DataFrame.
        # dataframe.printSchema()

        # Register the DataFrame as a temporary view.
        dataframe.createTempView("fake_friends")

        # Define the aggregation function to calculate the average number of friends,
        # rounding it to 2 decimal places.
        agg_column = spark_funcs.avg("number_of_friends")
        agg_column = spark_funcs.round(agg_column, 2)
        agg_column = agg_column.alias("average_number_of_friends")

        # Print the aggregation column to see its structure.
        # This will show the expression used for aggregation :
        # Column<'round(avg(number_of_friends), 2) AS average_number_of_friends'>
        spark.logger.print(agg_column, as_log = False)

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