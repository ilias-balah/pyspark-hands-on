"""
Practice pyspark dataframes by converting an RDD to a DataFrame.
"""
from pyspark.sql import Row, DataFrame
from src.internal.spark.proxies import SparkSessionProxy



if __name__ == '__main__':

    with SparkSessionProxy("RDD to dataframe") as spark:

        def create_row(line):
            """
            Create a Row object from a line of CSV data.
            """
            # Split the line to get the fields in a list.
            fields = line.split(',')

            # Prepare the fields for the Row object.
            id = int(fields[0])
            name = fields[1]
            age = int(fields[2])
            num_friends = int(fields[3])

            # Return a Row object with the fields.
            return Row(id=id, name=name, age=age, num_friends=num_friends)

        # Read the fake friends CSV file into an RDD.
        rdd = spark.session.sparkContext.textFile("file:///Users/balah/Desktop/Spark/data/csv/fake-friends.csv")

        # Map each line to a Row object.
        rdd_rows = rdd.map(create_row)

        # Create a DataFrame from the RDD.
        # NOTE: We can cache the RDD to improve performance if we plan to use it multiple times.
        dataframe: DataFrame = spark.session.createDataFrame(rdd_rows).cache()

        # Print the schema of the DataFrame.
        # dataframe.printSchema()

        # NOTE: Before querying the DataFrame, we need to register it as a temporary view.
        dataframe.createOrReplaceTempView("fake_friends")

        # Excute a SQL query to get the teenagers in the DataFrame.
        # NOTE: The results of a `spark.sql` query are also a DataFrame.
        teenagers_with_sql = spark.session.sql("SELECT * FROM fake_friends WHERE age >= 13 AND age <= 19 ORDER BY age, num_friends ASC")

        # NOTE: Alternatively, we can use pre-defined functions to query the DataFrame.
        teenagers_with_funcs = dataframe.filter("age >= 13 AND age <= 19").orderBy("age", "num_friends", ascending=True)

        # Ensure that both methods yield the same results.
        assert teenagers_with_funcs.toPandas().eq(teenagers_with_sql.toPandas()).all().all()