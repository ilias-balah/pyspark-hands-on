"""
Practice pySpaek dataframes by converting an RDD to a DataFrame.
"""
from src.internal.proxy_spark_session import ProxySparkSession, Row, DataFrame

if __name__ == '__main__':

    with ProxySparkSession.builder.appName("Test Spark Session").getOrCreate() as spark:

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
        rdd = spark.sparkContext.textFile("file:///Users/balah/Desktop/Spark/data/csv/fake-friends.csv")

        # Map each line to a Row object.
        rdd_rows = rdd.map(create_row)

        # Create a DataFrame from the RDD.
        dataframe: DataFrame = spark.createDataFrame(rdd_rows)

        # Show the DataFrame.
        dataframe.show()