"""
Exercice, to calculate the minimum temperature for each year in a given dataset.
"""
from pyspark.sql import functions as funcs
# from pyspark.sql.types import StructField
from pyspark.sql import types as st
from src.internal.proxy_spark_session import ProxySparkSession
from src.internal.utils.spark_function_helpers import apply_spark_function_with_alias


if __name__ == '__main__':

    with ProxySparkSession("Exercice for spark dataframes") as spark:

        # Customize the datfarme schema
        schema = st.StructType([
            st.StructField('station_id', st.StringType(), False),
            st.StructField('datetime', st.IntegerType(), False),
            st.StructField('element_type', st.StringType(), False),
            st.StructField('value', st.IntegerType(), False)
        ])

        # Read the temperature data CSV file into a DataFrame.
        dataframe = spark.read.schema(schema).csv("file:///Users/balah/Desktop/Spark/data/csv/1800.csv")

        # ---------------------------------------------------------------------
        # Select the minimum temerature for each station using a SQL query.
        # ---------------------------------------------------------------------
        # NOTE: Before quering any table, we need to register the
        # dataFrame as a temporary view first.
        dataframe.createTempView("temperature_table")
        spark.sql("""
            SELECT station_id, min(value) * 0.1 as min_value
            FROM temperature_table
            WHERE element_type = 'TMIN'
            GROUP BY station_id
            ORDER BY min(value)
        """)\
            .show()

        # ---------------------------------------------------------------------
        # Select the minimum temerature for each station using spark-functions.
        # ---------------------------------------------------------------------
        dataframe \
            .where(dataframe.element_type == 'TMIN') \
            .withColumn('value', dataframe.value * 0.1) \
            .groupby('station_id') \
            .agg(apply_spark_function_with_alias('value', 'min', 'min_value')) \
            .orderBy('min_value') \
            .show() \

        
