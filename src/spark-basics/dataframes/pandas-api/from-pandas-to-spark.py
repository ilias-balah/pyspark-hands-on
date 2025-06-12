# NOTE: To avoid the following user warning : `...lib\site-packages\pyspark\pandas
# \__init__.py:43: UserWarning: 'PYARROW_IGNORE_TIMEZONE' environment variable was
# not set. It is required to set this environment variable to '1' in both driver
# and executor sides if you use pyarrow>=2.0.0. pandas-on-Spark will set it for
# you but it does not work if there is a Spark context already launched.` we
# need to set the environment variable before importing pyspark.pandas.
import os; os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
# NOTE: To use pandas on Spark, we need to disable the ANSI mode
import pyspark.pandas as ps; ps.options.compute.fail_on_ansi_mode = False
import pandas as pd

from src.internal.proxy_spark_session import ProxySparkSession


if __name__ == '__main__':

    # Read the fake-friends dataset
    pandas_df = pd.read_csv("C:/Users/balah/Desktop/Spark/data/csv/fake-friends.csv", names=['ID', 'name', 'age', 'number_of_friends'])
    
    # ---------------------------------------------------------------------------
    # Convert the pandas DataFrame to a Spark DataFrame with a spark session
    # ---------------------------------------------------------------------------
    with ProxySparkSession("Test Pandas API") as spark:

        # Create a Spark DataFrame from the pandas DataFrame
        spark_df = spark.createDataFrame(pandas_df)

        # Show the created Spark dataframe
        spark_df.show(5)

    # ---------------------------------------------------------------------------
    # Convert the pandas Dataframe to a spark dataframe using pandas API on Spark
    # ---------------------------------------------------------------------------
    spark_df = ps.from_pandas(pandas_df).to_spark()

    # Show the created Spark dataframe
    spark_df.show(5)

    # Convert the Spark DataFrame back to a pandas DataFrame
    pandas_df_converted = spark_df.toPandas()

    # Ensure the conversion is correct
    assert isinstance(pandas_df_converted, pd.DataFrame)