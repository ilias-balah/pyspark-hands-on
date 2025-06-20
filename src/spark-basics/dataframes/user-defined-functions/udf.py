# NOTE: To avoid the following user warning : `...lib\site-packages\pyspark\pandas
# \__init__.py:43: UserWarning: 'PYARROW_IGNORE_TIMEZONE' environment variable was
# not set. It is required to set this environment variable to '1' in both driver
# and executor sides if you use pyarrow>=2.0.0. pandas-on-Spark will set it for
# you but it does not work if there is a Spark context already launched.` we
# need to set the environment variable before importing pyspark.pandas.
import os; os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
from pyspark.pandas import DataFrame as SparkDataFrame
from pyspark.sql import types as st

from src.internal.spark.proxies import SparkSessionProxy
from src.utils.spark_functions import spark_funcs


def __main__():
    """
    Main function to demonstrate the use of User Defined Functions (UDFs) in PySpark.
    This function creates a Spark session, defines a UDF to calculate the age of employees
    in the next 10 years, and applies this UDF to a pandas DataFrame of employees.
    """

    default_configs = dict({
        # Set environment variable for PyArrow compatibility
        "spark.ExecutorEnv.PYARROW_IGNORE_TIMEZONE": "1",
        # Disable ANSI mode for compatibility with certain operations
        "spark.sql.ansi.enabled": "false",
    })

    with SparkSessionProxy("Test UDFs", **default_configs) as spark:

        # Create a pandas DataFrame of employees with their age and salary
        # This dataframe will have 10 rows and 4 columns: ID, name, age, and salary
        pandas_df = SparkDataFrame({
            'ID': range(1, 11),
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy'],
            'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
            'salary': [70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 150000, 160000]
        }) \
            .to_spark(index_col='index')

        # Define a UDF to calculate the age in 10 years
        @spark_funcs.udf(returnType=st.StringType())
        def in_10_years(age: int) -> int:
            """
            Calculate the age in 10 years.
            """
            return age + 10

        # Apply the UDF to the 'age' column
        _in_10_years_df = pandas_df.withColumn('age_in_10_years', in_10_years(pandas_df['age']))

        # Show the resulting DataFrame with the new 'age_in_10_years' column
        spark.logger.info("DataFrame with age in 10 years :")
        _in_10_years_df.show()


if __name__ == '__main__':

    try:
        __main__()

    except Exception as e:
        pass