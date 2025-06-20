# NOTE: To avoid the following user warning : `...lib\site-packages\pyspark\pandas
# \__init__.py:43: UserWarning: 'PYARROW_IGNORE_TIMEZONE' environment variable was
# not set. It is required to set this environment variable to '1' in both driver
# and executor sides if you use pyarrow>=2.0.0. pandas-on-Spark will set it for
# you but it does not work if there is a Spark context already launched.` we
# need to set the environment variable before importing pyspark.pandas.
import os; os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import pyspark.pandas as ps
from src.internal.spark.session import SparkSession
from src.logs import logging_utils


def _age_in_10_years(value: int) -> int:
    """
    Calculate the age of an employee in the next 10 years.
    """
    # NOTE: We used a match-case statement to calculate the age in 10 years
    # for learning purposes. In practice, you might use a simple addition.
    match value:
        # If the age is less than 0, return 0
        case _ if value < 0: return 0
        # Otherwise, return the age plus 10
        case _: return value + 10


def _categorize_salary(value) -> str:
    """
    Categorize the salary into 'Low', 'Medium', or 'High'.
    """
    # NOTE: We used a match-case statement to categorize the salary
    # into 'Low', 'Medium', or 'High' based on the value,
    # for learning purposes. In practice, you might use if-elif-else
    # statements for such cases.
    match value:
        # Low salary if less than 80,000
        case _ if value < 80000: return 'Low'
        # Medium salary if between 80,000 and 120,000
        case _ if value < 120000: return 'Medium'
        # High salary if 120,000 or more
        case _: return 'High'


def _calculate_raise(row):
    """
    Calculate the expected raise based on the employee's salary and age.
    """
    row_age = row['age']
    row_salary = row['salary']
    # 10% raise for employees under 30
    if row_age < 30: return int(row_salary * 0.10)
    # 7% raise for employees between 30 and 50
    elif row_age < 50: return int(row_salary * 0.07)
    # 5% raise for employees over 50
    else: return int(row_salary * 0.05)


def see_employees_age_in_10_years(pandas_df: ps.DataFrame) -> ps.DataFrame:
    """
    Calculate the age of employees in the next 10 years.
    """
    _in_10_years_df = pandas_df.copy()
    # NOTE: The function `transform()` is usually used for element-wise operations
    _in_10_years_df['age_int_10_years'] = _in_10_years_df['age'].transform(_age_in_10_years)
    # Return the modified DataFrame with the new age column
    return _in_10_years_df


def see_employees_with_categorized_salary(pandas_df: ps.DataFrame) -> ps.DataFrame:
    """
    Categorize the salary of employees into 'Low', 'Medium', or 'High'.
    """
    _categorized_salary_df = pandas_df.copy()
    # Apply the categorization function to the salary column
    _categorized_salary_df['salary_category'] = _categorized_salary_df['salary'].apply(_categorize_salary)
    # Return the modified DataFrame with the new salary category column
    return _categorized_salary_df

def see_employees_with_expected_raise(pandas_df: ps.DataFrame) -> ps.DataFrame:
    """
    Calculate the expected raise for each employee based on their age and salary.
    """
    _raise_df = pandas_df.copy()
    # Apply the raise calculation function to each row
    _raise_df['expected_raise'] = _raise_df.apply(_calculate_raise, axis=1)
    # Return the modified DataFrame with the new expected raise column
    return _raise_df


if __name__ == '__main__':

    # Initialize a Spark session with required configurations
    spark = SparkSession.builder \
        .appName("Pandas on Spark") \
        .config("spark.sql.ansi.enabled", "false") \
        .config("spark.ExecutorEnv.PYARROW_IGNORE_TIMEZONE", "1") \
        .getOrCreate()
    
    logger = logging_utils.get_logger(__name__)

    # Create a pandas DataFrame of employees with their age and salary
    # This dataframe will have 10 rows and 4 columns: ID, name, age, and salary
    pandas_df = ps.DataFrame({
        'ID': range(1, 11),
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy'],
        'age': [25, 30, 35, 40, 45, 50, 55, 60, 65, 70],
        'salary': [70000, 80000, 90000, 100000, 110000, 120000, 130000, 140000, 150000, 160000]
    })

    # -----------------------------------------------------------------------------
    # Practice using pandas API on Spark, with transformations and operations
    # on columns, by calculate the age in the next 10 years, and categorizing
    # the salary into 'Low', 'Medium', or 'High'.
    # -----------------------------------------------------------------------------
    logger.print("Employees DataFrame with age in the next 10 years :")
    logger.print(see_employees_age_in_10_years(pandas_df).head(), as_log = False)

    logger.print("Employees DataFrame with categorized salary :")
    logger.print(see_employees_with_categorized_salary(pandas_df).head(), as_log = False)

    # -----------------------------------------------------------------------------
    # Practice using pandas API on Spark, with transformations and operations
    # on rows, by calculating the expected raise for each employee based on
    # their age and salary.
    # -----------------------------------------------------------------------------
    logger.print("Employees DataFrame with expected raise :")
    logger.print(see_employees_with_expected_raise(pandas_df).head(), as_log = False)