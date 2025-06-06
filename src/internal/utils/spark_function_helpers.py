from pyspark.sql import functions as spark_funcs
from pyspark.sql import Column

class SparkFunctionNotFoundError(Exception):
    """
    Raised when a function name does not match any available function
    in `pyspark.sql.functions`.
    """
    def __init__(self, function_name: str):
        message = "The function '{}' is not found in `pyspark.sql.functions`."
        super().__init__(message.format(function_name))


class InvalidColumnAliasError(Exception):
    """
    Raised when the provided alias is not a valid Python identifier
    (e.g., contains spaces or starts with a digit).
    """
    def __init__(self, alias: str):
        message = "The alias '{}' is not a valid Python identifier."
        super().__init__(message.format(alias))


def apply_spark_function_with_alias(column_name: str, spark_function_name: str, alias_name: str | None = None) -> Column:
    """
    Applies a PySpark SQL function to a given column name and optionally
    assigns an alias to the resulting column.

    Parameters
    ----------
    column_name: str
        The name of the column to apply the function to.
    spark_function_name: str
        The name of the function from pyspark.sql.functions to apply.
    alias_name: str, optional
        An optional alias to assign to the resulting column.

    Returns
    -------
    pyspark.sql.column.Column
        The resulting PySpark Column object.

    Raises
    ------
    - `SparkFunctionNotFoundError` if the specified function is not found.
    - `InvalidColumnAliasError` if the alias name is not a valid Python identifier.
    """
    # Try to retrieve the function by name
    spark_function = getattr(spark_funcs, spark_function_name, None)

    # Ensure the function is found
    if spark_function is None:
        raise SparkFunctionNotFoundError(spark_function_name)

    # Apply the function to the specified column
    transformed_column = spark_function(column_name)

    # Assign alias if provided
    if alias_name is not None:
        # Ensure the provided alias is an identifier
        if not alias_name.isidentifier():
            raise InvalidColumnAliasError(alias_name)
        # Rename the column with the alias
        transformed_column = transformed_column.alias(alias_name)            

    return transformed_column