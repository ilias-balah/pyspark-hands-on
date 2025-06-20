import pyspark.sql as pyspark_sql



class SparkSession(pyspark_sql.SparkSession):
    """
    A customized extension of the base SparkSession that supports
    additional features like structured logging.

    Additional Attributes
    ----------
    logger : logging.Logger
        Optional logger for session-level logging.
    """

    class Defaults:
        """
        A class to hold default values for Spark session configurations.
        """
        spark_configs: dict[str, str] = dict({
            # Set environment variable for PyArrow compatibility
            "spark.ExecutorEnv.PYARROW_IGNORE_TIMEZONE": "1",
            # Disable ANSI mode for compatibility with certain operations
            "spark.sql.ansi.enabled": "false",
        })