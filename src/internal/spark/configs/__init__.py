import pyspark


class SparkConf(pyspark.SparkConf):
    """
    A wrapper around PySpark's SparkConf to provide structured default configurations.

    This class introduces organized defaults tailored for different execution modes,
    such as session-based or context-based Spark environments, and includes common
    settings that enhance compatibility and development ergonomics.
    """

    class Defaults:
        """
        Container for predefined Spark configuration profiles.

        The configurations are grouped based on their intended scope:
        - `session` : Specific to SparkSession-related jobs.
        - `context` : Specific to low-level SparkContext use.
        """

        # Common settings shared across session and context modes
        __common__ = {
            # Ensure consistent behavior between PyArrow and Spark
            "spark.ExecutorEnv.PYARROW_IGNORE_TIMEZONE": "1",
            # Disable ANSI SQL mode for more lenient SQL expression behavior
            "spark.sql.ansi.enabled": "false",
        }

        # Additional settings specific to SparkSession
        __session__ = {
            # Future session-specific settings can be added here
        }

        # Additional settings for SparkContext
        __context__ = {
            # Use local master with all available cores
            "spark.master": "local"
        }

        session = dict(**__common__, **__session__)
        """
        Default configuration for use with SparkSession
        """

        context = dict(**__common__, **__context__)
        """
        Default configuration for use with SparkContext
        """
