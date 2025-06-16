from pyspark import SparkContext, SparkConf
from src.logs import logging_utils


def assert_spark_context_appname(spark_context: SparkContext, expected_app_name: str):
    """
    Assert that the SparkContext has the expected application name.
    """
    assert spark_context is not None, "SparkContext should not be None"
    assert hasattr(spark_context, "appName"), "SparkContext should have an 'appName' attribute"
    assert spark_context.appName == expected_app_name, "Expected appName '{}', but got '{}'".format(expected_app_name, spark_context.appName)


def test_spark_context():
    """
    Test to ensure that a SparkContext can be created successfully.
    """
    # Test with default configuration
    logger.debug("Testing SparkContext creation with default configuration ...")
    default_app_name = "pyspark-shell"
    with SparkContext.getOrCreate() as spark_context:
        assert_spark_context_appname(spark_context, default_app_name)

    # Test with specific configuration
    logger.debug("Testing SparkContext creation with specific configuration ...")
    spark_app_name = "Test spark context"
    configuration = SparkConf().setAppName(spark_app_name).setMaster("local[*]")
    with SparkContext(conf=configuration) as spark_context:
        assert_spark_context_appname(spark_context, spark_app_name)


if __name__ == "__main__":

    # Initialize logger
    logger = logging_utils.get_logger(__name__)

    try:
        test_spark_context()
        logger.info("Test passed successfully.")

    except AssertionError as e:
        logger.error("Test failed : {}".format(e))
        raise