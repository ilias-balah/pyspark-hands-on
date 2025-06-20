import pyspark.sql as pyspark_sql
from src.logs import logging_utils



class SparkSession(pyspark_sql.SparkSession):
    """
    A customized extension of the base SparkSession that supports
    additional features like structured logging.

    Additional Attributes
    ----------
    logger : logging.Logger
        Optional logger for session-level logging.
    """

    # def __init__(self, sparkContext, jsparkSession = None, options = ...):
    #     """
    #     Initialize the extended SparkSession.
    #     """
    #     super().__init__(sparkContext, jsparkSession, options)
    #     # Add a placeholder for the logger to be attached to the session object.
    #     self.logger: logging_utils.Logger = None

    # def add_logger(self, logger: logging_utils.Logger) -> logging_utils.Logger:
    #     """
    #     Attach a logger to the SparkSession instance if not already set.

    #     Parameters
    #     ----------
    #     logger : logging.Logger
    #         The logger to associate with this session.

    #     Returns
    #     -------
    #     logging.Logger
    #         The newly added logger, or the existing one if already set.
    #     """
    #     if not hasattr(self, 'logger') or self.logger is None:
    #         self.logger = logger

    #     return self.logger