# -*- coding: utf-8 -*-
from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import SparkSession

# Import the user-defined utilities
from .utils.helpers import TimeUtils
from .utils.logger import LoggerUtils


class ProxySparkSession:
    """
    A context-managed wrapper for SparkSession to add log control and session timing.
    """

    # Define a class to hold the default values and valid log levels.
    class defaults:
        app_name: str = 'Proxy Spark Session'
        log_level: str = 'error'
        valid_log_level: list[str] = ['all', 'debug', 'error', 'fatal', 'info', 'off', 'trace', 'warn']

    # Define a class to hold messages for session creation and ending.
    class messages:
        session_creation: str = "Spark Session '{}' created successfully."
        session_termination: str = "Spark Session '{}' terminated successfully : took {} microseconds."
        invalid_app_name: str = "Invalid app name '{}' provided. Defaulting to '{}'."
        invalid_log_level: str = "Invalid log level '{}' provided. Defaulting to '{}'."

    logger = LoggerUtils.get_logger("ProxySparkSessionLogger")

    def __init__(self, app_name: str = None, log_level: str = None, **kwargs):
        """
        Initialize the ProxySparkSession with the given arguments.
        Defaults the app name to 'ProxySparkSession' if not provided.
        """
        # Initialize the start and end times for session duration tracking.
        self._start_time = TimeUtils.get_current_time()
        self._end_time: TimeUtils.time_type = None
        # Create a placeholder for the Spark session.
        self._session: SparkSession = None
        # Save the provided arguments, for future reference, and validate them
        # using the defaults class.
        self._user_args = {
            'app_name': app_name,
            'log_level': log_level,
            **kwargs
        }

    @property
    def context(self) -> SparkContext:
        """
        Get the SparkContext associated with the Spark session.
        """
        # Before accessing the SparkContext, ensure that the Spark session
        # has been started.
        if not self._session:
            raise RuntimeError("Spark session has not been started. Use "
                               "'with ProxySparkSession() as spark:' to "
                               "start the session.")
        return self._session.sparkContext
    
    @property
    def app_name(self) -> str:
        """
        Get the application name of the Spark session.
        """
        return self.context.appName

    def __enter__(self):
        """
        Enter the runtime context, set log level, and return the Spark session instance.
        """
        # Create the spark session with the provided app name if valid,
        # otherwise use the default app name.
        self._session = SparkSession.builder \
                                    .appName(self._validate_app_name(self._user_args['app_name'])) \
                                    .getOrCreate()
        # Print the app name and the start time.
        self._inform_start()
        # Enforce teh user-provided log level on the Spark session.
        self.context.setLogLevel(self._validate_log_level(self._user_args['log_level']))
        # Return the Spark session instance for use within the context.
        return self._session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        """
        # Terminate the Spark session.
        self._session.stop()
        # Inform the user that the session has ended and calculate
        # the duration.
        self._inform_termination()

    def _inform_start(self):
        """
        Inform the user that the Spark session has started, and set the start time.
        """
        # Reset the start time to the current time.
        self._start_time = TimeUtils.get_current_time()
        # Print the app name and the start time of the session.
        self.logger.debug(self.messages.session_creation.format(self.app_name))
        # Return the start time in microseconds.
        return self._start_time

    def _inform_termination(self) -> int:
        """
        End the Spark session and calculate the duration.
        This method is called when exiting the context manager.
        """
        # Set the end time to the current time if it hasn't been set yet.
        if not self._end_time:
            self._end_time = TimeUtils.get_current_time()
        # Calculate the session duration in microseconds.
        duration_us = TimeUtils.get_delta_microseconds(self._start_time, self._end_time)
        # Print the end message with the app name and duration.
        self.logger.debug(self.messages.session_termination.format(self.app_name, duration_us))
        # Return the duration in microseconds.
        return duration_us
    
    def _validate_app_name(self, name: str) -> str:
        """
        Validate the application name. If not provided, default to
        'Proxy Spark Session'.
        """
        # Ensure the app name is a non-empty string.
        if not name or not isinstance(name, str):
            # Default to the default app name `defaults.app_name`.
            self.logger.warning(self.messages.invalid_app_name.format(name, self.defaults.app_name))
            name = self.defaults.app_name
        # Return the validated app name.
        return name

    def _validate_log_level(self, log_level: str) -> str:
        """
        Validate the log level against Spark's supported levels.
        """
        if log_level not in self.defaults.valid_log_level:
            # Default to the default log level `defaults.log_level`.
            self.logger.warning(self.messages.invalid_log_level.format(log_level, self.defaults.log_level))
            log_level = self.defaults.log_level
        # Return the validated log level.
        return log_level