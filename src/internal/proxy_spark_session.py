# -*- coding: utf-8 -*-
from dataclasses import dataclass

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import SparkSession

# Import the user-defined utilities
from .utils.helpers import TimeUtils
from .utils.logger import LoggerUtils


@dataclass
class _Defaults:
    """
    A class to hold the default values and valid log levels.
    """
    app_name: str = 'Proxy Spark Session'
    logger_name: str = '{} Logger'.format(app_name)
    log_level: str = 'error'
    valid_log_level: tuple[str] = ('all', 'debug', 'error', 'fatal', 'info', 'off', 'trace', 'warn')


@dataclass
class _Messages:
    """
    A class to hold messages for session creation and ending.
    """
    session_creation: str = lambda *args: "Spark Session '{}' created successfully.".format(*args)
    session_termination: str = lambda *args: "Spark Session '{}' terminated successfully : took {:.3f} seconds.".format(*args)
    invalid_app_name: str = lambda *args: "Invalid app name '{}' provided. Defaulting to '{}'.".format(*args)
    invalid_log_level: str = lambda *args: "Invalid log level '{}' provided. Defaulting to '{}'.".format(*args)
    

def _get_logger(name):
    """
    """
    return LoggerUtils.get_logger(name)


class _BaseProxySession:

    # Define a unique logger
    _logger = _get_logger(_Defaults.logger_name)

    def __init__(self):
        """
        Initialize the base class and initiate the time management's variables.
        """
        self._start_time = TimeUtils.get_current_time()
        self._end_time: TimeUtils.time_type = None
        # Create a placeholder for the Spark session.
        self._session: SparkSession = None

    @property
    def _app_name(self):
        """
        The the spark's application's name if already set under the name
        `app_name`, otherwise, the default.
        """
        return getattr(self, 'app_name', _Defaults.app_name)

    def _session_created(self):
        """
        Inform the user that the Spark session has started, and set the start time.
        """
        # Reset the start time to the current time.
        self._start_time = TimeUtils.get_current_time()
        # Log the creation message, with the app name if already set.
        self._logger.debug(_Messages.session_creation(self._app_name))
        # Return the start time in seconds.
        return self._start_time

    def _session_terminated(self) -> int:
        """
        End the Spark session and calculate the duration.
        This method is called when exiting the context manager.
        """
        # Set the end time to the current time.
        self._end_time = TimeUtils.get_current_time()
        # Calculate the session duration in seconds.
        duration_us = TimeUtils.get_delta_seconds(self._start_time, self._end_time)
        # Print the end message with the app name and duration.
        self._logger.debug(_Messages.session_termination(self._app_name, duration_us))
        # Return the duration in seconds.
        return duration_us

    def __enter__(self):
        """
        Enter the runtime context, and return the Spark session instance.
        """
        self._session_created()
        # Return the Spark session instance for use within the context manager.
        return self._session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        """
        self._session_terminated()



class ProxySparkSession(_BaseProxySession):
    """
    A context-managed wrapper for SparkSession to add log control and session timing.
    """

    def __init__(self, app_name: str = None, log_level: str = None, **kwargs):
        """
        Initialize the ProxySparkSession with the given arguments.
        Defaults the app name to 'ProxySparkSession' if not provided.
        """
        super().__init__()
        # Save the provided arguments, for future reference.
        self._user_args = { 'app_name': app_name, 'log_level': log_level, **kwargs}


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
        __validated_app_name = self._validate_app_name(self._user_args['app_name'])
        __validated_log_level = self._validate_log_level(self._user_args['log_level'])
        # Create the spark session with the provided app name if valid,
        # otherwise use the default app name.
        self._session = SparkSession.builder.appName(__validated_app_name).getOrCreate()
        # Enforce the user-provided log level on the Spark session.
        self._session.sparkContext.setLogLevel(__validated_log_level)
        # Return the Spark session instance for use within the context manager.
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        """
        # Terminate the Spark session.
        try:
            self._session.stop()
        except Exception as e:
            self._logger.error("Failed to stop the spark session.", exc_info=1)
        finally:
            super().__exit__(exc_type, exc_val, exc_tb)
    
    def _validate_app_name(self, name: str) -> str:
        """
        Validate the application name. If not provided, default to
        'Proxy Spark Session'.
        """
        # Ensure the app name is a non-empty string.
        if not name or not isinstance(name, str):
            # Default to the default app name `defaults.app_name`.
            self._logger.warning(_Messages.invalid_app_name(name, _Defaults.app_name))
            name = _Defaults.app_name
        # Return the validated app name.
        return name

    def _validate_log_level(self, log_level: str) -> str:
        """
        Validate the log level against Spark's supported levels.
        """
        if log_level not in _Defaults.valid_log_level:
            # Default to the default log level `defaults.log_level`.
            self._logger.warning(_Messages.invalid_log_level(log_level, _Defaults.log_level))
            log_level = _Defaults.log_level
        # Return the validated log level.
        return log_level