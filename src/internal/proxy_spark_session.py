# -*- coding: utf-8 -*-
from dataclasses import dataclass

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql import SparkSession

# Import the user-defined utilities
from .utils.helpers import TimeUtils as _time_utils
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
    invalid_app_name: str = lambda *args: "Invalid app name '{}' provided. Defaulting to '{}'.".format(*args)
    invalid_log_level: str = lambda *args: "Invalid log level '{}' provided. Defaulting to '{}'.".format(*args)
    

def _get_logger(name):
    """
    Get a logger instance with the specified name.
    """
    return LoggerUtils.get_logger(name)


class _BaseProxySession:

    # Define a unique logger
    _logger = _get_logger(_Defaults.logger_name)

    def __init__(self):
        """
        Initialize the base class and initiate the time management's variables.
        """
        self._start_time = _time_utils.get_current_time()
        self._end_time: _time_utils.time_type = None
        # Create a placeholder for the Spark session.
        self._session: SparkSession = None

    def _get_session(self, app_name: str, configs: dict = None) -> SparkSession:
        """
        Create, or retrieve if already exists, a Spark session instance.

        Parameters
        ----------
        app_name : str
            The name of the Spark application. Defaults to 'Proxy Spark Session'.

        configs : dict, optional
            A dictionary of Spark configuration key-value pairs to set for the session.
            If not provided, defaults to an empty dictionary.

        Returns
        -------
        SparkSession
            A Spark session instance with the specified application name and configurations.
            Returns None if there is an error during session creation or retrieval.
        """
        self._logger.debug("Creating/retrieving a spark session ...".format(app_name or _Defaults.app_name))

        if configs is None:
            configs = {}
            
        if not isinstance(configs, dict):
            raise TypeError("The 'configs' parameter must be a dictionary.")

        # Prepare a builder for the Spark session.
        builder = SparkSession.builder
        
        for key, value in configs.items():

            # Ensure that the key is a string and the value is not None.
            if not isinstance(key, str) or value is None:
                raise ValueError("Invalid configuration key-value pair : {} = {}. "
                                 "Key must be a string and value must not be None."
                                 .format(key, value))
            # Attempt to set the configuration in the builder.
            try:
                builder = builder.config(key, value)
            
            # Log any exceptions that occur while setting the configuration, and
            # return None to indicate failure.
            except Exception as e:
                self._logger.error("Failed to set the configuration \"{}\" = {} : {}.".format(key, value, e))
                self._logger.print("Traceback : ", exc_info=True)
                return None

        # Set the application name for the Spark session.
        builder = builder.appName(app_name or _Defaults.app_name)

        # Attempt to create or retrieve the Spark session.
        try:
            ss = builder.getOrCreate()

        # Log any exceptions that occur while creating or retrieving the Spark session.
        except Exception as e:
            self._logger.error("Failed to create or retrieve a spark session : {}".format(e))
            self._logger.print("Traceback : ", exc_info=True)
            return None

        # Log the successful creation or retrieval of the Spark session.
        self._logger.debug("Session '{}' created successfully.".format(ss.sparkContext.appName))

        # Finally, return the Spark session instance.
        return ss

    def _stop_session(self, session: SparkSession) -> None:
        """
        Stop the Spark session if it exists.

        This method should be called to clean up the Spark session when it is
        no longer needed.

        Parameters
        ----------
        session : SparkSession
            The Spark session to stop. If None, no action is taken.
        """
        if not session:
            # If the session is None, log a warning and return.
            self._logger.warning("No session to stop. Skipping cleanup.")
            return
        
        # Log the session termination message with the app name.
        app_name = session.sparkContext.appName if session.sparkContext else "Unknown"
        
        # Attempt to stop the Spark session and log the result.
        try:
            self._logger.debug("Terminating session '{}' ...".format(app_name))
            session.stop()
            self._logger.debug("Session terminated successfully.")
        
        # Log the session termination message with the duration.
        except Exception as e:
            self._logger.error("Failed to terminate session '{}' : {}".format(app_name, e))
            self._logger.print("Traceback :", exc_info=True)

    def _mark_session_start(self) -> None:
        """
        Record the start time of the Spark session.

        This method should be called when entering the context manager.
        """
        # Reset the start time to the current time.
        self._start_time = _time_utils.get_current_time()

    def _mark_session_end(self) -> None:
        """
        Record the end time and compute the session duration.

        This method should be called when exiting the context manager.
        """
        # Reset the end time to the current time.
        self._end_time = _time_utils.get_current_time()

    def _get_session_duration(self) -> float:
        """
        Calculate the duration of the Spark session in seconds.

        This method should be called after marking the session end.
        Returns the duration in seconds.
        """
        # Ensure that both start and end times are set.
        if not self._start_time or not self._end_time:
            raise RuntimeError("Session start or end time is not set. "
                               "Ensure that the session is properly started and ended.")
        # Calculate the duration in seconds.
        return _time_utils.get_delta_seconds(self._start_time, self._end_time)

    def __enter__(self, *args, **kwargs):
        """
        Enter the runtime context, and return the Spark session instance.
        """
        # Mark the session start time.
        self._mark_session_start()
        # Return the Spark session instance for use within the context manager.
        return self._session if self._session else None
    
    def __exit__(self, exc_type, exc_val, exc_tb, *args, **kwargs):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        Logs or handles any exception raised within the context before termination.
        """
        if exc_type:
            # Log or handle the exception before Spark session cleanup
            self._logger.error(f"{exc_type.__name__} : {exc_val}")
            self._logger.print("Traceback :", exc_info=exc_tb)
        # Mark the session end time.
        self._mark_session_end()


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
        # Extract the configuration parameters from the kwargs if provided under
        # the key `configs`.
        self._user_configs = kwargs.pop('configs', {})
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
        self._session = self._get_session(__validated_app_name, self._user_configs)
        # Enforce the user-provided log level on the Spark session.
        self._session.sparkContext.setLogLevel(__validated_log_level)
        # Return the Spark session instance for use within the context manager.
        return super().__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        """
        # Call the base class exit method
        super().__exit__(exc_type, exc_val, exc_tb)
        # Stop the Spark session if it exists.
        self._stop_session(self._session)
        # Log the session's duration
        self._logger.info("Ran for {:.3f} seconds.".format(self._get_session_duration()))

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