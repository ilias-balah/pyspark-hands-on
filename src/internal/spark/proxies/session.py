from src.internal.spark.session import SparkSession
from src.internal.spark.configs import SparkConf
from src.logs import logging_utils
from src.utils import time_utils


class SparkSessionProxy:
    """
    A context manager-based proxy for managing the lifecycle of a
    SparkSession.

    This class simplifies the creation, configuration, and shutdown
    of a SparkSession, while adding enhanced logging, structured
    configuration handling, and support for measuring execution time.

    Ideal for use in scripts and applications where a reliable, configurable,
    and debuggable SparkSession setup is needed.
    """

    # Logger used throughout the lifecycle of the proxy
    logger = logging_utils.get_logger(__qualname__)

    def __init__(self, app_name: str = None, **config_options):
        """
        Initialize the proxy with an optional application name and
        additional configuration.

        Parameters
        ----------
        app_name : str, optional
            The name of the Spark application. Overrides the default if provided.

        **config_options : dict
            Additional key-value pairs for Spark configuration settings.
        """
        self.configs: dict = self._get_configs(app_name = app_name, **config_options)
        self.start_time: time_utils.time_type = time_utils.get_current_time()
        self.end_time: time_utils.time_type = None
        self.session: SparkSession = None

    def _get_configs(self, app_name: str = None, **overrides) -> dict:
        """
        Generate the Spark configuration by merging a default app name and
        user-specified options.

        Parameters
        ----------
        app_name : str, optional
            Optional Spark application name. Used if provided.

        **overrides : dict
            Custom Spark configurations to override default or initial values.

        Returns
        -------
        dict
            A dictionary of valid Spark configuration entries.

        Raises
        ------
        ValueError
            If any configuration key is not a string or any value is None.
        """
        configs = SparkConf.Defaults.session.copy()

        # Set application name if provided
        configs["spark.app.name"] = app_name or SparkSession.Defaults.app_name

        # Update with any additional user-specified configuration overrides
        for key, value in overrides.items():
            # Ensure that the key is a string and the value is not None
            if not isinstance(key, str) or value is None:
                raise ValueError("Invalid configuration : '{}' = '{}'. "
                                 "Keys must be strings and values must "
                                 "not be None.".format(key, value))
            configs[key] = value

        return configs
    
    @property
    def configs_app_name(self):
        """
        Retrieve the application name as defined in the configuration dictionary.

        Returns
        -------
        str or None
            The application name if set; otherwise, None.
        """
        return self.configs.get('spark.app.name', None)

    @property
    def session_app_name(self) -> str:
        """
        Retrieve the application name from the active Spark session's context.

        Returns
        -------
        str
            The application name from the Spark session's SparkContext.

        Raises
        ------
        RuntimeError
            If the Spark session is not initialized or has been stopped.
        """
        if self.session is None:
            raise RuntimeError("Cannot retrieve the application name as the "
                               "Spark session is not initialized or has been stopped.")
        return self.session.sparkContext.appName

    def start(self) -> 'SparkSessionProxy':
        """
        Initialize and start a Spark session using the provided or
        default configurations.

        The session is stored internally and can be accessed through
        the `session` attribute.

        Returns
        -------
        SparkSessionProxy
            The instance itself, allowing for method chaining.
        """
        self.logger.debug("Starting a new Spark session ...")

        # Prepare the Spark session builder
        builder = SparkSession.builder

        # Set each configuration property in the builder
        for key, value in self.configs.items():
            # Use try-except to catch invalid configuartion
            try:
                builder = builder.config(key, value)
            except Exception as e:
                self.logger.error("Failed to set Spark config '{}' = '{}' : {}".format(key, value, e))
                self.logger.print("Traceback:", exc_info=True)

        # Attempt to retrieve or create a Spark session, and assign it
        # to the proxy
        try:
            self.session = builder.getOrCreate()
            self.logger.debug("Spark session '{}' created successfully.".format(self.configs_app_name))

        except Exception as e:
            self.logger.error("Failed to create/retrieve Spark session : {}".format(e))
            self.logger.print("Traceback :", exc_info=True)

        # Return the proxy instance, allowing method chaining
        return self

    def stop(self):
        """
        Stop the Spark session if it has been initialized.

        If an error occurs during shutdown, it is logged with traceback.
        """
        try:
            self.session.stop()
            self.logger.debug("Spark session '{}' stopped successfully.".format(self.session_app_name))

        except Exception as e:
            self.logger.error("Failed to stop the Spark session : {}".format(e))
            self.logger.print("Traceback :", exc_info=True)

    def __enter__(self) -> 'SparkSessionProxy':
        """
        Context manager entry point. Creates the spark session, and
        returns the Proxy instance.
        """
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit point. Handles errors and terminates the
        Spark session.

        Parameters
        ----------
        exc_type : Type[BaseException] | None
            The type of exception raised, if any.

        exc_val : BaseException | None
            The exception instance raised.

        exc_tb : TracebackType | None
            The traceback object associated with the exception.
        """
        # Trace errors on exit.
        if exc_type:
            self.logger.error("Exception occurred : {} - {}".format(exc_type.__name__, exc_val))
            self.logger.print("Traceback :", exc_info=exc_tb)

        # Stop the Spark session if exists.
        self.stop()