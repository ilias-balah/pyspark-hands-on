from src.internal.spark.context import SparkContext
from src.internal.spark.configs import SparkConf
from src.logs import logging_utils
from src.utils import time_utils


class SparkContextProxy:
    """
    A context manager-based proxy for managing the lifecycle of a
    SparkContext.

    This class simplifies the creation, configuration, and shutdown
    of a SparkContext, while adding enhanced logging, structured
    configuration handling, and support for measuring execution time.

    Ideal for use in scripts and applications where a reliable, configurable,
    and debuggable SparkContext setup is needed.
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
        self.context: SparkContext = None

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
        configs = SparkConf.Defaults.context.copy()

        # Set application name if provided
        configs["spark.app.name"] = app_name

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
        Return the Spark application name as defined in the configuration dictionary.

        Returns
        -------
        str or None
            The configured application name, or None if not set.
        """
        return self.configs.get('spark.app.name', None)

    @property
    def context_app_name(self) -> str:
        """
        Retrieve the actual application name from the SparkContext.

        Returns
        -------
        str
            The name reported by the active SparkContext.

        Raises
        ------
        RuntimeError
            If the SparkContext has not been initialized.
        """
        if self.context is None:
            raise RuntimeError("Cannot retrieve the application name as the "
                               "Spark context is not initialized or has been stopped.")
        return self.context.appName

    def start(self) -> 'SparkContextProxy':
        """
        Create or retrieve a SparkContext using the defined configurations.

        This method configures the SparkConf object and assigns the resulting
        context to the proxy.

        Returns
        -------
        SparkContextProxy
            The proxy instance itself, supporting method chaining.
        """
        self.logger.debug("Starting a new Spark context ...")

        # Prepare the Spark configuration
        conf = SparkConf()

        # Set each configuration property in the builder
        for key, value in self.configs.items():
            # Use try-except to catch invalid configuartion
            try:
                conf.set(key, value)
            except Exception as e:
                self.logger.error("Failed to set Spark config '{}' = '{}' : {}".format(key, value, e))
                self.logger.print("Traceback:", exc_info=True)

        # Attempt to retrieve or create a Spark context, and assign it
        # to the proxy
        try:
            self.context = SparkContext.getOrCreate(conf=conf)
            self.logger.debug("Spark context '{}' created successfully.".format(self.configs_app_name))

        except Exception as e:
            self.logger.error("Failed to create/retrieve Spark context : {}".format(e))
            self.logger.print("Traceback :", exc_info=True)

        # Return the proxy instance, allowing method chaining
        return self

    def stop(self):
        """
        Stop the Spark context if it has been initialized.

        If an error occurs during shutdown, it is logged with traceback.
        """
        try:
            self.context.stop()
            self.logger.debug("Spark context '{}' stopped successfully.".format(self.context_app_name))

        except Exception as e:
            self.logger.error("Failed to stop the Spark context : {}".format(e))
            self.logger.print("Traceback :", exc_info=True)
    
    def __enter__(self) -> 'SparkContextProxy':
        """
        Enter the context manager and start the SparkContext.

        Returns
        -------
        SparkContextProxy
            The proxy instance with an active SparkContext.
        """
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Exit the context manager, handle any raised exceptions, and stop
        the SparkContext.

        Parameters
        ----------
        exc_type : type or None
            Type of the exception, if raised.

        exc_val : Exception or None
            The exception instance, if any.

        exc_tb : TracebackType or None
            The traceback object, if any exception occurred.
        """
        # Trace errors on exit.
        if exc_type:
            self.logger.error("Exception occurred : {} - {}".format(exc_type.__name__, exc_val))
            self.logger.print("Traceback :", exc_info=exc_tb)

        # Stop the Spark context if exists.
        self.stop()