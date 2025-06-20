from src.internal.spark.session import SparkSession
from src.logs import logging_utils
from src.utils import time_utils


class SparkSessionProxy:
    """
    A proxy class to manage the full lifecycle of a Spark session,
    including configuration, initialization, logging, and graceful shutdown.

    This proxy simplifies session management by merging default and
    user-defined configurations, logging the session creation and
    termination process, and measuring the session runtime.
    """

    # Logger used throughout the lifecycle of the proxy
    logger = logging_utils.get_logger(__qualname__)

    def __init__(self, app_name: str = None, **config_options):
        """
        Initialize the SparkSessionProxy instance with optional custom
        configurations.

        Parameters
        ----------
        app_name : str, optional
            The name to assign to the Spark application. If not provided, 
            a default name is used.

        **config_options : dict
            Additional Spark configuration options to override the defaults.
        """
        self.configs: dict = self._get_configs(app_name = app_name, **config_options)
        self.start_time: time_utils.time_type = time_utils.get_current_time()
        self.end_time: time_utils.time_type = None
        self.session: SparkSession = None

    def _get_configs(self, app_name: str = None, **overrides) -> dict:
        """
        Merge default Spark configurations with user-supplied overrides.

        Parameters
        ----------
        app_name : str, optional
            Optional application name to override the default app name.

        **overrides : dict
            Key-value pairs of Spark configuration settings.

        Returns
        -------
        dict
            A dictionary containing the final set of Spark configurations.
        
        Raises
        ------
        ValueError
            If a configuration key is not a string or its value is None.
        """
        configs = SparkSession.Defaults.spark_configs.copy()

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