from pyspark.sql import *
from pyspark.sql import SparkSession



class ProxySparkSession(SparkSession):
    """
    A proxy class for SparkSession that enhances session management and logging behavior.

    This class sets the Spark log level to 'ERROR' upon entering the session,
    helping reduce noise during development and debugging. It is intended to be used as
    a context manager, ensuring the SparkSession is properly closed after use.

    This proxy is designed with future extensibility in mind, allowing for the addition of
    other custom behaviors or configuration management as needed.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the ProxySparkSession with the given arguments.
        Defaults the app name to 'ProxySparkSession' if not provided.
        """
        super().__init__(*args, **kwargs)
        self.appName = self.sparkContext.appName or 'ProxySparkSession'

    def __enter__(self):
        """
        Enter the runtime context, set log level, and return the Spark session instance.
        """
        # Set the logging level to error.
        self.sparkContext.setLogLevel('ERROR')
        print("", "> Spark Session created successfully.", "", sep="\n")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark session.
        """
        return super().__exit__(exc_type, exc_val, exc_tb)