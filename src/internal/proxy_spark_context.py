from pyspark import SparkContext


class ProxySparkContext(SparkContext):
    """
    A proxy class for SparkContext that enhances context management and logging behavior.

    Currently, this class sets the Spark log level to 'ERROR' upon entering the context,
    helping reduce noise during development and debugging. It is intended to be used as
    a context manager, ensuring the SparkContext is properly closed after use.

    This proxy is designed with future extensibility in mind, allowing for the addition of
    other custom behaviors or configuration management as needed.
    """
    
    def __init__(self, *args, **kwargs):
        """
        Initialize the ProxySparkContext with the given arguments.
        Defaults the app name to 'ProxySparkContext' if not provided.
        """
        super().__init__(*args, **kwargs)
        self.appName = self.appName or 'ProxySparkContext'

    def __enter__(self):
        """
        Enter the runtime context, set log level, and return the Spark context instance.
        """
        # Set the logging level to error.
        self.setLogLevel('ERROR')
        print("", "> Spark Context {} created successfully.".format(self.appName), "", sep="\n")
        return self
    
    def __exit__(self, type, value, trace):
        """
        Exit the runtime context, ensuring proper cleanup of the Spark context.
        """
        return super().__exit__(type, value, trace)