import logging as python_logging
# NOTE: We used the the name python_logging to avoid any probable
# confusion between the default module and this customized one.
from .defaults import ANSI_ESCAPE_CHARACTER, LOG_LEVELS_COLORS


class Logger(python_logging.Logger):
    """
    A class to represent the built-in logger from the python's logging modudle.
    """

    def print(self, msg, *args, **kwargs):
        """
        Place holder for aditional function to log messages with
        level 9.
        """


class Formatter(python_logging.Formatter):
    """
    Custom formatter to add colors to log messages.
    """

    # Define the available log levels
    available_log_levels = ['print', 'debug', 'info', 'warning', 'error', 'critical']

    # ANSI escape sequences for colors
    ansi_escape_sequences = {
        level.upper(): ANSI_ESCAPE_CHARACTER.format(LOG_LEVELS_COLORS[level]['fg'])
        for level
        in available_log_levels
    }

    # Add a reset sequence to the ANSI escape sequences
    ansi_escape_sequences.setdefault('RESET', ANSI_ESCAPE_CHARACTER.format('0'))

    def format(self, record: python_logging.LogRecord) -> str:
        """
        Format the log record with color based on its level.
        """
        reset_color = self.ansi_escape_sequences['RESET']
        # Get the color for the log level, default to reset if not found
        msg_color = self.ansi_escape_sequences.get(record.levelname, reset_color)
        message = super().format(record)
        return f"{msg_color}{message}{reset_color}"


class Main:
    """
    A utility class for logging operations.    
    """

    # A custom print level for logging, lower than DEBUG.
    PRINT_LEVEL: int = 9
    PRINT_LEVEL_NAME: str = 'PRINT'

    # Define a custom method for the print level
    def print_function(self: Logger, message, *args, **kwargs):
        """
        Custom print function that either logs a message at the PRINT
        level or prints it directly.

        Parameters
        ----------
        message : str
            The message to log or print.
        *args :
            Additional arguments passed to the logger.
        **kwargs :
            as_log (bool) :
                If False, the message is printed using the
                built-in `print()`. If True or not provided,
                the message is treated as a log and handled
                through the logger. 

        Returns
        -------
        None
        """
        # If 'as_log' is not set to True, fallback to standard print
        if not kwargs.pop('as_log', True):
            print(message)
            return

        # Log using the custom PRINT level if it's enabled
        if self.isEnabledFor(Main.PRINT_LEVEL):
            self.log(Main.PRINT_LEVEL, message, *args, **kwargs)

    @classmethod
    def get_logger(cls, name: str) -> Logger:
        """
        Get a logger with the specified name.
        """
        # Add the print level to the logger if it doesn't already exist
        if not hasattr(python_logging, cls.PRINT_LEVEL_NAME):
            python_logging.addLevelName(cls.PRINT_LEVEL, cls.PRINT_LEVEL_NAME)
            python_logging.Logger.print = cls.print_function
            setattr(python_logging, cls.PRINT_LEVEL_NAME, cls.PRINT_LEVEL)

        # Create a logger with the specified name
        logger = python_logging.getLogger(name)

        # Always set the logger's level to PRINT
        logger.setLevel(python_logging.PRINT)

        # If the logger has no handlers, add a default handler with colored output.
        if not logger.hasHandlers():
            handler = python_logging.StreamHandler()
            formatter = Formatter('[%(asctime)s] [%(levelname)-8s] %(message)s', '%Y-%m-%d %H:%M:%S')
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        # Return the configured logger
        return logger
        
    @classmethod
    def test_colored_formatter(cls):
        """
        Test the Formatter to ensure it formats messages correctly.
        """
        logger = cls.get_logger("test_logger")
        # Log messages at different levels
        logger.print("This is a log message.")
        logger.debug("This is a debug message.")
        logger.info("This is an info message.")
        logger.warning("This is a warning message.")
        logger.error("This is an error message.")
        logger.critical("This is a critical message.")
