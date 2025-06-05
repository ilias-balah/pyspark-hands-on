import logging


# NOTE: ANSI escape codes for colors. For more information,
# see https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797.
ANSI_COLORS = {
    "black":   { 'fg': "30", 'bg': "40" },
    "red":     { 'fg': "31", 'bg': "41" },
    "green":   { 'fg': "32", 'bg': "42" },
    "yellow":  { 'fg': "33", 'bg': "43" },
    "blue":    { 'fg': "34", 'bg': "44" },
    "magenta": { 'fg': "35", 'bg': "45" },
    "cyan":    { 'fg': "36", 'bg': "46" },
    "white":   { 'fg': "37", 'bg': "47" },
}

# Define a mapping of log levels to their corresponding ANSI colors
LOG_LEVELS_COLORS: dict[str, dict] = {
    'debug':    ANSI_COLORS["cyan"],
    'info':     ANSI_COLORS["green"],
    'warning':  ANSI_COLORS["yellow"],
    'error':    ANSI_COLORS["red"],
    'critical': ANSI_COLORS["magenta"],
}

# ANSI escape character for formatting
ANSI_ESCAPE_CHARACTER = '\033[{}m'


class LoggerUtils:
    """
    A utility class for logging operations.    
    """

    class ColoredFormatter(logging.Formatter):
        """
        Custom formatter to add colors to log messages.
        """

        # Define the available log levels
        available_log_levels = ['debug', 'info', 'warning', 'error', 'critical']

        # ANSI escape sequences for colors
        ansi_escape_sequences = {
            level.upper(): ANSI_ESCAPE_CHARACTER.format(LOG_LEVELS_COLORS[level]['fg'])
            for level
            in available_log_levels
        }

        # Add a reset sequence to the ANSI escape sequences
        ansi_escape_sequences.setdefault('RESET', ANSI_ESCAPE_CHARACTER.format('0'))

        def format(self, record: logging.LogRecord) -> str:
            """
            Format the log record with color based on its level.
            """
            reset_color = self.ansi_escape_sequences['RESET']
            # Get the color for the log level, default to reset if not found
            msg_color = self.ansi_escape_sequences.get(record.levelname, reset_color)
            message = super().format(record)
            return f"{msg_color}{message}{reset_color}"

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a logger with the specified name.
        """
        logger = logging.getLogger(name)

        # If the logger has no handlers, add a default handler with colored output.
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = cls.ColoredFormatter('[%(asctime)s] [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)

        # Set the logger to the desired level
        logger.setLevel(logging.DEBUG)

        # Return the configured logger
        return logger
        
    @classmethod
    def test_colored_formatter(cls):
        """
        Test the ColoredFormatter to ensure it formats messages correctly.
        """
        logger = cls.get_logger("test_logger")
        # Log messages at different levels
        logger.log(0, "This is a debug message.")
        logger.debug("This is a debug message.")
        logger.info("This is an info message.")
        logger.warning("This is a warning message.")
        logger.error("This is an error message.")
        logger.critical("This is a critical message.")



if __name__ == "__main__":
    
    # Run the test for the colored formatter
    print("Testing ColoredFormatter ...")
    LoggerUtils.test_colored_formatter()
