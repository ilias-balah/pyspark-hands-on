"""
Utility functions for Spark applications.
"""
from datetime import datetime


class TimeUtils:
    """
    A utility class for handling time-related operations in Spark applications.
    """

    # The default time type used in this utility class.
    time_type = datetime

    @staticmethod
    def get_current_time() -> datetime:
        """
        Get the current time as a datetime object.

        Returns
        -------
        datetime
            The current time as a datetime object.
        """
        return datetime.now()
    
    @staticmethod
    def get_current_timestamp(format: str = None) -> str:
        """
        Get the current timestamp in a formatted string.
        
        Parameters
        ----------
        format : str, optional
            The format string for the timestamp. If None, defaults to '%Y-%m-%d %H:%M:%S.%f'.

        Returns
        -------
        str
            The current timestamp formatted as a string.
        """
        return datetime.now().strftime(format or '%Y-%m-%d %H:%M:%S.%f')
    
    @staticmethod
    def get_delta_seconds(start_time: datetime, end_time: datetime = None) -> int:
        """
        Calculate the difference in seconds between two datetime objects.

        Parameters
        ----------
        start_time : datetime
            The start time.

        end_time : datetime, optional
            The end time. If None, uses the current time.

        Returns
        -------
        int
            The difference in seconds.
        """
        # If end_time is not provided, use the current time.
        if end_time is None:
            end_time = datetime.now()
        
        # Calculate the difference in seconds
        return (end_time - start_time).total_seconds()

