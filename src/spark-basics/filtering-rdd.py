"""
Practice the `filter` function to remove elements from an RDD : Compute
the minimum temperature for each weather station.
"""
from src.internal.proxy_spark_context import ProxySparkContext


def parse_line(line: str):
    """
    Parses a line from the weather data CSV and extracts key fields.

    Extracts the following from a CSV line:
    - station_id : Unique identifier of the weather station
    - element : Type of observation (e.g., TMAX, TMIN, PRCP)
    - value : Recorded measurement (e.g., temperature in tenths of °C)

    Parameters
    ----------
    line : str
        A single comma-separated line from the weather data CSV.

    Returns
    -------
    tuple[str, str, float]
        A tuple containing the station ID, element type, and value.

    """
    # Unpack the fields
    station_id, _, element, value, _, _, _, _ = line.split(',')
    # Convert value to float
    value = float(value)
    # Return a tuple of (station_id, element, value)
    return station_id, element, value
    

if __name__ == "__main__":

    # Use ProxySparkContext as a context manager to ensure proper resource cleanup.
    with ProxySparkContext('local', 'Key-Value RDD') as spark_context:

        # Load the CSV file into an RDD.
        # NOTE: Each line in the file follows this format : station_id, date, element, value,
        # m_flag, q_flag, s_flag, obs_time
        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/csv/1800.csv")

        # Extract temperature records and convert each line into a tuple
        # (station_id, element, value)
        data = lines.map(parse_line)

        # Filter out non-TMIN records
        tmin_data = data.filter(lambda record: record[1] == 'TMIN')

        # Get the minimum temperature for each station over the entire dataset
        min_temps = (
            # Map to (station_id, value) pairs
            tmin_data.map(lambda record: (record[0], record[2]))
            # Reduce by key to find the minimum temperature for each station
            .reduceByKey(lambda a, b: min(a, b))
        )

        # Collect and print the results
        for station_id, min_temp in min_temps.collect():
            print(f"Station ID : {station_id}, Minimum Temperature : {min_temp:.2f}")