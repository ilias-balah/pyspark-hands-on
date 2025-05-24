"""
Practice the `filter` function to remove elements from an RDD : Compute
the minimum and the maximum temperature for each weather station.
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

        # Filter out non-TMIN/TMAX records
        temp_data = data.filter(lambda record: record[1] in ('TMIN', 'TMAX'))



        # Get the minimum/maximum temperature for each station over the entire dataset
        minmax_temps = (
            # Map to (station_id, value) pairs
            # NOTE: The temperature values are in tenths of degrees Celsius, so we divide by 10
            temp_data.map(lambda record: (record[0], record[2] * 0.1))
            # Map to (station_id, (value, value)) pairs
            .mapValues(lambda value: (value, value))
            # Reduce by key to find the minimum and maximum temperature for each station
            # NOTE: As, for each station, the maximum is always greater than the minimum,
            # we can calculate both values in a single pass.
            .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1])))
            # NOTE: Alternatively, we can use combineByKey to achieve the same result
            # .combineByKey(
            #     lambda value: (value, value),  # Create initial tuple (min, max)
            #     lambda acc, value: (min(acc[0], value), max(acc[1], value)),  # Update tuple
            #     lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]))  # Merge tuples
            # )
        )

        # Collect and print the results
        for station_id, (min_temp, max_temp) in minmax_temps.collect():
            print(f"Station ID : {station_id}, Minimum/Maximum Temperature : {min_temp:.2f}°C /{max_temp:.2f}°C")