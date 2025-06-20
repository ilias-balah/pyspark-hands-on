"""
Practice key-value in RDD : Compute the average number of friends by age.
"""
from src.internal.spark.proxies import SparkContextProxy


def parse_line(line: str):
    """
    Parses a line of CSV data to extract age and number of friends.

    Parameters
    ----------
    line : str
        A single line from the CSV file in the format : id, name, age, num_friends

    Returns
    -------
    tuple[int, int]
        A tuple containing the age and number of friends.
    """
    # Unpack the fields
    _, _, age, num_friends = line.split(',')
    # Convert age and num_friends to integers
    age = int(age)
    num_friends = int(num_friends)
    # Return a tuple of (age, num_friends)
    return age, num_friends


if __name__ == "__main__":

    spark_configs = dict({
        # Set the application name
        "spark.app.name": "Key-Value RDD",
        # Configure for a local setup with 2 cores
        "spark.master": "local[2]"
    })

    # Use SparkContextProxy as a context manager to ensure proper resource cleanup.
    with SparkContextProxy(**spark_configs) as spark:

        # Load the CSV file into an RDD.
        # NOTE: Each line in the file follows this format : id, name, age, num_friends
        lines = spark.context.textFile("file:///Users/balah/Desktop/Spark/data/csv/fake-friends.csv")

        # Convert each line into (age, num_friends) pairs.
        data = lines.map(parse_line)

        # Compute the average number of friends by age.
        avg_friends_by_age = (
            # Map values to (num_friends, 1) for counting
            data.mapValues(lambda num_friends: (num_friends, 1))
                # Reduce by key to sum total friends and count
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                # Map values to compute average
                .mapValues(lambda x: x[0] / x[1])
        )

        # Collect and print results, sorted by age.
        for age, avg_friends in sorted(avg_friends_by_age.collect()):
            spark.logger.print(f"Age : {age}, Average number of friends : {avg_friends:.2f}", as_log=False)
