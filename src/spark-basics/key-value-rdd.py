"""
Count the average number of friends by age.
"""
from src.internal.proxy_spark_context import ProxySparkContext

def parse_age_friends(line: str):
    """
    Parse the line to extract age and number of friends.
    """
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)


if __name__ == "__main__":

    # Use the spark context in a context manager to ensure the closure.
    with ProxySparkContext('local', 'Key-Value RDD') as spark_context:

        # Read the CSV file into an RDD.
        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/csv/fake_friends.csv")
        
        # Read the age and number of friends and transform them into a tuple (age, num_friends).
        ages_friends = lines.map(parse_age_friends)

        # Get the average number of friends by age
        avg_friends_by_age = (
            ages_friends.mapValues(lambda num_friends: (num_friends, 1))       # Transform values to (num_friends, 1)
                        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Reduce by key to sum the number of friends and count
                        .mapValues(lambda x: x[0] / x[1])                      # Calculate the average number of friends by age
        )

        # Collect the results and print them
        for age, avg_friends in sorted(avg_friends_by_age.collect()):
            print(f"Age : {age}, Average number of friends : {avg_friends:.2f}")