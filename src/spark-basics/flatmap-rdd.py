"""
Practice using flatMap on RDDs, by splitting lines into words and
counting occurrences.
"""
from src.internal.proxy_spark_context import ProxySparkContext


if __name__ == "__main__":

    # Use ProxySparkContext as a context manager to ensure proper resource cleanup.
    with ProxySparkContext('local', 'Flatmap on RDD') as spark_context:

        # Read the book data from a text file into an RDD.
        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/text/self-employment-book.txt")

        # Use flatMap to split each line into words and flatten the result.
        words = lines.flatMap(lambda line: line.split())

        # Count the occurrences of each word in the RDD.
        # NOTE: Alternatively, we can use the countByValue() method to count the
        # occurrences of each word. This method returns a dictionary with the word
        # as the key and the count as the value.
        words_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

        # # Print the first 10 words.
        print(words_count.take(10))