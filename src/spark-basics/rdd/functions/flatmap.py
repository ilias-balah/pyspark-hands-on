"""
Practice using flatMap on RDDs, by splitting lines into words and
counting occurrences.
"""
import re
from src.internal.proxy_spark_context import ProxySparkContext


if __name__ == "__main__":

    # Use ProxySparkContext as a context manager to ensure proper resource cleanup.
    with ProxySparkContext('local', 'Flatmap on RDD') as spark_context:

        # Read the book data from a text file into an RDD.
        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/text/self-employment-book.txt")

        # NOTE: Spliting lines by space is not the best way to split lines into words, as
        # it may not handle punctuation correctly. A better way to split lines into words
        # is to use a regular expression that matches words, after lowering the case of
        # all characters. This can be done using the re module in Python.
        words = (
            lines
                .map(lambda line: line.lower())
                .flatMap(lambda line: re.compile(r'\w+').findall(line))
        )

        # Count the occurrences of each word in the RDD.
        # NOTE: Alternatively, we can use the countByValue() method to count the
        # occurrences of each word. This method returns a dictionary with the word
        # as the key and the count as the value.
        words_count = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)

        # # Print the first 10 words.
        print(words_count.take(10))