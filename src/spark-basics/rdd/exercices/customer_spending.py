"""
Prictice the RDD API by calculating the total spending of each customer
from a CSV file containing customer orders.
"""
from src.internal.spark.proxies import SparkContextProxy


if __name__ == '__main__':

    spark_configs = dict({
        # Set the application name
        "spark.app.name": "Customer Spending RDD",
        # Configure for a local setup with 2 cores
        "spark.master": "local[2]"
    })

    # Use SparkContextProxy as a context manager to ensure proper resource cleanup.
    with SparkContextProxy(**spark_configs) as spark:

        # Load the CSV file into an RDD.
        # Each line in the file follows this format: customer_id, order_id, amount
        lines = spark.context.textFile("file:///Users/balah/Desktop/Spark/data/csv/customer-orders.csv")

        # Parse each line to extract the customer id and amount spent.
        spendings = (
            lines
                .map(lambda line: line.split(','))
                # Map each line to a tuple (customer_id, amount)
                .map(lambda fields: (fields[0], fields[2]))
                # NOTE: In order to compute how many orders each customer has made,
                # we can map the values into a tuple (1, amount), where 1 is a
                # placeholder for counting the number of orders, and amount is the
                # amount spent by the customer.
                .mapValues(lambda value: (1, float(value)))
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        )

        # Sort the spendings by total spending in descending order.
        sorted_spendings = spendings.sortBy(lambda record: record[1][1], ascending=False)

        # Collect the results and print them.
        for customer_id, (order_count, total_spending) in sorted_spendings.collect():
            # Print the customer id, total spending, and number of orders.
            spark.logger.print(f"Customer {customer_id} has spend ${total_spending:.2f} for {order_count} orders.", as_log=False)