"""
Prictice the RDD API by calculating the total spending of each customer
from a CSV file containing customer orders.
"""
from src.internal.proxy_spark_context import ProxySparkContext


if __name__ == '__main__':

    # Use ProxySparkContext as a context manager to ensure proper resource cleanup.
    with ProxySparkContext('local', 'Customer Spending RDD') as spark_context:

        # Load the CSV file into an RDD.
        # Each line in the file follows this format: customer_id, order_id, amount
        lines = spark_context.textFile("file:///Users/balah/Desktop/Spark/data/csv/customer-orders.csv")

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

        # Print the total spending for each customer.
        for customer_id, (order_count, total_spending) in spendings.collect():
            print(f"Customer {customer_id} has spend ${total_spending:.2f} for {order_count} orders.")