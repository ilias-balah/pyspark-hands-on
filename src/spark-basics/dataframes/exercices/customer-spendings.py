"""
Exercice to calculate the total spending and the number of orders per customer.
"""
from pyspark.sql import types as st
from src.internal.spark.proxies import SparkSessionProxy
from src.utils.spark_functions import apply_spark_function_with_alias, spark_funcs


if __name__ == '__main__':

    with SparkSessionProxy("Customers' total spendings exercice") as spark:

        # Customize a schema fo the customer-orders dataframe
        schema = st.StructType([
            st.StructField('customer_id', st.IntegerType(), False),
            st.StructField('order_id', st.IntegerType(), False),
            st.StructField('amount', st.FloatType(), False)
        ])

        # Read the oredrs dataframe
        dataframe = spark.session.read.schema(schema).csv("file:///Users/balah/Desktop/Spark/data/csv/customer-orders.csv")

        # ---------------------------------------------------------------------
        # Calculate the number of orders and the apent amount per customer
        # using a SQL query.
        # ---------------------------------------------------------------------
        dataframe.createTempView("customer_orders_view")
        spark.session.sql("""
            SELECT
                customer_id,
                COUNT(*) AS number_of_orders,
                ROUND(SUM(amount), 2) AS total_amount
            FROM customer_orders_view
            GROUP BY customer_id
            ORDER BY COUNT(*) DESC
        """)\
            .show(5)
        
        # ---------------------------------------------------------------------
        # Calculate the number of orders and the apent amount per customer
        # using a spark-functions.
        # ---------------------------------------------------------------------
        dataframe \
            .groupby('customer_id') \
            .agg({'customer_id': 'count', 'amount': 'sum'}) \
            .withColumnsRenamed({'count(customer_id)': 'number_of_orders', 'sum(amount)': 'total_amount'}) \
            .withColumn('total_amount', spark_funcs.round('total_amount', 2)) \
            .orderBy('number_of_orders', ascending=False) \
            .select('customer_id', 'number_of_orders', 'total_amount') \
            .show(5)