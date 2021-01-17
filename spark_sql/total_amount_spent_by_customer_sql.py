from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import types

if __name__ == '__main__':
    spark = SparkSession.builder.appName('TotalAmountSpentByCustomer').master('local[*]').getOrCreate()

    df_schema = types.StructType([
        types.StructField("customer_id", types.IntegerType(), False),
        types.StructField("product_id", types.IntegerType(), False),
        types.StructField("price", types.DoubleType(), False)
    ])

    dataset_df = spark.read.schema(df_schema).csv('dataset/customer-orders.csv').cache()

    customer_price_df = dataset_df.select('customer_id', 'price')

    amount_spent_by_customer_df = customer_price_df.groupBy('customer_id').\
        agg(func.round(func.sum('price'), 2).alias('spending')).orderBy('spending', ascending=False)

    amount_spent_by_customer_df.show()

    spark.stop()
