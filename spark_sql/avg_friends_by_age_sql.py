from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pprint import pprint
from pyspark.sql import functions as func
import sys

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Avg Friends By Aqe').getOrCreate()

    # Create a schema for dataframe.
    df_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), False),
        StructField("friends_count", IntegerType(), True)
    ])

    dataset_df = spark.read.schema(df_schema).csv('dataset/fakefriends-header.csv', header=True, sep=',', inferSchema=False).cache()

    # Select only required columns and discard the useless ones as soon as possible to prevent wasting cluster resource.
    age_friends_count_df = dataset_df.select('age', 'friends_count')

    # Simple average
    # avg_friends_count_by_age = age_friends_count_df.groupBy('age').avg('friends_count')

    # Round average and rename the aggregation column.
    # Needs agg function to modify average.
    avg_friends_count_by_age = age_friends_count_df.groupBy('age').\
        agg(func.round(func.avg('friends_count'), 2).alias('avg_friends_count')).\
        orderBy('avg_friends_count')

    avg_friends_count_by_age.show()

    spark.stop()
