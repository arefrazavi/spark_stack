from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql import functions as func

if __name__ == '__main__':
    spark = SparkSession.builder.appName('SparkDataframeSql').getOrCreate()

    # Read dataframe from a structured csv (i.e. with header)
    dataset_df = spark.read.csv('dataset/fakefriends-header.csv', header=True, sep=',').cache()

    dataset_df.printSchema()

    # Point to a column in 4 different ways:
    dataset_df.select('age').show(3)
    dataset_df.select(dataset_df['age']).show(3)
    dataset_df.select(dataset_df.userID, dataset_df.name).show(3)
    dataset_df.select(func.col("userID"), dataset_df.name).show(3)

    # Filter by condition and sorting
    dataset_df.filter(dataset_df.friends > 10).orderBy(dataset_df.friends, ascending=False).show(1)

    # Aggregation
    friends_count_by_age = dataset_df.groupBy(dataset_df.age).count().orderBy(dataset_df.age)\

    # show is an action. Call it only for debugging and remove it in production.
    friends_count_by_age.show(3)

    spark.stop()
