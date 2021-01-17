from pyspark.sql import SparkSession, functions as func, types

if __name__ == '__main__':
    spark = SparkSession.builder.appName('PopularMovie').getOrCreate()

    df_schema = types.StructType([
        types.StructField('user_id', types.IntegerType(), False),
        types.StructField('movie_id', types.IntegerType(), False),
        types.StructField('rating', types.IntegerType(), False),
        types.StructField('date', types.LongType(), False),
    ])

    # \t: dataset is tap-separated.
    dataset_df = spark.read.schema(df_schema).csv('dataset/spark_ml-100k/u.data', sep='\t').cache()
    movie_rating_df = dataset_df.select('movie_id', 'rating')

    # Measure movie's popularity by average rating
    avg_ratings_by_movie = movie_rating_df.groupBy('movie_id').\
        agg(func.round(func.avg('rating'), 2).alias('avg_rating')).orderBy(func.desc('avg_rating'))

    avg_ratings_by_movie.show()

    spark.stop()
