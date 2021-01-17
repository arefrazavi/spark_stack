from pyspark.sql import SparkSession, functions as func, types
import codecs


def extract_movie_names():
    # Extract movies name
    movie_names_dic = {}
    with codecs.open('dataset/spark_ml-100k/u.item', "r", encoding='ISO-8859-1', errors='ignore') as file:
        for line in file:
            fields = line.split('|')
            movie_names_dic[int(fields[0])] = str(fields[1])

    return movie_names_dic


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

    ratings_count_by_movie = dataset_df.groupBy('movie_id'). \
        agg(func.count('movie_id').alias('ratings_count')).orderBy(func.desc('ratings_count'))

    # Bootstrap the value (movie names dictionary) to all executor nodes of the driver program only once
    # to be accessible by them later.
    movie_names_dict_bc = spark.sparkContext.broadcast(extract_movie_names())

    def get_movie_name(movie_id):
        # .value: get the actual object (dictionary) back from the bootstrap object.
        return movie_names_dict_bc.value[movie_id]

    # convert a Python function to a UDF (user defined function) that can be used within Spark SQL.
    get_movie_name_udf = func.udf(get_movie_name)

    ratings_count_by_movie_with_name = ratings_count_by_movie. \
        withColumn('movie_name', get_movie_name_udf(func.col('movie_id')))

    ratings_count_by_movie_with_name.show()

    spark.stop()
