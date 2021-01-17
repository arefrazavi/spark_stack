from pyspark.sql import SparkSession, DataFrame, functions as func, types
import sys

# python3 dataframe/similar_movies_1m_emr.py 'Star Wars (1977)'


def extract_datasets(spark: SparkSession) -> (DataFrame, DataFrame):
    movie_names_schema = types.StructType([
        types.StructField('id', types.IntegerType(), False),
        types.StructField('name', types.StringType(), False),
    ])

    movie_names_df = spark.read.schema(movie_names_schema).csv('dataset/spark_ml-100k/u.item', sep='|')

    movie_ratings_schema = types.StructType([
        types.StructField('user_id', types.IntegerType(), False),
        types.StructField('movie_id', types.IntegerType(), False),
        types.StructField('rating', types.IntegerType(), False),
        types.StructField('date', types.IntegerType(), False),
    ])

    movie_ratings_df = spark.read.schema(movie_ratings_schema).csv('dataset/spark_ml-100k/u.data', sep='\t')

    return movie_names_df, movie_ratings_df


def cal_movies_similarities(movie_ratings_df: DataFrame):
    """
    Calculate similarity of movie pairs based on their co-occurrence
    and the cosine similarity of their ratings when watched by same person.
    :param movie_ratings_df:
    :return:
    """
    # Find all pair of different movies watched by the same person.
    # func.col('mr1.movie_id') < func.col('mr2.movie_id') to avoid duplication.
    # Parenthesis is mandatory for combined condition (e.g. &, |)
    ratings_pairs_df = movie_ratings_df.alias('mr1'). \
        join(movie_ratings_df.alias('mr2'),
             (func.col('mr1.user_id') == func.col('mr2.user_id')) & (
                         func.col('mr1.movie_id') < func.col('mr2.movie_id'))). \
        select(
        func.col('mr1.movie_id').alias('movie_id_1'),
        func.col('mr2.movie_id').alias('movie_id_2'),
        func.col('mr1.rating').alias('rating_1'),
        func.col('mr2.rating').alias('rating_2')
    )

    # Calculate dot product (numerator) and magnitude (denominator) of cosine similarity equation.
    # Each movie is considered a vector of its ratings.
    ratings_pairs_df = ratings_pairs_df.groupBy('movie_id_1', 'movie_id_2'). \
        agg(func.sum(func.col('rating_1') * func.col('rating_2')).alias('sim_dot_product'),
            (func.sqrt(func.sum(func.pow(func.col('rating_1'), 2))) * func.sqrt(
                func.sum(func.pow(func.col('rating_2'), 2)))).alias('sim_magnitude'),
            func.count(func.col('movie_id_1')).alias('co_occurrence_count')
            )

    # Calculate cosine similarity as a new column:
    # (doc product of two movie ratings / doc product of two magnitude ratings)
    movies_similarities_df = ratings_pairs_df. \
        withColumn('similarity_score',
                   func.when(func.col('sim_magnitude') != 0,
                             func.col('sim_dot_product') / func.col('sim_magnitude')).otherwise(0)
                   ).select('movie_id_1', 'movie_id_2', 'similarity_score', 'co_occurrence_count')

    return movies_similarities_df


def find_similar_movies_by_movie_id(
        movie_id: int, movie_ratings_df: DataFrame, movie_names_df: DataFrame,
        similar_movies_count: int = 10, score_threshold: float = 0.97,
        co_occurrence_threshold: int = 50) -> DataFrame:
    # Remove useless columns to shrink dataframe before join.
    movie_ratings_df = movie_ratings_df.select('user_id', 'movie_id', 'rating')

    movies_similarities_df = cal_movies_similarities(movie_ratings_df).cache()

    similar_movies_df = movies_similarities_df. \
        filter(((func.col('movie_id_1') == movie_id) | (func.col('movie_id_2') == movie_id))
               & (func.col('co_occurrence_count') > co_occurrence_threshold)
               & (func.col('similarity_score') > score_threshold)). \
        orderBy(func.desc('similarity_score'), func.desc('co_occurrence_count')). \
        limit(similar_movies_count). \
        select(
        func.when(func.col('movie_id_1') == movie_id, func.col('movie_id_2')).otherwise(func.col('movie_id_1')).alias(
            'movie_id'),
        func.col('similarity_score'),
        func.col('co_occurrence_count')
        )

    similar_movies_names_df = similar_movies_df.alias('sm'). \
        join(movie_names_df.alias('mn'), func.col('mn.id') == func.col('sm.movie_id')). \
        select(func.col('sm.movie_id').alias('movie_id'),
               func.col('mn.name').alias('movie_name'),
               func.col('sm.similarity_score').alias('similarity_score'),
               func.col('sm.co_occurrence_count').alias('co_occurrence_count')). \
        orderBy(func.desc('similarity_score'), func.desc('co_occurrence_count'))

    return similar_movies_names_df


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.exit('Enter Movie name as command-line argument.')

    movie_name = str(sys.argv[1].strip())

    spark = SparkSession.builder.appName('SimilarMovies').master('local[*]').getOrCreate()

    movie_names_df, movie_ratings_df = extract_datasets(spark)

    # Debugging
    movie_ratings_df.show()

    search_movie_df = movie_names_df.filter(func.col('name') == movie_name)
    if not search_movie_df.take(1):
        print('Movie not found. :(')
    else:
        movie_id = search_movie_df.first()['id']
        similar_movies_names_df = find_similar_movies_by_movie_id(movie_id, movie_ratings_df, movie_names_df)
        print('Searched movie id:', movie_id)
        similar_movies_names_df.show()

    spark.stop()
