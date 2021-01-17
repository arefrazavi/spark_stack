import sys
from pyspark.sql import SparkSession, types, DataFrame, functions as func
from pyspark.ml.recommendation import ALS
from typing import Tuple, Union
from pprint import pprint


def extract_datasets(spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    movie_names_schema = types.StructType([
        types.StructField('id', types.IntegerType(), False),
        types.StructField('name', types.StringType(), False),
    ])

    # Change dataset/spark_ml-100k/u.item to movies.dat and sep='::'
    movie_names_df = spark.read.schema(movie_names_schema).csv('dataset/spark_ml-100k/u.item', sep='|')

    # We don't need timestamps col.
    ratings_schema = types.StructType([
        types.StructField('user_id', types.IntegerType(), False),
        types.StructField('movie_id', types.IntegerType(), False),
        types.StructField('rating', types.IntegerType(), False)
    ])

    ratings_df = spark.read.schema(ratings_schema).csv('dataset/spark_ml-100k/u.data', sep='\t').cache()

    return movie_names_df, ratings_df,


def train_als_model_by_ratings(ratings_df: DataFrame) -> ALS:
    """
    The system generates recommendations using only information about rating profiles for different users or items.
    spark.spark_ml currently supports model-based collaborative filtering, in which users and products are described by a small set of latent factors that can be used to predict missing entries.
    spark.spark_ml uses the alternating least squares (ALS) algorithm to learn these latent factors.
    :return:
    """

    # maxIter: maximum number of iterations to run
    # regParam: specifies the regularization parameter in ALS (defaults to 1.0).
    # These hyper parameters can only be tuned by trial and error.
    # We can use train/test framework to evaluate permutations of the parameters.
    als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol('user_id').setItemCol('movie_id').setRatingCol('rating')

    # Generate a trained ALS model based on ratings data
    als_model = als.fit(ratings_df)

    return als_model


def find_movie_recommendations_by_user_id(user_id: int, ratings_df: DataFrame, movie_names_df: DataFrame,
                                          spark: SparkSession) -> Union[DataFrame, bool]:
    """
    Classification Algorithm:
    Use Collaborative filtering to find similar movies to recommend new movies based on their similarity
    Collaborative filtering is based on the assumption that people who agreed in the past will agree in the future,
    and that they will like similar kinds of items as they liked in the past.
    https://spark.apache.org/docs/3.0.1/ml-collaborative-filtering.html
    :param search_movie_df:
    :param ratings_df:
    :param movie_names_df:
    :return:
    """
    als_model = train_als_model_by_ratings(ratings_df)

    # Examples of find recommendations for top numItems movies (items) or users
    # // Generate top 10 movie recommendations for each user
    # userRecs = model.recommendForAllUsers(numItems)
    # // Generate top 10 user recommendations for each movie
    # movieRecs = model.recommendForAllItems(numItems)
    # // Generate top 10 movie recommendations for a specified set of users (in dataframe format).
    # userSubsetRecs = model.recommendForUserSubset(users, numItems)
    # // Generate top 10 user recommendations for a specified set of movies (in dataframe format).
    # movieSubSetRecs = model.recommendForItemSubset(movies, numItems)

    numItems = 10
    # The recommendForUserSubset input must be a dataframe which only include one column called "user_id".
    user_schema = types.StructType([types.StructField('user_id', types.IntegerType())])
    users_df = spark.createDataFrame([[user_id, ]], user_schema)
    # movie_recs_df consists of a dataframe for Rows with two columns "user_id" and "recommendations".
    # Each row hold the the recommendations for each user in users_df subset (users_df has only one item here.)
    # "recommendations" are stored as an array of (movie_id, rating) Rows.
    # "rating" is actually predicted rating that the given give to each movie from 10.
    movies_recs_df = als_model.recommendForUserSubset(users_df, numItems)

    if not movies_recs_df.take(1):
        return False

    movie_rating_df = spark.createDataFrame(movies_recs_df.collect()[0].recommendations)

    movie_rating_df = movie_rating_df.alias('mr').join(movie_names_df.alias('mn'), func.col('mr.movie_id') == func.col('mn.id')).\
        select(func.col('mr.movie_id').alias('id'), func.col('mn.name').alias('name'), func.col('mr.rating').alias('predicted_rating'))

    return movie_rating_df


if __name__ == '__main__':
    if len(sys.argv) <= 1:
        sys.exit('Enter Movie name as command-line argument.')

    user_id = int(sys.argv[1].strip())

    spark = SparkSession.builder.appName('movie_recommendation_als').master('local[*]').getOrCreate()
    (movie_names_df, ratings_df) = extract_datasets(spark)

    recommended_movies_df = find_movie_recommendations_by_user_id(user_id, ratings_df, movie_names_df, spark)

    if not recommended_movies_df:
        print('No movie recommendation found for the user with ID' + str(user_id))
    else:
        print('Top 10 movie recommendations for user with ID ' + str(user_id))
        # Results change everytime and is not accurate. ALS is not that good!
        recommended_movies_df.show()

    spark.stop()
