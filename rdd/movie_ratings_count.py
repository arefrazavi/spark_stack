from pyspark import SparkConf, SparkContext
from collections import OrderedDict
from pprint import pprint


if __name__ == '__main__':

    # Set master node on which the context will run.
    conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")

    # Create an object of Spark driver program.
    sc = SparkContext(conf=conf)

    # Extract the data in the given file and convert into an RDD.
    # Each line (row) in data corresponds to a value in the RDD.
    # Each value is a string that represents a line of text.
    # Each line contains: user ID, movie ID, rating value, timestamp.
    lines = sc.textFile("dataset/spark_ml-100k/u.data")

    # Transform RDD into a new single-value RDD containing the rating values
    # by extracting rating value from each line.
    # Map transforms each element of an RDD into a new element.
    # In Map, there is a one-to-one relationship between input and output RDDs.
    ratings = lines.map(lambda x: x.split()[2])

    # Action on RDD: Group by RDD (ratings) by counting each value.
    # ratings_count is a dictionary of (value: count) pairs.
    ratings_count = ratings.countByValue()

    # Sort ratings count by count (value) in ascending order.
    ratings_count = OrderedDict(sorted(ratings_count.items(), key=lambda item: item[1]))
    for key, value in ratings_count.items():
        print("%s %i" % (key, value))
