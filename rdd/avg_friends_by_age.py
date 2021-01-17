from pyspark import SparkContext, SparkConf
from pprint import pprint
import sys

def parse_line(line: str) -> tuple:
    """
    Extract the age and friends count pairt from the given line.
    # Parse each line
    :param line:
    :return:
    """
    # Line: id, name, age, friends_count
    row = line.split(',')
    age = int(row[2])
    friends_count = int(row[3])

    return age, friends_count


if __name__ == '__main__':
    spark_conf = SparkConf().setMaster('local[*]').setAppName('AverageNumberOfFriendsByAge')
    sc = SparkContext(conf=spark_conf)

    lines_rdd = sc.textFile('dataset/fakefriends.csv')

    # Transform lines_rdd to a key-value rdd containing (age: friend_count) tuples.
    # > In RDD, when each element is a tuple of two items, it is considered as a (key: value) pair.
    age_friends_count_rdd = lines_rdd.map(parse_line)

    # Convert value of each key to (value, 1) tuple.
    # 1 will be used to count number of occurrences of an age later on.
    age_friends_count_rdd = age_friends_count_rdd.mapValues(lambda value: (value, 1))

    # Merge the values for each key using a associative reduce function.
    # Similar to GroupBy but custom reduce action on values can be done.
    # For each two observed values (tuple_1 and tuple_2) of each age (key):
    # 1) Add up their first elements (friends_count) to calculate sum of the number of friends for the age (key)
    # 2) Add up their second elements (1) to calculate the number of records having that age (key)
    friends_count_group_by_age = age_friends_count_rdd.reduceByKey(
        lambda tuple_1, tuple_2: (tuple_1[0] + tuple_2[0], tuple_1[1] + tuple_2[1]))

    avg_friends_count_by_age = friends_count_group_by_age.mapValues(lambda value: round(value[0] / value[1], 2))

    for age_avg_friend_count in avg_friends_count_by_age.collect():
        print(age_avg_friend_count)
