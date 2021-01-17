from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pprint import pprint

if __name__ == '__main__':
    spark = SparkSession.builder.appName('WordCount').getOrCreate()

    # Where dataset is unstructured,
    # spark.read puts content of each line of dataset into a row with one column ("value").
    dataset_df = spark.read.text('dataset/book.txt')

    # func.explode (~= flatMap in RDD): Returns a new row for each element in the given array or map.
    # Split string up into word. '\\W+': matches a single word (more smart than space char).
    # Filter out empty and one-character values (words).
    # It's always better to alias on the final func or agg to have a nice column name.
    words_df = dataset_df.select(func.explode(func.split('value', '\\W+')).alias('word')).\
        filter(func.length('word') > 1)

    # Lower case all the words, so all capital-related variants of word could be considered as one word in word count
    words_df = words_df.select(func.lower(words_df.word).alias('word'))

    # Group by words by their frequency (count).
    words_by_count_df = words_df.groupBy('word').agg(func.count('word').alias('frequency')).\
        orderBy('frequency', ascending=False)

    words_by_count_df.show()
    # Show all rows
    # words_by_count_df.show(words_by_count_df.count())

    spark.stop()
