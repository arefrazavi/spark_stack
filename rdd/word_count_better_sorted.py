from pyspark import SparkConf, SparkContext
import re


def convert_to_words(line: str):
    # W+: break the line based on words by striping out punctuation and other things.
    # re.UNICODE: line may have unicode chars
    # split it up based on words detected by regular expression.
    return re.compile(r'\W+', re.UNICODE).split(line.lower())


if __name__ == '__main__':
    spark_config = SparkConf().setMaster('local[*]').setAppName('WordCount')
    sc = SparkContext(conf=spark_config)

    dataset_rdd = sc.textFile('dataset/book.txt')

    # Flatmap convert each element into multiple entries elements from each individual element.
    # There is one-to-many relationship between input and output RDDs.
    words_rdd = dataset_rdd.flatMap(convert_to_words)

    # Remove words with less than 2 chars.
    words_rdd = words_rdd.filter(lambda x: len(x) > 1)

    # Create a key-value rdd in which each element is tuple of (word, count).
    # This way is more scalable way of counting by value compared to countByValue function.
    # because there result is an RDD.
    words_count_rdd = words_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

    # Sort the elements by count
    # Flip the pair from (word, count) to (count, word) and then sort by new key (count).
    words_count_rdd = words_count_rdd.map(lambda x: (x[1], x[0])).sortByKey()

    for count, word in words_count_rdd.collect():
        # Convert encoding for characters to ASCII to be properly shown in terminal
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word.decode(), ': ', count)
