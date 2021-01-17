from pyspark import SparkConf, SparkContext
import re

def normalize_text(line: str):
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
    words_rdd = dataset_rdd.flatMap(normalize_text)

    # Return a dictionary of (value, count) pairs
    words_count_dict = words_rdd.countByValue()

    for word, count in words_count_dict.items():
        # Convert encoding for characters to ASCII to be properly shown in terminal
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word.decode(), ': ', count)
