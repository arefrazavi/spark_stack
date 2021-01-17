from pyspark import SparkConf, SparkContext



if __name__ == '__main__':
    spark_config = SparkConf().setMaster('local[*]').setAppName('WordCount')
    sc = SparkContext(conf=spark_config)

    dataset_rdd = sc.textFile('dataset/book.txt')


    # Flatmap convert each element into multiple entries elements from each individual element.
    # There is one-to-many relationship between input and output RDDs.
    words_rdd = dataset_rdd.flatMap(lambda element: element.split())

    # Return a dictionary of (value, count) pairs
    words_count_dict = words_rdd.countByValue()

    for word, count in words_count_dict.items():
        # Convert encoding for characters to ASCII to be properly shown in terminal
        clean_word = word.encode('ascii', 'ignore')
        if clean_word:
            print(clean_word.decode(), ': ', count)
