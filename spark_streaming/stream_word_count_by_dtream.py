from pyspark import SparkContext
from pyspark.streaming import StreamingContext


# Create a local StreamingContext with two working thread and batch interval of 1 second
# It means taking batch interval (i.e. 1s) period of data,
# making individual RDD's out of each period worth of data and then processing those RDD's individually.
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

dataDirectory = 'dataset/book_dir'
# Read data from a directory of files on any file system compatible with the HDFS API.
lines = ssc.textFileStream(dataDirectory)
words = lines.flatMap(lambda line: line.split(" "))

# Count each word in each batch
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# Print the first ten elements of each RDD generated in this DStream to the console
wordCounts.pprint()

# Start the computation
ssc.start()
# Wait for the computation to terminate
ssc.awaitTermination()
