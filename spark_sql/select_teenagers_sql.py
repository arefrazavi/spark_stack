from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, Row


def convert_to_sql_row(line):
    row = line.split(',')

    return Row(id=int(row[0]), name=str(row[1]), age=int(row[2]), friends_count=int(row[3]))


if __name__ == '__main__':

    sc_conf = SparkConf().setMaster('local[*]').setAppName('SelectTeenagers')
    sc = SparkContext(conf=sc_conf)

    # Since the dataset file doesn't have a header of columns,
    # we have to first create an rdd from file and them convert that to a dataframe.
    dataset_rdd = sc.textFile('dataset/fakefriends.csv')

    dataset_rows = dataset_rdd.map(convert_to_sql_row)

    # Create a SparkSQL session.
    spark = SparkSession.builder.appName('SelectTeenagers').getOrCreate()

    # Create a dataframe by inferring schema from row objects.
    dataset_df = spark.createDataFrame(dataset_rows).cache()

    # Three ways to select teenagers by filtering rows by age column

    # 1) Use filter function and conditions on dataframe object
    #teenagers_df = dataset_df.filter(dataset_df.age >= 13).filter(dataset_df.age <= 19).\
    #    orderBy(dataset_df.friends_count, ascending=False)

    # 2) Use filter function and sql-like conditions
    #teenagers_df = dataset_df.filter('age >= 13 AND age <= 19').orderBy('friends_count', ascending=False)

    # 3) Perform SQL query on the dataframe
    # 3.1) create a view of dataframe.
    dataset_df.createOrReplaceTempView('friends')
    # 3.2) Run SQL query on the view
    teenagers_df = spark.sql('SELECT * FROM friends WHERE age >= 12 AND age <= 19 ORDER BY friends_count DESC')

    # Print row objects
    for row in teenagers_df.collect():
        print(row)

    # Print n rows of dataframe
    teenagers_df.show(n=20)

    spark.stop()
