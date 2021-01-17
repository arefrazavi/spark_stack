from pyspark.sql import SparkSession, functions as func, types, DataFrame


def extract_dataset(spark: SparkSession, path: str) -> DataFrame:
    # Read all files in logs directory constantly.
    # Returns a dataframe with "value" column for each row (line)
    logs_df = spark.readStream.text(path)

    # Parse out the common log format to a DataFrame
    content_size_pattern = r'\s(\d+)$'
    time_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) \+\d{4}]'
    status_pattern = r'\s(\d{3})\s'
    general_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'

    return logs_df.select(
        func.regexp_extract('value', host_pattern, 1).alias('host'),
        func.to_timestamp(func.regexp_extract('value', time_pattern, 1), "dd/MMM/yyyy:hh:mm:ss").cast(types.TimestampType()).alias('timestamp'),
        func.regexp_extract('value', general_pattern, 1).alias('method'),
        func.regexp_extract('value', general_pattern, 2).alias('endpoint'),
        func.regexp_extract('value', general_pattern, 3).alias('protocol'),
        func.regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
        func.regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size')
    )


def stream_windowed_top_urls(logs_df: DataFrame, window_duration: str, slide_duration: str) -> None:
    """
    Stream the most requested URLs within window_duration slot, every slide_duration
    based on the timestamp column of rows in dataset.
    :param logs_df:
    :param window_duration:
    :param slide_duration:
    :return: None
    """
    # Aggregate based on a time window and URL (endpoint)
    # Split the window column to see the dates clearly.
    windowed_top_urls_df = logs_df.groupBy(
        func.window(func.col('timestamp'), windowDuration=window_duration, slideDuration=slide_duration),
        func.col('endpoint')).agg(func.count('*').alias('url_count')).\
        orderBy(func.desc('url_count')).\
        select(func.col('window.*'), func.col('endpoint').alias('url'), func.col('url_count'))

    query_stream = windowed_top_urls_df.writeStream.\
        outputMode('complete').\
        queryName('windowed_top_urls').\
        format('console').\
        start()

    query_stream.awaitTermination()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('stream_top_urls').master('local[*]').getOrCreate()

    log_dir_path = 'dataset/logs'
    logs_df = extract_dataset(spark, log_dir_path)

    stream_windowed_top_urls(logs_df, "30 seconds", "10 seconds")

