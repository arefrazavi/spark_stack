from pyspark.sql import SparkSession, functions as func, DataFrame, types


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


def stream_response_status_count(logs_df: DataFrame, output_path: str) -> None:
    status_count_df = logs_df.groupBy('status').agg(func.count('*').alias('status_count'))

    # Start the streaming query
    # Output the result on console.
    # outputMode: specifies how data in dataframe is written to the output (streaming sink).
    query = status_count_df.writeStream.format('console').\
        outputMode("complete").\
        queryName('status_count').start()

    # Output the result on a CSV file.
    # status_count_df = logs_df.\
    #     withWatermark("timestamp", "1 minutes").\
    #     groupBy('status', "timestamp").\
    #     agg(func.count('*').alias('status_count'), func.max(func.col('timestamp')).alias('timestamp'))
    #
    # query = status_count_df.writeStream.\
    #     outputMode("Append").\
    #     option('path', output_path).option("checkpointLocation", 'spark_streaming/checkpoints').\
    #     format('csv').\
    #     queryName('status_count').start()

    # Run forever until terminated.
    # Add files to logs directory to see a new output.
    # Ctrl + C to terminate query.
    query.awaitTermination()


if __name__ == '__main__':
    log_dir_path = 'dataset/logs'
    spark = SparkSession.builder.appName('stream_logs').master('local[*]').getOrCreate()
    logs_df = extract_dataset(spark, log_dir_path)

    output_path = 'spark_streaming/streaming_output'
    stream_response_status_count(logs_df, output_path)

    spark.stop()
