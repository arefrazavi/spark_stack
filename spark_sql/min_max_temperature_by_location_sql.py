from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import types

if __name__ == '__main__':
    spark = SparkSession.builder.appName('MinMaxTemperature').getOrCreate()

    df_schema = types.StructType([
        types.StructField("location_id", types.StringType(), False),
        types.StructField("date", types.StringType(), False),
        types.StructField("temperature_type", types.StringType(), False),
        types.StructField("temperature", types.FloatType(), False)
    ])

    # The order in the chain of operations is important. schema must come before csv.
    dataset_df = spark.read.schema(df_schema).csv('dataset/temperature_by_location_1800.csv', header=False,
                                                  sep=',').cache()

    location_temp_df = dataset_df.select('location_id', 'temperature_type', 'temperature'). \
        filter("temperature_type LIKE '%MIN%'").select('location_id', 'temperature')

    min_max_temp_by_location = location_temp_df.groupBy('location_id').\
        agg(func.min('temperature').alias('min_temp (C)'), func.max('temperature').alias('max_temp (C)'))

    # Add cols for fahrenheit
    min_max_temp_by_location_with_f = min_max_temp_by_location.withColumn('min_temp (F)', func.round(func.col('min_temp (C)') * 0.1 * (9 / 5) + 32, 2)).\
        withColumn('max_temp (F)', func.round(func.col('max_temp (C)') * 0.1 * (9 / 5) + 32, 2))

    min_max_temp_by_location_with_f.show()

    spark.stop()
