from pyspark.sql import SparkSession, functions as func, types

if __name__ == '__main__':
    spark = SparkSession.builder.appName('ObscureSuperHero').master('local[*]').getOrCreate()

    # In input, in each line, first value is superhero_id and
    # the rest of the values are IDs of superheros appeared with superhero_id in comic books.
    # In returned dataframe, each row has only "value" column.
    superhero_graph_df = spark.read.text('dataset/Marvel+Graph').cache()

    # Return a dataframe with each row containing (superhero_id, connections_count).
    superhero_connections_df = superhero_graph_df.withColumn('id', func.split(func.col('value'), '\\s')[0]). \
        withColumn('connections_count', (func.size(func.split(func.col('value'), '\\s')) - 1)). \
        select('id', 'connections_count')

    # Find the most popular superhero with the most connections.
    # Result is a Row object
    superhero_connections_by_id_df = superhero_connections_df.groupBy('id'). \
        agg(func.sum('connections_count').alias('connections_freq'))

    superhero_connections_by_id_df.show()

    # Find min frequency in superhero connections.
    # first: to convert it to Row object
    min_frequency = superhero_connections_by_id_df.agg(func.min(func.col('connections_freq')).alias('min_freq')). \
        first()['min_freq']


    # Get the dataset of superheros' name.
    superhero_names_schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("name", types.StringType(), False)
    ])
    superhero_names_df = spark.read.schema(superhero_names_schema).csv('dataset/Marvel+Names', sep=' ').cache()

    # Get superheros with their name having minimum connection frequency.
    # First filter and then join two dataframes.
    most_obscure_superheros = superhero_connections_by_id_df.filter(func.col('connections_freq') == min_frequency).\
        join(superhero_names_df, on='id')

    most_obscure_superheros.show()

    spark.stop()
