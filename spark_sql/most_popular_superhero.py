from pyspark.sql import SparkSession, functions as func, types

if __name__ == '__main__':
    spark = SparkSession.builder.appName('PopularSuperHero').master('local[*]').getOrCreate()

    # In input, in each line, first value is superhero_id and
    # the rest of the values are IDs of superheros appeared with superhero_id in comic books.
    # In returned dataframe, each row has only "value" column.
    superhero_graph_df = spark.read.text('dataset/Marvel+Graph').cache()

    # Return a dataframe with each row containing (superhero_id, connections_count).
    superhero_connections_df = superhero_graph_df.withColumn('id', func.split(func.col('value'), '\\s')[0]).\
        withColumn('connections_count', (func.size(func.split(func.col('value'), '\\s')) - 1)).\
        select('id', 'connections_count')

    superhero_connections_df.show()

    # Find the most popular superhero with the most connections.
    # Result is a Row object
    popular_superhero_connection_row = superhero_connections_df.groupBy('id').\
        agg(func.sum('connections_count').alias('connections_freq')).\
        orderBy(func.desc('connections_freq')).first()

    # Get the dataset of superheros' name.
    superhero_names_schema = types.StructType([
            types.StructField("id", types.IntegerType(), False),
            types.StructField("name", types.StringType(), False)
    ])
    superhero_names_df = spark.read.schema(superhero_names_schema).csv('dataset/Marvel+Names', sep=' ').cache()

    # Get the name of the most popular superhero.
    popular_superhero_name_row = superhero_names_df.\
        filter(func.col('id') == popular_superhero_connection_row['id']).first()

    print('Most popular superhero is', popular_superhero_name_row['name'],
          'with', popular_superhero_connection_row['connections_freq'], 'connections!')

    spark.stop()
