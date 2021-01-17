# SparkSQL
SparkSQL runs Spark on top of a hive context and deal with structured data within Spark and run SQL queries on top of it.   
Hive runs on top of Hadoop and is for basically data warehousing.     
Spark SQL treats Spark as a data warehouse.       

## Dataframe
Dataframe API extends RDD to dataframe object:
+ It's like a database table containing a number of columns for each row object.
+ Run SQL queries
+ Create schema (leading to efficient storage)
+ Read from and Write to JSON, Hive, CSV
  The input data must be structured. I.e. There must be column names pointing to each value of row.

+ Communicate with outside API like Tableau acting as a relational database.

DataFrame vs DataSet:
DataFrame is actually a DataSet of row objects.
DataSet can wrap structured data types as well.
Sine Python is untyped, dataset is useless in Python.
But in Scala it's more useful, since they can be compiled optimally.

## SparkSession
Spark session: it is an object which where SparkSQL lives 
and by which we can interact with SparkSQL.

## Partitioning
+ repartition or repartitionByRange, coalesce in dataframe    

## Sharing Variables Across Executors
+ Broadcast a variable (e.g. list or dict): 
  spark.sparkContext.broadcast(variable_obj)
  
+ Accumulator:  
  It's a counter that's maintained and synchronized across all the different nodes (executors) in your cluster.
  spark.sparkContext.accumulator(initial_value)

## Caching
Cache a dataframe or RDD anytime you perform more than one action on it.

.cache(): Store it to memory.
.persist(): Store it to space.


## Useful functions in pyspark.sql

- RDD's transform functions equivalency in Dataframe:   
  + flatMap -> func.explode : Split a string column into new rows.
  + map -> df.select or df.withColumn: Manipulate a column or create a new one. 
  
```
# Split an Array/Struct type column to multiple columns
select(func.col('col.*'))
select(df.array_col[0], ....)
select(df.struct_col.key_1, ....)
```

```