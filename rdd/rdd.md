# RDD (Resilient Distributed Dataset)

A fault-tolerant collection of elements that can be operated on in parallel.
There are two ways to create RDDs: parallelizing an existing collection in your driver program,
or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

Fundamentally, it's a data set, and it's an abstraction for a giant set of data and
what you're going to do is being setting up RDD objects and loading them up with big data sets and
then calling various methods on the RDD object to distribute the processing of that data.

RDD's are both distributed and resilient,
you know they can be spread out across an entire cluster of computers that may or may not be running locally and they can also handle the failure of specific executor nodes in your cluster automatically and keep on going even
if one node shuts down and redistribute the work as needed when that occurs.

- RDD Format:   
  Each line (row) in data corresponds to a value in the RDD.  
  Each value can be a string, dict or list, etc.    
  The values don't have to be structured.   
  

- SparkContext:
An object of driver program by which we can create RDDs.

- SparkConfig:
Configuration of Spark Context: E.g. Run on one computer or a cluster.

- Transforming RDDS: Methods to transform rdd.  
  map
  flatmap
  filter
  distinct
  sample
  Union, intersection, subtract or certesian of two RDDs


- RDDs Actions: Perform an action on it to get a result out of it.  
  collect
  count
  countyValue
  take
  top
  reduce
  ...

## Lazy Evaluation:
In working with RDD, nothing actually happens until you call an action.
Your Spark script isn't actually going to do anything until you call one of these action methods.

## Functional programming:
Many of the methods on RDD's actually accept a function as a parameter as opposed to a more typical parameter like you know a float or a string or something like that.   
Use a defined function or lambda (anonymous function).  

## Partitioning
Spark does not automatically optimally spread out the work of your job (e.g. self-join) throughout a cluster.   
+ partitionBy (RDD):  
  Use .partitionBy on an RDD before a large operation to split the operation on different executors.  
        e.g. join(), rightOutJoin(), cogroup(), groupByKey(), reduceByKey(), lookup(), ...  
  The operations preserve the partitioning in their results too.  
+ Choose Partition size:     
  - Too few partitions can't speed up the process that much.
  - Too many partitions create overhead.
  - At least as many as the cores or executors that fit in memory.
  - .partitionBy(100) is a good for 1000000 (1 million) records.


