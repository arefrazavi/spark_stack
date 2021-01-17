from pyspark import SparkContext
from pprint import pprint
from pyspark.sql import SparkSession, Row

number_dataset = [0, 1, 2, 3, 4, 5]

sc = SparkContext()

# Create an RDD from the dataset list.
# It's a single value RDD. Every element of RDD contains a single value.
number_rdd = sc.parallelize(number_dataset)

# Transform RDD
pow_2_number_rdd = number_rdd.map(lambda item: pow(2, item))

# Loop over RDD after transform
print('Power of 2 RDD after transformation:')
pprint(pow_2_number_rdd.collect())

print('Convert a spark context RDD to spark session dataframe:')

# Create a spark session
spark = SparkSession(sc)
hasattr(pow_2_number_rdd, "toDF")

# *** Convert RDD to dataframe: .toDF
# convert integers to tuple, because in dataframe there must be column names pointing to each value of row.
# If there is no column name, spark try to infer it. For strings, it chooses "value" column,
# but it can not infer schema for type int and ... .
pow_2_number_tuple_rdd = pow_2_number_rdd.map(lambda item: Row(pow_2=item))
pow_2_number_df = pow_2_number_tuple_rdd.toDF()
pow_2_number_df.show()

# *** Convert dataframe to RDD: .rdd
# Convert dataframe to RDD having Row objects
pprint(pow_2_number_df.rdd.collect())
# Convert dataframe to original RDD having single integers,
pprint(pow_2_number_df.rdd.map(tuple).map(lambda x: x[0]).collect())
