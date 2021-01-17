from pyspark import SparkContext, SparkConf
from pprint import pprint

def parse_line(line):

    # Each line: location_id, date, temperature_type, temperature, ...
    row = line.split(',')
    location = row[0]
    temperature_type = row[2]
    # convert Celsius to fahrenheit.
    temperature = round(float(row[3]) * 0.1 * (9 / 5) + 32, 2)

    return location, temperature_type, temperature


if __name__ == '__main__':
    spark_conf = SparkConf().setMaster('local[*]').setAppName('MaxTemperatureByLocation')
    sc = SparkContext(conf=spark_conf)

    dataset_rdd = sc.textFile('dataset/temperature_by_location_1800.csv')

    location_type_temperature_rdd = dataset_rdd.map(parse_line)
    #pprint(location_type_temperature_rdd.collect())
    # Remove elements from RDD by a filter and return the removed elements as a new RDD.
    # filter out those having MIN in their temperature type.
    location_min_type_temperature_rdd = location_type_temperature_rdd.filter(lambda item: 'MAX' in item[1])

    # Convert the rdd to a key-value RDD by removing temperature type from each element.
    # Each element in RDD will be a (location, temperature) pair.
    location_temperature_rdd = location_min_type_temperature_rdd.map(lambda element: (element[0], element[2]))

    # Group by location by minimum of temperature in year (all days in 1800).
    min_temperature_by_location = location_temperature_rdd.reduceByKey(lambda temp_1, temp_2: max(temp_1, temp_2))

    for element in min_temperature_by_location.collect():
        print(element[0], ': ', str(element[1]) + 'F')


