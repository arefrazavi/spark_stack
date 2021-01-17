from pyspark import SparkConf, SparkContext

def parse_line(line: str) -> tuple:
    # Line: customer_id, product_id, cost
    row = line.split(',')
    customer_id = row[0]
    cost = float(row[2])

    return customer_id, cost


if __name__ == '__main__':
    spark_config = SparkConf().setMaster('local[*]').setAppName('TotalAmountSpentByCustomer')
    sc = SparkContext(conf=spark_config)

    dataset_rdd = sc.textFile('dataset/customer-orders.csv')
    # Convert to key-value RDD (customer_id, amount)
    customer_amount_rdd = dataset_rdd.map(parse_line)

    # Sum amount for each customer_id and sort by amount.
    amount_spend_by_customer_rdd = customer_amount_rdd.\
        reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey()

    for amount, customer_id in amount_spend_by_customer_rdd.collect():
        print(customer_id, ':', round(amount, 2))
