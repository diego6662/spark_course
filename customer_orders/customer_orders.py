from pyspark import  SparkContext, SparkConf


conf = (SparkConf()
.setMaster('local')
.setAppName('CustomerOrders'))
sc = SparkContext(conf = conf)

def get_data(filename):
    return sc.textFile(filename)

def split_data(lines):
    line = lines.split(',')
    id = int(line[0])
    total = float(line[2])
    return (id, total)

def main():
    filename = './customer_orders/data/customer-orders.csv'
    rdd = get_data(filename)
    customer_rdd = rdd.map(split_data)
    customer_total_rdd = customer_rdd.reduceByKey(lambda x, y: x + y)
    flipped = customer_total_rdd.map(lambda x: (x[1], x[0]))
    sorted_rdd = flipped.sortByKey()
    result = sorted_rdd.collect()
    for r in result:
        print(r)

if __name__ == '__main__':
    main()