from pyspark import SparkContext, SparkConf


conf = SparkConf().setMaster("local").setAppName("fakeFriendsMean")
sc = SparkContext(conf = conf)

def read_text(filename):
    text_rdd = sc.textFile(filename)
    return text_rdd

def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return (age, num_friends)

if __name__ == '__main__':
    file_loc = './fakefriends/data/fakefriends.csv'
    lines = read_text(file_loc)
    rdd = lines.map(parse_line)
    total_by_age = (rdd
    .mapValues(lambda x: (x, 1))
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])))
    # this return somenthing like this (age, (number of friends, number of persons in this age))
    mean_by_age = (total_by_age
    .mapValues(
        lambda x: x[0] / x[1]
    ))
    result = mean_by_age.collect()
    for r in result:
        print(r)
