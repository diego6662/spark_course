from pyspark import SparkContext, SparkConf


conf = (SparkConf()
.setMaster("local")
.setAppName("MaxTemperature"))
sc = SparkContext(conf = conf)

def get_data(filename):
    rdd = sc.textFile(filename)
    return rdd

def split_data(lines):
    field = lines.split(',')
    stationID = field[0]
    entryType = field[2]
    temperature = float(field[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

def main():
    filePath = './temperature/data/1800.csv'
    data_rdd = get_data(filePath)
    parsedLines = data_rdd.map(split_data)
    maxTemps = parsedLines.filter(lambda x: 'TMAX' in x[1])
    stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
    maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
    results = maxTemps.collect()
    for result in results:
        print(f'station: {result[0]}, temperature:{result[1]}')

if __name__ == '__main__':
    main()
