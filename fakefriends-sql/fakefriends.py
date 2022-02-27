from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count


spark = (SparkSession
.builder
.appName('fakefriends')
.getOrCreate())

def get_data(filename):
    rdd = (spark.
    read
    .format('csv')
    .option('header', 'true')
    .option('inferSchema', 'true')
    .load(filename))
    return rdd

def main():
    filename = './fakefriends-sql/data/fakefriends-header.csv'
    rdd = get_data(filename)
    age_friends_df = rdd.select('Age','Friends')
    mean_friends_by_age = (age_friends_df
    .groupBy('Age')
    .agg((sum('Friends') / count('Friends')).alias('Mean'))
    .orderBy('Age'))
    mean_friends_by_age.show(n=100)
    spark.stop()
if __name__ == '__main__':
    main()