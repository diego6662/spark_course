from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField, StructType, IntegerType, StringType


spark = (
    SparkSession
    .builder
    .appName('MarvelSuperHeroe')
    .getOrCreate()
)

schema = StructType(
    [
        StructField('id', IntegerType(), True),
        StructField('name', StringType(), True),
    ]
)

names = (
    spark
    .read
    .schema(schema)
    .option('sep', ' ')
    .csv('./marvel-super-hero/data/Marvel+Names')
)

lines = spark.read.text('./marvel-super-hero/data/Marvel+Graph')

connections = (
    lines
    .withColumn('id', func.split(func.col('value'), ' ')[0])
    .withColumn('connections', func.size(func.split(func.col('value'), ' ')) - 1)
    .groupBy('id')
    .agg(func.sum('connections').alias('connections'))
)

hero_only_one_conn = (
    connections
    .filter(func.col('connections') == 1)
)
hero_only_one_conn_name = (
    hero_only_one_conn
    .join(names, 'id')
    .select('name', 'connections')
)
hero_only_one_conn_name.show(hero_only_one_conn_name.count())

min_connection = (
    connections
    .agg(func.min(func.col('connections')))
    .first()
)

print(f'the minimun number of connections is {min_connection[0]} connections')

spark.stop()