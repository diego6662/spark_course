from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = (SparkSession
    .builder
    .appName("SparkSQLExampleApp")
    .getOrCreate())

csv_path = "./flights/data/departuredelays.csv"
# option 1 using the schema inference of spark
# df = (spark.read.format("csv")
#     .option("inferSchema", "true")
#     .option("header", "true")
#     .load(csv_path)
# )
# option 2 given defined schema
schema = '`date` STRING, `delay` INT, `distance` INT, `origin` STRING, `destination` STRING'

df = (spark.read.format("csv")
    .option("header", "true")
    .schema(schema)
    .load(csv_path)
)

df.createOrReplaceTempView("us_delay_flights_tbl")

# flights with distance greater than 1000 miles

spark.sql(
    """SELECT distance, origin, destination
    FROM us_delay_flights_tbl
    WHERE distance > 1000
    ORDER BY distance DESC
    """
).show(10)

spark.sql(
    """SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
    ORDER BY delay DESC
    """
).show(10)

spark.sql(
    """SELECT  delay, origin, destination,
    CASE
        WHEN delay > 360 THEN 'Very Long Delays'
        WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
        WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
        WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delays'
        WHEN delay = 0 THEN 'No Delays'
        ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin,delay DESC
    """
).show(10)
# querys writes as dataframe operation
(df.select("distance", "origin", "destination")
.where(func.col("distance") > 1000)
.orderBy(func.desc("distance"))).show(10)



(df.select("date","delay","origin","destination")
.where((func.col('delay') > 120) & (func.col('origin') == 'SFO') & (func.col('destination') == 'ORD'))
.orderBy(func.desc('delay'))).show(10)


(df.select('delay', 'origin', 'destination')
.withColumn('Flight_Delays', func.when(func.col('delay') > 360,'Very Long Delays')
.when((func.col('delay') > 120) & (func.col('delay') < 360), 'Long Delays')
.when((func.col('delay') > 60) & (func.col('delay') < 120), 'Short Delays')
.when((func.col('delay') > 0) & (func.col('delay') < 60), 'Tolerable Delays')
.when(func.col('delay') == 0, 'No Delays')
.otherwise('Early'))
.orderBy("origin",func.desc("delay"))).show(10)
