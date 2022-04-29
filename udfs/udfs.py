from pyspark.sql import SparkSession
from pyspark.sql.types import LongType
import pandas as pd
from pyspark.sql.functions import col, pandas_udf

spark = (SparkSession
.builder
.appName("udfs")
.getOrCreate())

def cubed(s):
    return s * s * s

spark.udf.register('cubed', cubed, LongType())

spark.range(1, 9).createOrReplaceTempView("udf_test")

spark.sql(
    """SELECT  id, cubed(id) AS id_cubed
       FROM udf_test"""
).show()

# using pandas_udf

def cubed(s: pd.Series) -> pd.Series:
    return s * s * s

cubed_udf = pandas_udf(cubed, returnType=LongType())

x = pd.Series([1,2,3])
print(cubed(x))

df = spark.range(1, 4)
df.select('id', cubed_udf(col('id'))).show()