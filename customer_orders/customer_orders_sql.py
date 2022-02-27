from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


spark = (SparkSession
.builder
.appName('customerOrderSql')
.getOrCreate())

schema = StructType([
    StructField('id', IntegerType(), True),
    StructField('product', IntegerType(), True),
    StructField('amount', FloatType(), True)
])

df = (spark
.read
.schema(schema)
.csv('./customer_orders/data/customer-orders.csv'))
df.printSchema()

id_amount_df = df.select('id', 'amount')
id_total_amount_df = (id_amount_df
.groupBy('id')
.agg(
    func.round(
        func.sum('amount'),
        2
    ).alias('Total')
)
.orderBy('Total')
)

id_total_amount_df.show(id_total_amount_df.count())
