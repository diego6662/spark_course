from pyspark.sql import SparkSession
from pyspark.sql.functions import count


spark = (SparkSession
        .builder
        .appName("PythonMnMCount")
        .getOrCreate())

def read_data(mnm_file: str) :
    mnm_df = (spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load(mnm_file))
    return mnm_df

if __name__ == '__main__':
    mnm_file = "./data/mnm_dataset.csv"
    mnm_df = read_data(mnm_file)
    count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))
    count_mnm_df.show(n = 60, truncate = False)
    print(f"Total rows = {count_mnm_df.count()}")

    ca_count_mnm_df = (mnm_df
    .select("State", "Color", "Count")
    .where(mnm_df.State == "CA")
    .groupBy("State", "Color")
    .agg(count("Count").alias("Total"))
    .orderBy("Total", ascending=False))

    ca_count_mnm_df.show(n = 10, truncate = False)
    spark.stop()