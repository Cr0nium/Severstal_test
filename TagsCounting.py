from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, LongType, StringType, StructField

if __name__ == '__main__':
    print("Start Application")

    spark = SparkSession \
        .builder \
        .appName("Severstal_test") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    schemaLong = StructType([
        StructField("ts", TimestampType(), True),
        StructField("value", LongType(), True),
        StructField("tag", StringType(), True)
    ])

    schemaRolls = StructType([
        StructField("roll_id", LongType(), True),
        StructField("roll_start", TimestampType(), True),
        StructField("roll_end", TimestampType(), True)
    ])

    longDF = spark.read\
        .schema(schemaLong)\
        .option("header", "true")\
        .option("delimiter", ";")\
        .csv("C:\\Users\\timur\\PycharmProjects\\Severstal_test\\long.csv")

    rollsDF = spark.read \
        .schema(schemaRolls) \
        .option("header", "true") \
        .option("delimiter", ";") \
        .csv("C:\\Users\\timur\\PycharmProjects\\Severstal_test\\rolls.csv")

    rollsDF.show(truncate=False)

    spark.stop()
    print("Application Completed")
