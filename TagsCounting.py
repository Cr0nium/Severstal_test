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
    spark.conf.set("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 6000)

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

    longDF = spark.read \
        .schema(schemaLong) \
        .option("header", "true") \
        .option("delimiter", ";") \
        .csv("C:\\Users\\timur\\PycharmProjects\\Severstal_test\\long_short.csv") \
        .repartitionByRange(48, 'ts')

    rollsDF = spark.read \
        .schema(schemaRolls) \
        .option("header", "true") \
        .option("delimiter", ";") \
        .csv("C:\\Users\\timur\\PycharmProjects\\Severstal_test\\rolls_short.csv")

    rollsAndLongRightJoinDF = longDF.join(rollsDF, (rollsDF.roll_start <= longDF.ts) & (longDF.ts <= rollsDF.roll_end), how='right')\
        .drop("ts", "roll_start", "roll_end")
    rollsAndLongRightJoinDF.createOrReplaceTempView("tmp_view")
    resultAggDF = spark.sql(
        "select roll_id, tag, max(value) as max, mean(value) as mean, percentile_approx(value, 0.5) as median, " +
        "percentile_approx(value, 0.99) as 99_percentile, percentile_approx(value, 0.01) as 1_percentile from tmp_view group by roll_id, tag order by roll_id")

    resultAggDF \
        .orderBy("roll_id") \
        .coalesce(1) \
        .write \
        .format("csv") \
        .option("header", "true") \
        .save("H:\\res\\finalPython1")

    spark.stop()
    print("Application Completed")
