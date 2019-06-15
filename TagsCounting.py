from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("Start Application")

    spark = SparkSession\
        .builder\
        .appName("Severstal_test")\
        .master("local[*]")\
        .getOrCreate()
    rollDF = spark.read.csv("C:\\Users\\timur\\PycharmProjects\\Severstal_test\\rolls.csv")
    rollDF.show()
