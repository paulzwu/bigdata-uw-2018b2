
'''

Word count problem.

By Paul Wu uwzwu@uw.edu

'''

from pyspark.sql import SparkSession
spark_builder = SparkSession.builder

spark_builder.master("local[*]")
spark = spark_builder.appName("SummerProgram").getOrCreate()
sc = spark.sparkContext
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("flight_data")
spark.sql("select * from flight_data").show()
spark.sql("select * from flight_data where DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME").show()

spark.stop()