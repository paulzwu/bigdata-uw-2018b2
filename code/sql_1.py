
from pyspark.sql import SparkSession
spark_builder = SparkSession.builder

spark_builder.master("local[*]")
spark = spark_builder.appName("SummerProgram").getOrCreate()
sc = spark.sparkContext

spark.sql("create table if not exists students (id int, name varchar(20))")
spark.sql("insert into students values (1, 'paul')")
spark.sql("select * from students").show()
spark.stop()