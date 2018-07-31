'''

By Paul Wu uwzwu@uw.edu
'''


from pyspark.sql.types import *
from pyspark.sql import Row
import pandas as pd
from pyspark.sql import SparkSession
import platform

spark_builder = SparkSession.builder
if "Windows" in platform.platform():
    spark_builder.master("local[*]")
spark = spark_builder.appName("SummerProgram").getOrCreate()
sc = spark.sparkContext
