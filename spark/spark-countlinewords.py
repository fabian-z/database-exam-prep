from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
#from pyspark.sql.functions import split, explode, lower, col, regexp_extract, length, greatest, countDistinct
import pyspark.sql.functions as F

config = SparkConf().setAppName("MySparkApp")
context = SparkContext(conf=config)

# Create SparkSession
spark = SparkSession.builder \
 .master("local[1]") \
 .appName("SparkApp") \
 .getOrCreate() # getOrCreate returns new or existing session

txt = spark.read.text("/home/student/Desktop/Klausurtest.csv")
txt.show(100, truncate=False)

# DESIGN: 5 ist kein Wort ;)
lines = txt.select(F.split(txt.value, "[^a-z,A-Z,ß]").alias("Zeile"))
lines.show(100,truncate=False)

# .alias not working - srsly?
# DESIGN: Leerzeichen nicht mitzählen
non_empty = lines.select(F.array_remove(F.col("Zeile"), "")).withColumnRenamed("array_remove(Zeile, )", "Zeile")
counted = non_empty.withColumn("Count", F.size(F.col("Zeile")))
counted.select(counted.Count, counted.Zeile).show(truncate=False)