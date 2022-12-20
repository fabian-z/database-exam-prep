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

ride=spark.read.option("header","true").csv("201709-citibike-tripdata.csv")

# Column names
ride.head()

# First 5 lines
ride.show(5)

# Gruppierung nach den gestarteten Stationsnamen und diese hoch zählen
ride.groupBy("start station name").count().show(10, truncate=False)
#ride.groupBy("start station name").count().sort(F.desc("count")).show(10, truncate=False)

# Ergebnis abspeichern in einer .csv Datei
ride.coalesce(1).write.csv("ridesFrom")