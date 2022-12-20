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

df_sbier = spark.read.csv("studibier.csv", header=True, inferSchema=True)
df_sbier.show()

# Datatypes
df_sbier.dtypes

if df_sbier.dtypes[1][1] == "string":
    print("It's a string")

df_sbier.select(" Lieblingsbier").distinct().show()


#df_sbier.agg({' Alter':'avg'}).show()
df_sbier.groupBy(" Lieblingsbier").agg({' Alter':'avg'}).show()

# Fixen von Quelldaten
df_sbier.withColumnRenamed(" Alter", "Alter").agg({"Alter": "avg"}).show()

#df_sbier.select(" Lieblingsbier", " Alter").show()
df_sbier.select(" Lieblingsbier", " Alter").distinct().show()

df_sbier.withColumn("Alter nach 2 Jahren", df_sbier[" Alter"]+2).withColumn("Eins", F.lit(1)).show()

df_sbierMitMuell = df_sbier.withColumn("Alter nach 2 Jahren", df_sbier[" Alter"]+2).withColumn("Eins", F.lit(1))
df_sbierMitMuell.show()

# Drop column
df_sbierMitMuell.drop("Alter nach 2 Jahren").show

nullWerteDaten = [("Jenny", None, 22.0, 24.0, 1),("Jenny", None, None, 24.0, 1),("Jenny", None, None, None, 1)]
nwdSchema = df_sbierMitMuell.schema
df_bierWithNull = df_sbierMitMuell.union(spark.createDataFrame(data=nullWerteDaten, schema=nwdSchema))
df_bierWithNull.show()

df_bierWithNull.show()
# Dropen aller Null-Zeilen
df_bierWithNull.na.drop(how="any").show()