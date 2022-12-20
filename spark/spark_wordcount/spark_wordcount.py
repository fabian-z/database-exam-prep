from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import split, explode, lower, col, regexp_extract, length, greatest, countDistinct

config = SparkConf().setAppName("MySparkApp")
context = SparkContext(conf=config)
# Aufgabe: fuehre diese Zelle 2x aus. Was passiert
# wir wollen DataFrames nutzen, und dafuer brauchen wir eine SparkSession
# Import SparkSession
from pyspark.sql import SparkSession
# Create SparkSession
spark = SparkSession.builder \
 .master("local[1]") \
 .appName("Datenbanken mit Spark") \
 .getOrCreate() # getOrCreate liefert existierende Session wenn es schon eine gibt

frank = spark.read.text("frankenstein.txt")

frank.printSchema()
frank.show()

count = frank.count()
print(count)

words = frank.select(explode(split(frank.value, "[^a-z,A-Z]")).alias("words"))
words.show(10)

words_lower = words.select(lower(col("words")).alias("words_lower"))
words_lower.show()

words_clean = words_lower.select(regexp_extract("words_lower", "[a-zA-z]{3,}|a|i", 0).alias("words_clean"))
words_clean.show()

words_nonempty = words_clean.filter(words_clean.words_clean != "").withColumnRenamed("words_clean", "words_nonempty")
words_nonempty.show()

filter_words = ["is", "not", "if", "the"]
words_filtered = words_nonempty.filter(~words_nonempty.words_nonempty.isin(filter_words))
words_filtered.show()

words_withlength = words_filtered.withColumn("length", length(words_filtered.words_nonempty))
words_withlength.groupBy("length").count().sort("count").show()
#words_filtered.groupBy("words_nonempty").count().sort("count").show()
#words_filtered.agg(countDistinct("words_nonempty")).show()

words_nonempty.coalesce(1).write.csv("hallu2.csv")

#datenTest = spark.createDataFrame([["key", 20_000_000, 10_000_000_000]],["key", "value1", "value2"])
#datenTest.printSchema()

#print(datenTest.dtypes)
#datenA4M = datenTest.select(greatest(col("value1"), col("value2")).alias("maxVal")).show()

