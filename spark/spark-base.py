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

# See also https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html

# read text file as dataframe (column="value" each line in seperate row)
df = spark.read.text("text_file.txt")

# read csv file as dataframe infering types & reading first line as colum-headers (for tsv: sep="\t")
df = spark.read.csv("comma_seperated_values.csv", sep=",", header=True, inferSchema=True)

# split into words using java regex (every string of uninterrupted letters is considered a word)
df = df.select(F.split(df.value, "[^A-Za-z]+").alias("value"))

# flatten all arrays in each row (rows with empty arrays result in empty rows)
df = df.select(F.explode(df.value).alias("value"))

# remove empty rows
df = df.filter(df.value != "")

# print results (force evaluation of previous calls)
df.show(truncate=False)

# Access datatypes and schema
# e.g. df_sbier.dtypes[1][1] == "string"
df.dtypes

# Access columns
df.columns

df.printSchema()

