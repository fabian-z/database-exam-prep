from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Neue SparkSession erstellen
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("word count") \
      .getOrCreate() 

results = (
    # Einlesen der .csv Datei
    spark.read.text("frankenstein.txt")
    # DESIGN: Nach jedem Buchstaben ein split durchführen
    .select(F.split(F.col("value"), "").alias("letters"))
    # Ausführung eines explodes, umm die einzelnen Arrays als jeweilige Zeile ausgeben
    .select(F.explode(F.col("letters")).alias("letter"))
    # DESIGN: Jeden Buchstaben Klein schreiben
    .select(F.lower(F.col("letter")).alias("letter_lower"))
    # Mittels eines Regex nur die Buchstaben herausfiltern
    # DESIGN: Zahlen und Sonderzeichen werden außer Acht gelassen
    .select(F.regexp_extract(F.col("letter_lower"), "[a-z]*", 0).alias("only_letter"))
    # DESIGN: Sobald das Leerzeichen erscheint, wird dieses nicht mit übermittelt
    .where(F.col("only_letter") != "")
    .groupby(F.col("only_letter"))
    .count()
)

results.orderBy("only_letter", ascending=True).show(27)
#results.coalesce(1).write.csv("./results_single_partition.csv")
