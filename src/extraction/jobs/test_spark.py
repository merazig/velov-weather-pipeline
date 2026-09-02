from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("TestSpark")
    .master("spark://spark-master:7077")
    .getOrCreate()
)

data = [
    ("Villeurbanne", 100),
    ("Lyon", 250),
    ("Bron", 80),
]

df = spark.createDataFrame(
    data,
    ["commune", "nb_velos"],
)

df.show()

print("Nombre de lignes :", df.count())

spark.stop()