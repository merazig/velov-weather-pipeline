"""Configuration de la SparkSession."""

from pyspark.sql import SparkSession


def get_spark_session() -> SparkSession:
    """Crée et retourne une SparkSession."""
    spark = (
        SparkSession.builder
        .appName("Velov")
        .master("spark://spark-master:7077")
        .getOrCreate()
    )

    return spark