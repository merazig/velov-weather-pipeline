"""Fonctions de conversion des types des données."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


def cast_velov_types(df: DataFrame) -> DataFrame:
    """Convertit les types des données Vélo'v."""
    return df.withColumn("horodate", to_timestamp(col("horodate"), "yyyy-MM-dd HH:mm:ssXXX"))


def cast_weather_types(df: DataFrame) -> DataFrame:
    """Convertit les types des données météo."""
    return df.withColumn("datetime", col("datetime").cast("timestamp"))
