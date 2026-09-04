"""Fonctions d'agrégation et de calcul d'activité Vélo'v."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs, col, lag, sum
from pyspark.sql.window import Window


def calculate_station_usage(df: DataFrame) -> DataFrame:
    """Calcule l'utilisation de chaque station Vélo'v."""
    window = Window.partitionBy(
        "station_id",
    ).orderBy(
        "horodate",
    )

    return df.withColumn(
        "previous_bikes_available",
        lag("bikes_available").over(window),
    ).withColumn(
        "station_usage",
        abs(col("bikes_available") - col("previous_bikes_available")),
    )


def filter_open_velov(df: DataFrame) -> DataFrame:
    """Conserve uniquement les relevés des stations ouvertes."""
    return df.filter(col("status") == "OPEN")


def aggregate_station_activity(df: DataFrame) -> DataFrame:
    """Agrège l'utilisation des stations par fenêtre de 15 minutes."""
    return df.groupBy(
        "station_id",
        "time_15min",
    ).agg(
        sum("station_usage").alias("activity_15min"),
    )
