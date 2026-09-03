"""Netoyage des données."""

from pyspark.sql import DataFrame


def remove_velov_duplicates(df: DataFrame) -> DataFrame:
    """Supprime les doublons des disponibilités Vélo'v."""
    return df.dropDuplicates(["station_id", "horodate"])


def remove_weather_duplicates(df: DataFrame) -> DataFrame:
    """Supprime les doublons météo."""
    return df.dropDuplicates(["commune", "datetime"])


def remove_station_duplicates(df: DataFrame) -> DataFrame:
    """Supprime les doublons des stations Vélo'v."""
    return df.dropDuplicates(["idstation"])
