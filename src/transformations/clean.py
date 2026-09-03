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


def remove_invalid_velov_rows(df: DataFrame) -> DataFrame:
    """Supprime les lignes Vélo'v sans informations essentielles."""
    return df.dropna(subset=["station_id", "horodate", "status"])


def remove_invalid_weather_rows(df: DataFrame) -> DataFrame:
    """Supprime les lignes météo sans commune ou date."""
    return df.dropna(subset=["commune", "datetime"])
