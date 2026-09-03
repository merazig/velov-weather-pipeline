"""Fonctions de création de features temporelles."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    hour,
    dayofweek,
    when,
)


def add_time_features(df: DataFrame) -> DataFrame:
    """Ajoute les features temporelles aux données Vélo'v."""
    return (
        df.withColumn("annee", year(col("horodate")))
        .withColumn("mois", month(col("horodate")))
        .withColumn("jour", dayofmonth(col("horodate")))
        .withColumn("heure", hour(col("horodate")))
        .withColumn("jour_semaine", dayofweek(col("horodate")))
        .withColumn("week_end", when(col("jour_semaine").isin(1, 7), True).otherwise(False))
    )
