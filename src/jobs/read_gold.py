"""Lit et affiche les résultats Gold stockés dans MinIO."""

from pyspark.sql import functions as F

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


# Chemins des datasets Gold dans le Data Lake MinIO.
# Ces données ont déjà été agrégées par le job analysis.py.
METRICS_PATH = "s3a://datalake/gold/velov_weather_metrics"
WEATHER_PATH = "s3a://datalake/gold/weather_impact"


def main():
    """Lit et contrôle les indicateurs Gold pour l'année 2023."""

    # Création de la session Spark.
    spark = get_spark_session(
        "ReadGold2023"
    )

    # Configuration de Hadoop/S3A afin que Spark puisse
    # accéder au stockage objet MinIO.
    configure_minio(spark)

    try:
        # ============================================================
        # LECTURE DES DONNÉES GOLD
        # ============================================================

        # Lecture des KPI Vélo'v + météo.
        # Le filtre sur l'année 2023 permet à Spark de profiter
        # du partitionnement du dataset par année.
        metrics_df = (
            spark.read
            .parquet(METRICS_PATH)
            .filter(F.col("year") == 2023)
        )

        # Lecture du dataset permettant de comparer la disponibilité
        # des vélos selon la présence ou l'absence de pluie.
        weather_df = (
            spark.read
            .parquet(WEATHER_PATH)
            .filter(F.col("year") == 2023)
        )

        # ============================================================
        # CONTRÔLE DES KPI PRINCIPAUX
        # ============================================================

        # Affichage d'un échantillon des indicateurs calculés :
        # - vélos disponibles moyens ;
        # - places disponibles moyennes ;
        # - taux moyen de disponibilité ;
        # - température et humidité moyennes ;
        # - quantité de pluie ;
        # - vitesse moyenne du vent ;
        # - nombre d'observations.
        print("\n=== METRICS GOLD - 2023 ===")

        (
            metrics_df
            .orderBy(
                "month",
                "day",
                "hour",
                "commune",
            )
            .show(
                50,
                truncate=False,
            )
        )

        # ============================================================
        # CONTRÔLE DE L'IMPACT DE LA MÉTÉO
        # ============================================================

        # Comparaison des indicateurs de disponibilité entre
        # les périodes pluvieuses et non pluvieuses.
        print("\n=== WEATHER IMPACT GOLD - 2023 ===")

        (
            weather_df
            .orderBy(
                "month",
                "commune",
                "is_raining",
            )
            .show(
                50,
                truncate=False,
            )
        )

        # ============================================================
        # EXEMPLE DE CONTRÔLE CIBLÉ
        # LYON 1ER - SEPTEMBRE 2023
        # ============================================================

        # Analyse horaire d'une commune et d'un mois précis.
        # Ce contrôle permet notamment d'observer l'évolution
        # de la disponibilité des vélos au cours de la journée.
        print("\n=== LYON 1ER - SEPTEMBRE 2023 ===")

        (
            metrics_df
            .filter(
                (F.col("commune") == "Lyon 1er Arrondissement")
                & (F.col("month") == 9)
            )
            .orderBy(
                "day",
                "hour",
            )
            .show(
                30,
                truncate=False,
            )
        )

        # ============================================================
        # IMPACT DE LA PLUIE SUR UN CAS CONCRET
        # ============================================================

        # Comparaison de la disponibilité moyenne avec et sans pluie
        # pour Lyon 1er en septembre 2023.
        #
        # Ce contrôle permet de vérifier que les données Gold peuvent
        # être utilisées pour produire des indicateurs décisionnels
        # sur la relation entre météo et disponibilité des Vélo'v.
        print(
            "\n=== IMPACT PLUIE - LYON 1ER - SEPTEMBRE 2023 ==="
        )

        (
            weather_df
            .filter(
                (F.col("commune") == "Lyon 1er Arrondissement")
                & (F.col("month") == 9)
            )
            .orderBy("is_raining")
            .show(
                truncate=False,
            )
        )

    finally:
        # Arrêt propre de la session Spark à la fin du job.
        spark.stop()


if __name__ == "__main__":
    main()

""" Exemple d'exécution du job ReadGold dans le conteneur spark-master :

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/read_gold.py
    
    """

"""Lyon 1er en septembre 2023, un taux moyen de disponibilité de 37,32 % sans pluie
 contre 35,12 % avec pluie
"""
