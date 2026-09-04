"""Lit et affiche les résultats Gold de l'année 2023."""

from pyspark.sql import functions as F

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


METRICS_PATH = "s3a://datalake/gold/velov_weather_metrics"
WEATHER_PATH = "s3a://datalake/gold/weather_impact"


def main():
    """Lit et affiche les résultats Gold pour 2023."""

    spark = get_spark_session(
        "ReadGold2023"
    )

    configure_minio(spark)

    try:
        metrics_df = (
            spark.read
            .parquet(
                METRICS_PATH
            )
            .filter(
                F.col("year") == 2023
            )
        )

        weather_df = (
            spark.read
            .parquet(
                WEATHER_PATH
            )
            .filter(
                F.col("year") == 2023
            )
        )

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

        print("\n=== IMPACT PLUIE - LYON 1ER - SEPTEMBRE 2023 ===")

        (
            weather_df
            .filter(
                (F.col("commune") == "Lyon 1er Arrondissement")
                & (F.col("month") == 9)
            )
            .orderBy(
                "is_raining",
            )
            .show(
                truncate=False,
            )
        )

    finally:
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
