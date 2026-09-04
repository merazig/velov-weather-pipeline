"""Lit et affiche les résultats Gold stockés dans MinIO."""

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


METRICS_PATH = "s3a://datalake/gold/velov_weather_metrics"
WEATHER_PATH = "s3a://datalake/gold/weather_impact"


def main():
    """Lit et affiche les datasets Gold."""
    spark = get_spark_session("ReadGold")

    configure_minio(spark)

    try:
        # Indicateurs décisionnels
        metrics_df = spark.read.parquet(METRICS_PATH)

        print("\n=== INDICATEURS DECISIONNELS ===")

        metrics_df.orderBy(
            "year",
            "month",
            "day",
            "hour",
            "commune",
        ).show(
            50,
            truncate=False,
        )

        # Impact météo
        weather_df = spark.read.parquet(WEATHER_PATH)

        print("\n=== IMPACT DE LA METEO ===")

        weather_df.orderBy(
            "year",
            "month",
            "commune",
            "is_raining",
        ).show(
            50,
            truncate=False,
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
