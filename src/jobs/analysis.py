"""Analyse les données Silver et écrit les indicateurs dans MinIO Gold."""

from pyspark.sql import functions as F

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


SILVER_PATH = "s3a://datalake/silver/velov_weather"
GOLD_METRICS_PATH = "s3a://datalake/gold/velov_weather_metrics"
GOLD_WEATHER_PATH = "s3a://datalake/gold/weather_impact"


def build_metrics(df):
    """Calcule les indicateurs décisionnels par commune et par heure."""
    return df.groupBy(
        "commune",
        "year",
        "month",
        "day",
        "hour",
    ).agg(
        F.avg("bikes_available").alias("avg_bikes_available"),
        F.avg("stands_available").alias("avg_stands_available"),
        F.avg("availability_rate").alias("avg_availability_rate"),
        F.avg("temperature_2m_c").alias("avg_temperature_2m_c"),
        F.avg("relative_humidity_2m_pct").alias("avg_humidity_pct"),
        F.sum("rain_mm").alias("total_rain_mm"),
        F.avg("wind_speed_10m_kmh").alias("avg_wind_speed_kmh"),
        F.count("*").alias("observation_count"),
    )


def build_weather_impact_metrics(df):
    """Compare la disponibilité des vélos selon la présence de pluie."""
    return (
        df.withColumn(
            "is_raining",
            F.col("rain_mm") > 0,
        )
        .groupBy(
            "commune",
            "year",
            "month",
            "is_raining",
        )
        .agg(
            F.avg("bikes_available").alias("avg_bikes_available"),
            F.avg("stands_available").alias("avg_stands_available"),
            F.avg("availability_rate").alias("avg_availability_rate"),
            F.avg("temperature_2m_c").alias("avg_temperature_2m_c"),
            F.avg("rain_mm").alias("avg_rain_mm"),
            F.count("*").alias("observation_count"),
        )
    )


def main():
    """Lit Silver, calcule les indicateurs et écrit les résultats Gold."""
    spark = get_spark_session("VelovWeatherAnalysis")

    configure_minio(spark)
    try:
        print("\n=== LECTURE SILVER ===")

        silver_df = spark.read.parquet(SILVER_PATH)

        silver_df.printSchema()

        print("\n=== 5 LIGNES SILVER ===")

        silver_df.show(
            5,
            truncate=False,
        )

        # Indicateurs décisionnels
        metrics_df = build_metrics(silver_df)

        print("\n=== INDICATEURS DECISIONNELS ===")

        metrics_df.printSchema()

        metrics_df.show(
            10,
            truncate=False,
        )

        # Impact de la pluie
        weather_impact_df = build_weather_impact_metrics(silver_df)

        print("\n=== IMPACT DE LA PLUIE ===")

        weather_impact_df.printSchema()

        weather_impact_df.show(
            20,
            truncate=False,
        )

        # Écriture Gold des indicateurs
        print("\n=== ECRITURE GOLD - METRICS ===")

        (
            metrics_df.write.mode("overwrite")
            .partitionBy(
                "year",
                "month",
            )
            .parquet(GOLD_METRICS_PATH)
        )

        print(f"Écriture terminée : {GOLD_METRICS_PATH}")

        # Écriture Gold de l'impact météo
        print("\n=== ECRITURE GOLD - WEATHER IMPACT ===")

        (
            weather_impact_df.write.mode("overwrite")
            .partitionBy(
                "year",
                "month",
            )
            .parquet(GOLD_WEATHER_PATH)
        )

        print(f"Écriture terminée : {GOLD_WEATHER_PATH}")

        print("\n=== ANALYSE TERMINEE ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

    """Lancer le job Spark avec la commande suivante :

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/analysis.py

    """

    """ Pour vérifier les 2 sorties Gold dans MinIO:

    docker exec minio mc ls local/datalake/gold

    #puis:

    docker exec minio mc ls local/datalake/gold/velov_weather_metrics

     #puis:

     docker exec minio mc ls local/datalake/gold/weather_impact
    
    """

    """ La logique du pipeline est la suivante :
   
            MinIO Silver
                ↓
            analysis.py
                ↓
            build_metrics()        
                ↓
            indicateurs par commune / date / heure
            → impact pluie / météo sur la disponibilité
                ↓
            Gold/velov_weather_metrics

            MinIO Silver
                ↓
            build_weather_impact_metrics()
                ↓
            comparaison pluie / pas pluie
                ↓
            Gold/weather_impact
 """

# 1- indicateurs décisionnels Gold sont calculés:

# (par commune + année + mois + jour + heure), des métriques comme :
# avg_bikes_available
# avg_stands_available
# avg_availability_rate
# avg_temperature_2m_c
# avg_humidity_pct
# total_rain_mm
# avg_wind_speed_kmh
# observation_count

# 2- impact de la pluie sur la disponibilité des vélos Gold est calculé:
# Répends au question :
# Est-ce que les conditions météorologiques, notamment la pluie,
#  influencent la disponibilité des Vélo'v ?

# is_raining = true
# is_raining = false
