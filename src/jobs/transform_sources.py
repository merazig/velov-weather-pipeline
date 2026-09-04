"""Nettoie, prépare, joint et écrit les données dans MinIO Silver."""

from pyspark.sql import functions as F

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


SILVER_PATH = "s3a://datalake/silver/velov_weather"


def read_collection(
    spark,
    collection_name,
):
    """Lit une collection MongoDB avec Spark."""
    return (
        spark.read
        .format("mongodb")
        .option(
            "collection",
            collection_name,
        )
        .load()
    )


def transform_velov(df):
    """Nettoie les disponibilités Vélo'v."""
    return (
        df
        .select(
            "station_id",
            "horodate",
            "bikes_available",
            "stands_available",
            "capacity",
            "status",
        )
        .dropna(
            subset=[
                "station_id",
                "horodate",
            ]
        )
        .withColumn(
            "horodate",
            F.to_timestamp(
                "horodate"
            ),
        )
        .dropDuplicates(
            [
                "station_id",
                "horodate",
            ]
        )
    )


def transform_stations(df):
    """Nettoie le référentiel des stations."""
    return (
        df
        .select(
            "idstation",
            "nom",
            "commune",
            "lat",
            "lon",
            "nbbornettes",
            "ouverte",
        )
        .dropna(
            subset=[
                "idstation",
                "commune",
                "lat",
                "lon",
            ]
        )
        .dropDuplicates(
            ["idstation"]
        )
    )


def transform_meteo(df):
    """Nettoie les données météo."""
    return (
        df
        .select(
            "commune",
            "datetime",
            "temperature_2m_c",
            "apparent_temperature_c",
            "relative_humidity_2m_pct",
            "precipitation_mm",
            "rain_mm",
            "snowfall_cm",
            "weather_code",
            "wind_speed_10m_kmh",
            "wind_gusts_10m_kmh",
            "visibility_m",
            "is_day",
        )
        .dropna(
            subset=[
                "commune",
                "datetime",
            ]
        )
        .dropDuplicates(
            [
                "commune",
                "datetime",
            ]
        )
    )


def enrich_velov_with_stations(
    velov_df,
    stations_df,
):
    """Ajoute les informations des stations aux données Vélo'v."""
    return (
        velov_df
        .join(
            stations_df,
            velov_df["station_id"]
            == stations_df["idstation"],
            "left",
        )
        .drop(
            stations_df["idstation"]
        )
    )


def add_15_min_bucket(
    df,
    timestamp_column,
):
    """Arrondit un timestamp au quart d'heure inférieur."""
    timestamp_seconds = (
        F.col(timestamp_column)
        .cast("long")
    )

    bucket_seconds = (
        F.floor(
            timestamp_seconds / 900
        )
        * 900
    )

    return (
        df
        .withColumn(
            "datetime_15m",
            F.to_timestamp(
                F.from_unixtime(
                    bucket_seconds
                )
            ),
        )
    )


def add_time_features(df):
    """Ajoute les variables temporelles et le taux de disponibilité."""
    return (
        df
        .withColumn(
            "year",
            F.year("horodate"),
        )
        .withColumn(
            "month",
            F.month("horodate"),
        )
        .withColumn(
            "day",
            F.dayofmonth("horodate"),
        )
        .withColumn(
            "hour",
            F.hour("horodate"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("horodate"),
        )
        .withColumn(
            "is_weekend",
            F.col("day_of_week").isin(
                1,
                7,
            ),
        )
        .withColumn(
            "availability_rate",
            F.when(
                F.col("capacity") > 0,
                F.col("bikes_available")
                / F.col("capacity"),
            ).otherwise(None),
        )
    )


def main():
    """Lit, transforme et écrit les données dans MinIO Silver."""
    spark = get_spark_session(
        "TransformSources"
    )

    configure_minio(spark)

    try:
        # Lecture MongoDB
        velov_df = read_collection(
            spark,
            "velov_availabilities",
        )

        stations_df = read_collection(
            spark,
            "velov_stations",
        )

        meteo_df = read_collection(
            spark,
            "lyon_meteo",
        )

        # Nettoyage
        velov_clean = transform_velov(
            velov_df
        )

        stations_clean = transform_stations(
            stations_df
        )

        meteo_clean = transform_meteo(
            meteo_df
        )

        # Jointure Vélo'v + stations
        velov_enriched = (
            enrich_velov_with_stations(
                velov_clean,
                stations_clean,
            )
        )

        # Suppression des lignes
        # sans commune correspondante
        velov_enriched_clean = (
            velov_enriched
            .filter(
                F.col("commune").isNotNull()
            )
        )

        # Création de la tranche 15 minutes
        velov_ready = (
            add_15_min_bucket(
                velov_enriched_clean,
                "horodate",
            )
        )

        # Préparation météo
        meteo_ready = (
            meteo_clean
            .withColumnRenamed(
                "datetime",
                "datetime_15m",
            )
        )

        # Jointure spatio-temporelle
        final_df = (
            velov_ready
            .join(
                meteo_ready,
                on=[
                    "commune",
                    "datetime_15m",
                ],
                how="left",
            )
        )

        # Features analytiques
        final_ready = (
            add_time_features(
                final_df
            )
        )

        print(
            "\n=== DONNEES FINALES ENRICHIES ==="
        )

        final_ready.printSchema()

        final_ready.show(
            5,
            truncate=False,
        )

        # Écriture Silver
        print(
            "\n=== ECRITURE SILVER DANS MINIO ==="
        )

        (
            final_ready
            .write
            .mode("overwrite")
            .partitionBy(
                "year",
                "month",
            )
            .parquet(
                SILVER_PATH
            )
        )

        print(
            "Écriture Silver terminée."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()


# lancer le job avec la commande suivante dans le terminal : (copy/paste dans le terminal)

    """
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0,org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/transform_sources.py
    
    """
    # puis pour vérifier dans Minio:
    """
    docker exec minio mc ls --recursive local/datalake/silver
    """

  # pour vérifié dans MINIO que les fichiers Parquet ont bien été écrits,
  # on peut utiliser la commande suivante dans le terminal :

    """
    docker exec minio mc ls local/datalake/silver/velov_weather
    """  

  # Et pour vérifier le partitionnement :

    """
    docker exec minio mc ls local/datalake/silver/velov_weather/year=2023
    """

    """
    - Nombre total de lignes Vélo'v : 45 966 665
    - Nombre de lignes sans station : 521 937
    - Pourcentage sans station : 1.14%
    Donc 98,86 % des observations trouvent bien une station correspondante. 
    La jointure: station_id = idstation est donc valide pour l`immense majorité des données.

    Les station_id sans correspondance sont :
    1
    201
    202
    555
    2039
    3033
    3035
    5050
    10049
    10079
    10122
    """

# logique de transformation

"""
MongoDB
│
├── velov_availabilities
│        ↓
│   nettoyage
│        ↓
│   horodate → timestamp
│
├── velov_stations
│        ↓
│   nettoyage référentiel
│
│
│   JOIN station_id = idstation
│        ↓
│   Vélo'v enrichi
│        ↓
│   suppression commune NULL
│        ↓
│   horodate → tranche 15 min
│
└── lyon_meteo
         ↓
    nettoyage
         ↓
    datetime → datetime_15m

             ↓

        JOIN :
        commune + datetime_15m

             ↓
création des features (year, month, day, hour, day_of_week, is_weekend, availability_rate)
             ↓
         final_df
            ↓
         MinIO Silver          

"""
"""
les resultats des indicateurs décisionnels fonctionnent:
    Par exemple, pour Albigny-sur-Saône, le 23/02/2023 entre 10h et 11h,
    Spark a regroupé 9 observations et calculé :
    environ 4,44 vélos disponibles, 13,56 places disponibles,
    un taux moyen de disponibilité de 24,69 %, 
    une température moyenne de 8,3 °C, 
    une humidité moyenne de 95,56 %, 
    aucune pluie et un vent moyen d`environ 3,59 km/h.
"""