"""Nettoie, prépare et joint les sources MongoDB avec PySpark."""

from pyspark.sql import functions as F

from utils.spark_session import get_spark_session


def read_collection(spark, collection_name):
    """Lit une collection MongoDB avec Spark."""
    return (
        spark.read
        .format("mongodb")
        .option("collection", collection_name)
        .load()
    )


def transform_velov(df):
    """Nettoie les disponibilités Vélo'v."""
    return (
        df
        .dropDuplicates()
        .dropna(
            subset=[
                "station_id",
                "horodate",
            ]
        )
        .withColumn(
            "horodate",
            F.to_timestamp("horodate"),
        )
        .select(
            "station_id",
            "horodate",
            "bikes_available",
            "stands_available",
            "capacity",
            "status",
        )
    )


def transform_stations(df):
    """Nettoie le référentiel des stations."""
    return (
        df
        .dropDuplicates(["idstation"])
        .dropna(
            subset=[
                "idstation",
                "commune",
                "lat",
                "lon",
            ]
        )
        .select(
            "idstation",
            "nom",
            "commune",
            "lat",
            "lon",
            "nbbornettes",
            "ouverte",
        )
    )


def transform_meteo(df):
    """Nettoie les données météo."""
    return (
        df
        .dropDuplicates()
        .dropna(
            subset=[
                "commune",
                "datetime",
            ]
        )
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
            stations_df["idstation"],
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

    return df.withColumn(
        "datetime_15m",
        F.to_timestamp(
            F.from_unixtime(
                bucket_seconds
            )
        ),
    )


def main():
    """Lit, transforme et joint les sources."""

    spark = get_spark_session(
        "TransformSources"
    )

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

        # Enrichissement Vélo'v avec les stations
        velov_enriched = (
            enrich_velov_with_stations(
                velov_clean,
                stations_clean,
            )
        )

        # On garde seulement les lignes
        # ayant une station / commune connue
        velov_enriched_clean = (
            velov_enriched
            .filter(
                F.col("commune").isNotNull()
            )
        )

        # Création de la tranche de 15 minutes
        velov_ready = add_15_min_bucket(
            velov_enriched_clean,
            "horodate",
        )

        # La météo est déjà disponible
        # toutes les 15 minutes
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

        print("\n=== VELOV READY ===")
        velov_ready.printSchema()
        velov_ready.show(
            5,
            truncate=False,
        )

        print("\n=== METEO READY ===")
        meteo_ready.printSchema()
        meteo_ready.show(
            5,
            truncate=False,
        )

        print("\n=== DONNEES FINALES ===")
        final_df.printSchema()
        final_df.show(
            5,
            truncate=False,
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
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 `
  /app/src/jobs/transform_sources.py
    
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

         final_df

"""