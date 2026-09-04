"""Test des jointures et agrégations PySpark."""

import os
import time

from src.transformations.aggregations import (
    aggregate_by_commune,
    calculate_station_usage,
    filter_open_velov,
)
from src.transformations.clean import (
    remove_invalid_velov_rows,
    remove_velov_duplicates,
    remove_invalid_weather_rows,
    remove_station_duplicates,
    remove_weather_duplicates,
)
from src.transformations.features import add_time_features
from src.transformations.joins import join_velov_stations, join_velov_weather
from src.transformations.typing import cast_velov_types
from src.utils.mongo import read_mongo_collection
from src.utils.spark_session import get_spark_session
from src.utils.minio import configure_minio, write_parquet


def main():
    """Teste la jointure Vélo'v/stations et l'agrégation par commune."""
    spark = get_spark_session()

    database = os.getenv("MONGO_DATABASE", "velov_weather")

    # =========================================================
    # LECTURE VÉLO'V
    # =========================================================

    print("=== Lecture des disponibilités Vélo'v ===")

    velov_pipeline = """
    [
        {
            "$match": {
                "horodate": {
                    "$gte": "2023-01-01 00:00:00+01:00",
                    "$lt": "2023-01-08 00:00:00+01:00"
                }
            }
        }
    ]
    """

    start = time.time()

    velov = read_mongo_collection(
        spark,
        database,
        "velov_availabilities",
        velov_pipeline,
    )

    # =========================================================
    # TRANSFORMATIONS VÉLO'V
    # =========================================================

    velov = cast_velov_types(velov)
    velov = remove_velov_duplicates(velov)
    velov = remove_invalid_velov_rows(velov)
    velov = add_time_features(velov)

    velov = calculate_station_usage(velov)
    velov = filter_open_velov(velov)

    # =========================================================
    # LECTURE DES STATIONS
    # =========================================================

    print("=== Lecture des stations ===")

    stations = read_mongo_collection(
        spark,
        database,
        "velov_stations",
    )

    stations = remove_station_duplicates(stations)

    # =========================================================
    # JOINTURE VÉLO'V / STATIONS
    # =========================================================

    print("=== Jointure Vélo'v / stations ===")

    print(f"Avant jointure : {velov.count():,} lignes")

    velov_stations = join_velov_stations(
        velov,
        stations,
    )

    print(f"Après jointure stations : {velov_stations.count():,} lignes")

    velov_stations.select(
        "station_id",
        "commune",
        "time_15min",
        "station_usage",
    ).show(20, truncate=False)

    print("=== Agrégation par commune / 15 minutes ===")

    activity = aggregate_by_commune(
        velov_stations,
    )

    print(f"Après agrégation commune : {activity.count():,} lignes")

    activity.orderBy(
        "commune",
        "time_15min",
    ).show(20, truncate=False)

    print("=== Jointure Vélo'v / météo ===")

    print(f"Avant jointure météo : {activity.count():,} lignes")

    # =========================================================
    # LECTURE MÉTÉO
    # =========================================================

    print("=== Lecture des données météo ===")

    weather_pipeline = """
    [
    {
    "$match": {
    "datetime": {
    "$gte": ISODate("2022-12-31T23:00:00Z"),
    "$lt": ISODate("2023-01-08T23:00:00Z")
    }
    }
    }
    ]
    """

    start_weather = time.time()

    weather = read_mongo_collection(
        spark,
        database,
        "lyon_meteo",
        weather_pipeline,
    )

    print(
        "Partitions météo :",
        weather.rdd.getNumPartitions(),
    )

    weather = remove_weather_duplicates(weather)
    weather = remove_invalid_weather_rows(weather)

    weather_count = weather.count()

    print(f"Documents météo : {weather_count:,}")

    print(f"Temps lecture météo : {time.time() - start_weather:.2f}s")

    # =========================================================
    # JOINTURE VÉLO'V / MÉTÉO
    # =========================================================

    print("=== Jointure Vélo'v / météo ===")

    activity_count = activity.count()

    print(f"Avant jointure météo : {activity_count:,} lignes")

    final = join_velov_weather(
        activity,
        weather,
    )

    final_count = final.count()

    print(f"Après jointure météo : {final_count:,} lignes")

    print(f"Lignes perdues : {activity_count - final_count:,}")

    # =========================================================
    # RÉSULTAT FINAL
    # =========================================================

    print("=== Résultat final ===")

    final.orderBy(
        "commune",
        "time_15min",
    ).show(50, truncate=False)

    print(f"Temps total : {time.time() - start:.2f}s")

    spark = get_spark_session()

    configure_minio(spark)

    write_parquet(
        final,
        bucket="velov",
        path="processed/2023/01",
    )

    spark.stop()


if __name__ == "__main__":
    main()
