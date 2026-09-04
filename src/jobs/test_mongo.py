"""Test des transformations PySpark."""

import os

from src.utils.mongo import read_mongo_collection
from src.utils.spark_session import get_spark_session
from pyspark.sql.functions import col


import time

from src.transformations.clean import (
    remove_velov_duplicates,
    remove_weather_duplicates,
    remove_station_duplicates,
    remove_invalid_velov_rows,
    remove_invalid_weather_rows,
)

from src.transformations.typing import (
    cast_velov_types,
    cast_weather_types,
)

from src.transformations.aggregations import (
    aggregate_station_activity,
    calculate_station_usage,
    filter_open_velov,
)


from src.transformations.features import add_time_features


def main():
    """Teste les transformations sur une semaine de données."""
    spark = get_spark_session()

    database = os.getenv("MONGO_DATABASE", "velov_weather")

    # =========================================================
    # VÉLO'V
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

    print("=== Lecture MongoDB ===")

    start = time.time()

    velov = read_mongo_collection(
        spark,
        database,
        "velov_availabilities",
        velov_pipeline,
    )

    print("Partitions :", velov.rdd.getNumPartitions())

    velov = cast_velov_types(velov)
    print("Après typing :", type(velov))

    velov = remove_velov_duplicates(velov)
    print("Après clean duplicates :", type(velov))

    velov = remove_invalid_velov_rows(velov)
    print("Après clean invalid :", type(velov))

    velov = add_time_features(velov)
    print("Après features :", type(velov))

    print("=== Calcul utilisation station ===")

    velov = calculate_station_usage(velov)

    velov.select(
        "station_id",
        "horodate",
        "status",
        "bikes_available",
        "previous_bikes_available",
        "station_usage",
        "time_15min",
    ).orderBy(
        "station_id",
        "horodate",
    ).show(30, truncate=False)

    velov_open = filter_open_velov(velov)

    print("=== Stations ouvertes ===")

    velov.filter((col("status") == "OPEN") & (col("station_usage") > 0)).select(
        "station_id",
        "horodate",
        "status",
        "bikes_available",
        "previous_bikes_available",
        "station_usage",
        "time_15min",
    ).orderBy(
        "station_id",
        "horodate",
    ).show(50, truncate=False)

    activity = aggregate_station_activity(velov_open)

    print("=== Activité par station / 15 minutes ===")

    activity = aggregate_station_activity(velov)

    activity.filter(col("station_id") == 555).orderBy("time_15min").show(50, truncate=False)

    start_count = time.time()
    count = velov.count()

    print(f"Documents : {count:,}")
    print(f"Temps count : {time.time() - start_count:.2f}s")
    print(f"Temps total : {time.time() - start:.2f}s")
    print("=== Lecture des données météo ===")

    weather_pipeline = """
    [
        {
            "$match": {
                "datetime": {
                    "$gte": ISODate("2022-12-31T23:00:00Z"),
                    "$lt": ISODate("2023-01-31T23:00:00Z")
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

    print("Partitions météo :", weather.rdd.getNumPartitions())

    weather = cast_weather_types(weather)
    weather = remove_weather_duplicates(weather)
    weather = remove_invalid_weather_rows(weather)

    print("Après features météo :", type(weather))

    weather.select(
        "commune",
        "datetime",
        "temperature_2m_c",
        "relative_humidity_2m_pct",
    ).orderBy(
        "commune",
        "datetime",
    ).show(20, truncate=False)

    start_weather_count = time.time()
    weather_count = weather.count()

    print(f"Documents météo : {weather_count:,}")
    print(f"Temps count météo : {time.time() - start_weather_count:.2f}s")
    print(f"Temps total météo : {time.time() - start_weather:.2f}s")
    spark.stop()


if __name__ == "__main__":
    main()
