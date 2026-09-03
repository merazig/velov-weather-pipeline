"""Test des transformations PySpark."""

import os

from src.utils.mongo import read_mongo_collection
from src.utils.spark_session import get_spark_session

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

    velov.explain(True)

    start_count = time.time()
    count = velov.count()

    print(f"Documents : {count:,}")
    print(f"Temps count : {time.time() - start_count:.2f}s")
    print(f"Temps total : {time.time() - start:.2f}s")


    spark.stop()


if __name__ == "__main__":
    main()
