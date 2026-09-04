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

    configure_minio(spark)

    test_df = spark.createDataFrame(
        [
            ("Villeurbanne", 10, 25.4),
            ("Lyon", 20, 24.8),
        ],
        ["commune", "activite", "temperature"],
    )

    write_parquet(
        test_df,
        bucket="velov",
        path="test/",
    )

    spark.stop()


if __name__ == "__main__":
    main()
