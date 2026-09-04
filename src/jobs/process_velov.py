"""Job principal de transformation et d'export Vélo'v + météo."""

import os

from src.utils.mongo import read_mongo_collection
from src.utils.spark_session import get_spark_session
from src.utils.minio import write_parquet, configure_minio

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

from src.transformations.aggregations import (
    calculate_station_usage,
    filter_open_velov,
    aggregate_by_commune,
)


def main():
    """Transforme et exporte les données d'un mois."""
    spark = get_spark_session()

    database = os.getenv("MONGO_DATABASE", "velov_weather")

    # =========================================================
    # PÉRIODE
    # =========================================================

    year = 2025
    month = 2
    next_month = 2
    # =========================================================
    # VÉLO'V
    # =========================================================

    velov_pipeline = f"""
    [
        {{
            "$match": {{
                "horodate": {{
                    "$gte": "{year:04d}-{month:02d}-01 00:00:00+01:00",
                    "$lt": "{year:04d}-{next_month:02d}-07 00:00:00+01:00"
                }}
            }}
        }}
    ]
    """

    velov = read_mongo_collection(
        spark,
        database,
        "velov_availabilities",
        velov_pipeline,
    )

    velov = remove_velov_duplicates(velov)

    velov = remove_invalid_velov_rows(velov)

    velov = cast_velov_types(velov)

    velov = add_time_features(velov)

    velov = calculate_station_usage(velov)

    # =========================================================
    # STATIONS
    # =========================================================

    station_pipeline = """
    [
        {
            "$project": {
                "_id": 0,
                "idstation": 1,
                "commune": 1
            }
        }
    ]
    """

    stations = read_mongo_collection(
        spark,
        database,
        "velov_stations",
        station_pipeline,
    )

    stations = remove_station_duplicates(stations)

    stations = stations.select(
        "idstation",
        "commune",
    )

    # =========================================================
    # JOINTURE VÉLO'V / STATIONS
    # =========================================================

    velov_commune = velov.join(
        stations,
        velov.station_id == stations.idstation,
        "inner",
    ).drop("idstation")

    # =========================================================
    # STATIONS OUVERTES
    # =========================================================

    velov_open = filter_open_velov(velov_commune)

    # =========================================================
    # AGRÉGATION PAR COMMUNE / 15 MINUTES
    # =========================================================

    activity = aggregate_by_commune(velov_open)

    # =========================================================
    # MÉTÉO
    # =========================================================

    weather_pipeline = f"""
    [
        {{
            "$match": {{
                "datetime": {{
                    "$gte": ISODate("{year:04d}-{month:02d}-01T00:00:00Z"),
                    "$lt": ISODate("{year:04d}-{next_month:02d}-07T00:00:00Z")
                }}
            }}
        }}
    ]
    """

    weather = read_mongo_collection(
        spark,
        database,
        "lyon_meteo",
        weather_pipeline,
    )

    weather = cast_weather_types(weather)

    weather = remove_weather_duplicates(weather)

    weather = remove_invalid_weather_rows(weather)

    weather = weather.select(
        "commune",
        "datetime",
        "temperature_2m_c",
        "relative_humidity_2m_pct",
        "apparent_temperature_c",
        "precipitation_mm",
        "rain_mm",
        "snowfall_cm",
        "weather_code",
        "wind_speed_10m_kmh",
        "wind_gusts_10m_kmh",
        "is_day",
        "visibility_m",
    )

    # =========================================================
    # JOINTURE ACTIVITÉ / MÉTÉO
    # =========================================================

    final_df = (
        activity.join(
            weather,
            ((activity.commune == weather.commune) & (activity.time_15min == weather.datetime)),
            "inner",
        )
        .drop(weather.commune)
        .drop("datetime")
    )

    # =========================================================
    # EXPORT PARQUET
    # =========================================================
    configure_minio(spark)

    write_parquet(
        final_df,
        bucket="velov",
        path=f"data/year={year}/month={month:02d}/",
    )

    spark.stop()


if __name__ == "__main__":
    main()
