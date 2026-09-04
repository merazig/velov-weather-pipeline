"""Fonctions de jointure des données Vélo'v."""

from pyspark.sql import DataFrame


def join_velov_stations(
    velov: DataFrame,
    stations: DataFrame,
) -> DataFrame:
    """Associe les disponibilités Vélo'v à leur commune."""
    stations_ref = stations.select(
        "idstation",
        "commune",
    )

    return velov.join(
        stations_ref,
        velov.station_id == stations_ref.idstation,
        "inner",
    ).select(
        velov.station_id,
        velov.time_15min,
        velov.station_usage,
        stations_ref.commune,
        velov.jour_semaine,
        velov.week_end,
    )


def join_velov_weather(
    activity: DataFrame,
    weather: DataFrame,
) -> DataFrame:
    """Associe l'activité Vélo'v aux données météo."""
    weather_ref = weather.select(
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

    return activity.join(
        weather_ref,
        ((activity.commune == weather_ref.commune) & (activity.time_15min == weather_ref.datetime)),
        "inner",
    ).select(
        activity.commune,
        activity.time_15min,
        activity.activite,
        activity.nombre_stations_actives,
        weather_ref.temperature_2m_c,
        weather_ref.relative_humidity_2m_pct,
        weather_ref.apparent_temperature_c,
        weather_ref.precipitation_mm,
        weather_ref.rain_mm,
        weather_ref.snowfall_cm,
        weather_ref.weather_code,
        weather_ref.wind_speed_10m_kmh,
        weather_ref.wind_gusts_10m_kmh,
        weather_ref.is_day,
        weather_ref.visibility_m,
    )
