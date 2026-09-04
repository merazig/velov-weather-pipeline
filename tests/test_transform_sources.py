"""Tests unitaires des transformations PySpark."""

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from jobs.transform_sources import (
    add_15_min_bucket,
    add_time_features,
    transform_meteo,
    transform_stations,
    transform_velov,
)


@pytest.fixture(scope="session")
def spark():
    """Crée une SparkSession locale pour les tests."""

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("PySparkTests")
        .getOrCreate()
    )

    session.sparkContext.setLogLevel("WARN")

    yield session

    session.stop()


def test_transform_velov(spark):
    """Vérifie le nettoyage et la déduplication des données Vélo'v."""

    schema = StructType(
        [
            StructField("station_id", IntegerType(), True),
            StructField("horodate", StringType(), True),
            StructField("bikes_available", IntegerType(), True),
            StructField("stands_available", IntegerType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("status", StringType(), True),
        ]
    )

    data = [
        (
            1001,
            "2023-01-01 10:05:00",
            10,
            6,
            16,
            "OPEN",
        ),
        (
            1001,
            "2023-01-01 10:05:00",
            10,
            6,
            16,
            "OPEN",
        ),
        (
            None,
            "2023-01-01 10:10:00",
            5,
            11,
            16,
            "OPEN",
        ),
        (
            1002,
            None,
            7,
            34,
            41,
            "OPEN",
        ),
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = transform_velov(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["station_id"] == 1001
    assert rows[0]["bikes_available"] == 10
    assert rows[0]["stands_available"] == 6
    assert rows[0]["capacity"] == 16
    assert rows[0]["status"] == "OPEN"
    assert isinstance(
        rows[0]["horodate"],
        datetime,
    )


def test_transform_stations(spark):
    """Vérifie le nettoyage du référentiel des stations."""

    schema = StructType(
        [
            StructField("idstation", IntegerType(), True),
            StructField("nom", StringType(), True),
            StructField("commune", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("nbbornettes", IntegerType(), True),
            StructField("ouverte", BooleanType(), True),
        ]
    )

    data = [
        (
            1001,
            "Terreaux / Terme",
            "Lyon 1er Arrondissement",
            45.76,
            4.83,
            16,
            True,
        ),
        (
            1001,
            "Terreaux / Terme",
            "Lyon 1er Arrondissement",
            45.76,
            4.83,
            16,
            True,
        ),
        (
            1002,
            "Opéra",
            None,
            45.76,
            4.83,
            41,
            True,
        ),
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = transform_stations(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["idstation"] == 1001
    assert rows[0]["nom"] == "Terreaux / Terme"
    assert rows[0]["commune"] == "Lyon 1er Arrondissement"
    assert rows[0]["nbbornettes"] == 16
    assert rows[0]["ouverte"] is True


def test_transform_meteo(spark):
    """Vérifie le nettoyage et la déduplication météo."""

    schema = StructType(
        [
            StructField("commune", StringType(), True),
            StructField("datetime", StringType(), True),
            StructField("temperature_2m_c", DoubleType(), True),
            StructField(
                "apparent_temperature_c",
                DoubleType(),
                True,
            ),
            StructField(
                "relative_humidity_2m_pct",
                IntegerType(),
                True,
            ),
            StructField(
                "precipitation_mm",
                DoubleType(),
                True,
            ),
            StructField(
                "rain_mm",
                DoubleType(),
                True,
            ),
            StructField(
                "snowfall_cm",
                DoubleType(),
                True,
            ),
            StructField(
                "weather_code",
                IntegerType(),
                True,
            ),
            StructField(
                "wind_speed_10m_kmh",
                DoubleType(),
                True,
            ),
            StructField(
                "wind_gusts_10m_kmh",
                DoubleType(),
                True,
            ),
            StructField(
                "visibility_m",
                DoubleType(),
                True,
            ),
            StructField(
                "is_day",
                BooleanType(),
                True,
            ),
        ]
    )

    data = [
        (
            "Lyon 1er Arrondissement",
            "2023-01-01 10:00:00",
            10.0,
            8.0,
            70,
            0.0,
            0.0,
            0.0,
            0,
            5.0,
            10.0,
            20000.0,
            True,
        ),
        (
            "Lyon 1er Arrondissement",
            "2023-01-01 10:00:00",
            10.0,
            8.0,
            70,
            0.0,
            0.0,
            0.0,
            0,
            5.0,
            10.0,
            20000.0,
            True,
        ),
        (
            None,
            "2023-01-01 10:15:00",
            11.0,
            9.0,
            65,
            0.0,
            0.0,
            0.0,
            1,
            4.0,
            8.0,
            20000.0,
            True,
        ),
    ]

    df = spark.createDataFrame(
        data,
        schema,
    )

    result = transform_meteo(df)

    rows = result.collect()

    assert len(rows) == 1
    assert rows[0]["commune"] == "Lyon 1er Arrondissement"
    assert rows[0]["temperature_2m_c"] == 10.0
    assert rows[0]["relative_humidity_2m_pct"] == 70


def test_add_15_min_bucket(spark):
    """Vérifie l'arrondi des horaires au quart d'heure inférieur."""

    data = [
        (
            1,
            datetime(
                2023,
                1,
                1,
                10,
                3,
                24,
            ),
        ),
        (
            2,
            datetime(
                2023,
                1,
                1,
                10,
                14,
                59,
            ),
        ),
        (
            3,
            datetime(
                2023,
                1,
                1,
                10,
                15,
                0,
            ),
        ),
        (
            4,
            datetime(
                2023,
                1,
                1,
                10,
                29,
                59,
            ),
        ),
        (
            5,
            datetime(
                2023,
                1,
                1,
                10,
                30,
                0,
            ),
        ),
    ]

    df = spark.createDataFrame(
        data,
        [
            "station_id",
            "horodate",
        ],
    )

    result = (
        add_15_min_bucket(
            df,
            "horodate",
        )
        .orderBy("station_id")
        .collect()
    )

    expected = [
        datetime(
            2023,
            1,
            1,
            10,
            0,
            0,
        ),
        datetime(
            2023,
            1,
            1,
            10,
            0,
            0,
        ),
        datetime(
            2023,
            1,
            1,
            10,
            15,
            0,
        ),
        datetime(
            2023,
            1,
            1,
            10,
            15,
            0,
        ),
        datetime(
            2023,
            1,
            1,
            10,
            30,
            0,
        ),
    ]

    actual = [
        row["datetime_15m"]
        for row in result
    ]

    assert actual == expected


def test_add_time_features(spark):
    """Vérifie les variables temporelles et le taux de disponibilité."""

    data = [
        (
            datetime(
                2023,
                1,
                1,
                10,
                30,
                0,
            ),
            8,
            16,
        ),
        (
            datetime(
                2023,
                1,
                2,
                15,
                0,
                0,
            ),
            5,
            10,
        ),
    ]

    df = spark.createDataFrame(
        data,
        [
            "horodate",
            "bikes_available",
            "capacity",
        ],
    )

    result = (
        add_time_features(df)
        .orderBy("horodate")
        .collect()
    )

    sunday = result[0]

    assert sunday["year"] == 2023
    assert sunday["month"] == 1
    assert sunday["day"] == 1
    assert sunday["hour"] == 10
    assert sunday["day_of_week"] == 1
    assert sunday["is_weekend"] is True
    assert sunday["availability_rate"] == pytest.approx(
        0.5
    )

    monday = result[1]

    assert monday["year"] == 2023
    assert monday["month"] == 1
    assert monday["day"] == 2
    assert monday["hour"] == 15
    assert monday["day_of_week"] == 2
    assert monday["is_weekend"] is False
    assert monday["availability_rate"] == pytest.approx(
        0.5
    )


    """Execute: Commande PowerShell pour lancer les tests :

   docker exec -it spark-master sh -c `
  "cd /app && PYTHONPATH=/app/src:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip `
  python3 -m pytest tests/test_transform_sources.py -v -p no:cacheprovider
  
  """