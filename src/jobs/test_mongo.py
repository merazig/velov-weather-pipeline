"""Test de connexion au cluster Spark."""

import os

from src.utils.mongo import read_mongo_collection
from src.utils.spark_session import get_spark_session

def main():
    """Teste la lecture d'une collection MongoDB avec Spark."""
    spark = get_spark_session()

    database = os.getenv("MONGO_DATABASE", "velov_weather")
    collection = "velov_availabilities"

    print("=== Lecture MongoDB avec Spark ===")

    df = read_mongo_collection(
        spark,
        database,
        collection,
    )

    print("=== Schéma ===")
    df.printSchema()

    print("=== 10 premières lignes ===")
    df.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()