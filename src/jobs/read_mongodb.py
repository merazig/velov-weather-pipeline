"""Teste la lecture de MongoDB avec PySpark."""

import os

from pyspark.sql import SparkSession


def main():
    """Lit la collection Vélo'v depuis MongoDB."""

    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database = os.getenv("MONGO_DATABASE")

    collection = "velov_availabilities"

    mongo_uri = (
        f"mongodb://{username}:{password}"
        f"@{host}:{port}/{database}"
        "?authSource=admin"
    )

    spark = (
        SparkSession.builder
        .appName("ReadMongoDB")
        .master("spark://spark-master:7077")
        .config(
            "spark.mongodb.read.connection.uri",
            mongo_uri,
        )
        .config(
            "spark.mongodb.read.database",
            database,
        )
        .config(
            "spark.mongodb.read.collection",
            collection,
        )
        .getOrCreate()
    )

# Réduction des logs
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = (
            spark.read
            .format("mongodb")
            .load()
        )

        print("=== SCHÉMA MONGODB ===")
        df.printSchema()

        print("=== 5 PREMIERS DOCUMENTS ===")
        df.show(
            5,
            truncate=False,
        )

        print("=== NOMBRE DE PARTITIONS ===")
        print(df.rdd.getNumPartitions())

    finally:
        spark.stop()


if __name__ == "__main__":
    main()