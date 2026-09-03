"""Mongo."""

import os

from pyspark.sql import DataFrame, SparkSession


def read_mongo_collection(
    spark: SparkSession,
    database: str,
    collection: str,
) -> DataFrame:
    """Lit une collection MongoDB avec Spark."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")

    uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"

    return (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", uri)
        .option("spark.mongodb.read.database", database)
        .option("spark.mongodb.read.collection", collection)
        .load()
    )
