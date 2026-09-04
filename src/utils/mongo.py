"""Fonctions de lecture des collections MongoDB."""

import os

from pyspark.sql import DataFrame, SparkSession


def read_mongo_collection(
    spark: SparkSession,
    database: str,
    collection: str,
    pipeline: str | None = None,
) -> DataFrame:
    """Lit une collection MongoDB avec Spark."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")

    uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource=admin"

    reader = (
        spark.read.format("mongodb")
        .option("spark.mongodb.read.connection.uri", uri)
        .option("spark.mongodb.read.database", database)
        .option("spark.mongodb.read.collection", collection)
        .option(
            "spark.mongodb.read.partitioner",
            "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner",
        )
    )

    if pipeline is not None:
        reader = reader.option(
            "spark.mongodb.read.aggregation.pipeline",
            pipeline,
        )

    return reader.load()
