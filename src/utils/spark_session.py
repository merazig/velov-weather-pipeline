"""Utilitaires de création et configuration de SparkSession."""

import os

from pyspark.sql import SparkSession


def get_spark_session(app_name):
    """Crée et configure une SparkSession pour MongoDB."""
    host = os.getenv("MONGO_HOST", "mongodb")
    port = os.getenv("MONGO_PORT", "27017")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database = os.getenv("MONGO_DATABASE")

    mongo_uri = f"mongodb://{username}:{password}@{host}:{port}/{database}?authSource=admin"

    spark = (
        SparkSession.builder.appName(app_name)
        .master("spark://spark-master:7077")
        .config(
            "spark.mongodb.read.connection.uri",
            mongo_uri,
        )
        .config(
            "spark.mongodb.read.database",
            database,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# lancer le script avec la commande suivante dans le terminal de votre machine hôte
"""
docker exec -e PYTHONPATH=/app/src -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 `
  /app/src/jobs/read_sources.py

  """
