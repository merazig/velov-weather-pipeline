"""Fonctions utilitaires pour l'écriture des données sur MinIO."""

import os

from pyspark.sql import DataFrame


def configure_minio(spark) -> None:
    """Configure Spark pour accéder à MinIO via le protocole S3A."""
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY")
    secret_key = os.getenv("MINIO_SECRET_KEY")

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.endpoint", endpoint)
    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )


def write_parquet(
    df: DataFrame,
    bucket: str,
    path: str,
) -> None:
    """Écrit un DataFrame au format Parquet dans MinIO."""
    output_path = f"s3a://{bucket}/{path}"

    (df.write.mode("overwrite").parquet(output_path))
