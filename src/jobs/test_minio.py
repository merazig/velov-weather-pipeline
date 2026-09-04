"""Teste l'écriture et la lecture Parquet dans MinIO."""

import os

from utils.spark_session import get_spark_session


def main():
    """Teste la connexion Spark vers MinIO."""
    spark = get_spark_session("TestMinIO")

    endpoint = os.getenv(
        "MINIO_ENDPOINT",
        "http://minio:9000",
    )
    access_key = os.getenv(
        "MINIO_ACCESS_KEY",
    )
    secret_key = os.getenv(
        "MINIO_SECRET_KEY",
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint,
    )
    hadoop_conf.set(
        "fs.s3a.access.key",
        access_key,
    )
    hadoop_conf.set(
        "fs.s3a.secret.key",
        secret_key,
    )
    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    try:
        data = [
            (1, "Lyon"),
            (2, "Villeurbanne"),
            (3, "Bron"),
        ]

        df = spark.createDataFrame(
            data,
            [
                "id",
                "commune",
            ],
        )

        output_path = "s3a://datalake/test/spark_parquet"

        print("\n=== ECRITURE MINIO ===")

        (df.write.mode("overwrite").parquet(output_path))

        print(
            "Écriture terminée :",
            output_path,
        )

        print("\n=== LECTURE MINIO ===")

        df_read = spark.read.parquet(output_path)

        df_read.show(
            truncate=False,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

    """
Lancer le job Spark avec la commande suivante :


docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages `
  org.mongodb.spark:mongo-spark-connector_2.13:11.1.0,`
  org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/test_minio.py

    
    """
