"""Configure l'accès de Spark à MinIO."""

import os


def configure_minio(spark):
    """Configure Hadoop S3A pour accéder à MinIO."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    hadoop_conf.set(
        "fs.s3a.endpoint",
        os.getenv(
            "MINIO_ENDPOINT",
            "http://minio:9000",
        ),
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        os.getenv("MINIO_ACCESS_KEY"),
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        os.getenv("MINIO_SECRET_KEY"),
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )
