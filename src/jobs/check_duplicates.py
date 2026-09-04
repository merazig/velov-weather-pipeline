"""Contrôle les doublons dans la zone Silver."""

from pyspark.sql import functions as F

from utils.minio_config import configure_minio
from utils.spark_session import get_spark_session


SILVER_PATH = "s3a://datalake/silver/velov_weather"


def main():
    """Vérifie les doublons dans les données Silver."""
    spark = get_spark_session("CheckSilverDuplicates")

    configure_minio(spark)

    try:
        silver_df = spark.read.parquet(SILVER_PATH)

        print("\n=== DOUBLONS SUR station_id + horodate ===")

        duplicate_keys = (
            silver_df.groupBy(
                "station_id",
                "horodate",
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_keys.show(
            20,
            truncate=False,
        )

        duplicate_key_count = duplicate_keys.count()

        print(
            "Nombre de clés station_id + horodate en doublon :",
            duplicate_key_count,
        )

        print("\n=== DOUBLONS EXACTS ===")

        duplicate_rows = silver_df.groupBy(silver_df.columns).count().filter(F.col("count") > 1)

        duplicate_rows.show(
            20,
            truncate=False,
        )

        duplicate_row_count = duplicate_rows.count()

        print(
            "Nombre de groupes de doublons exacts :",
            duplicate_row_count,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

"""executer le job Spark avec la commande suivante :

docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/check_duplicates.py
  
  """
