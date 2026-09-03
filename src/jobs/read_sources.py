"""Lit les différentes collections MongoDB avec PySpark."""

from utils.spark_session import get_spark_session


def read_collection(spark, collection_name):
    """Lit une collection MongoDB avec Spark."""
    return (
        spark.read
        .format("mongodb")
        .option("collection", collection_name)
        .load()
    )


def show_source(dataframe, source_name):
    """Affiche le schéma, quelques lignes et les partitions."""
    print(f"\n=== {source_name} - SCHÉMA ===")
    dataframe.printSchema()

    print(f"\n=== {source_name} - 5 PREMIÈRES LIGNES ===")
    dataframe.show(
        5,
        truncate=False,
    )

    print(
        f"Partitions {source_name} : "
        f"{dataframe.rdd.getNumPartitions()}"
    )


def main():
    """Lit et explore les trois collections MongoDB."""
    spark = get_spark_session(
        "ReadMongoSources"
    )

    try:
        velov_df = read_collection(
            spark,
            "velov_availabilities",
        )

        stations_df = read_collection(
            spark,
            "velov_stations",
        )

        meteo_df = read_collection(
            spark,
            "lyon_meteo",
        )

        show_source(
            velov_df,
            "VELOV AVAILABILITIES",
        )

        show_source(
            stations_df,
            "VELOV STATIONS",
        )

        show_source(
            meteo_df,
            "METEO",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

# pour lancer le script, utilisez la commande suivante dans le terminal de votre machine hôte 
# (assurez-vous que les conteneurs Docker sont en cours d'exécution) :
    
    """
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 `
  /app/src/jobs/read_sources.py
    
    """