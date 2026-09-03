"""Lit les différentes collections MongoDB avec PySpark."""

from utils.spark_session import get_spark_session


def read_collection(
    spark,
    collection_name,
):
    """Lit une collection MongoDB avec Spark."""
    return (
        spark.read
        .format("mongodb")
        .option(
            "collection",
            collection_name,
        )
        .load()
    )


def show_source(
    dataframe,
    source_name,
):
    """Affiche le schéma, quelques lignes et les partitions."""
    print()
    print("=" * 60)
    print(f"{source_name} - SCHÉMA")
    print("=" * 60)

    dataframe.printSchema()

    print()
    print("=" * 60)
    print(f"{source_name} - 5 PREMIÈRES LIGNES")
    print("=" * 60)

    dataframe.show(
        5,
        truncate=False,
    )

    print()
    print(
        f"Partitions {source_name} :",
        dataframe.rdd.getNumPartitions(),
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

        show_source(
            velov_df,
            "VELOV AVAILABILITIES",
        )

        stations_df = read_collection(
            spark,
            "velov_stations",
        )

        show_source(
            stations_df,
            "VELOV STATIONS",
        )

        meteo_df = read_collection(
            spark,
            "lyon_meteo",
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