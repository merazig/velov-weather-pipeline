"""Insère les données dans une base mongo."""

import os

from dotenv import load_dotenv
from pymongo import MongoClient


load_dotenv()


def insert_data_to_mongodb(data, collection_name):
    """Insère les données Velov dans MongoDB."""
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database_name = os.getenv("MONGO_DATABASE")

    client = MongoClient(
        f"mongodb://{host}:{port}",
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[database_name]
        collection = db[collection_name]

        if not data:
            return 0

        collection.insert_many(data, ordered=False)

        # print(f"{len(result.inserted_ids)} documents insérés dans MongoDB.")

        return len(data)

    finally:
        client.close()


def get_last_date(collection_name):
    """Retourne la dernière date insérer."""
    """Insère les données Velov dans MongoDB."""
    host = os.getenv("MONGO_HOST")
    port = os.getenv("MONGO_PORT")
    username = os.getenv("MONGO_USERNAME")
    password = os.getenv("MONGO_PASSWORD")
    database_name = os.getenv("MONGO_DATABASE")

    client = MongoClient(
        f"mongodb://{host}:{port}",
        username=username,
        password=password,
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[database_name]
        collection = db[collection_name]
        last_document = collection.find_one({}, {"horodate": 1, "_id": 0}, sort=[("horodate", -1)])

        horodate = last_document["horodate"].split(" ")[0] if last_document else None

        return horodate

    finally:
        client.close()
