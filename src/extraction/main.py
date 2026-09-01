"""Collect, load et insert data."""

from src.extraction.collect_velov import (
    get_velov_availabilities,
)


def main():
    """Fonction main."""
    maxfeatures = 250000
    start = 1
    url_availabilities = "https://data.grandlyon.com/fr/datapusher/ws/timeseries/jcd_jcdecaux.historiquevelov/all.json"
    while start < 200000000:
        get_velov_availabilities(url_availabilities, maxfeatures, start)

        start += maxfeatures


if __name__ == "__main__":
    main()
