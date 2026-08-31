"""Collection des données Velov."""

import requests

def get_velov_stations(url):
    """Cette fonction récupère les stations Velov."""
    maxfeatures = 100
    start = 1

    stations = []

    while True:
        params = {
            "maxfeatures": maxfeatures,
            "start": start
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        values = data.get("values", [])

        # Plus de données → on arrête
        if not values:
            break

        stations.extend(values)
        # On avance de la taille réellement récupérée
        start += len(values)
        
    return stations

def get_velov_availabilities(url):
    """Cette fonction récupère les stations Velov."""
    maxfeatures = 100000
    start = 1

    availabilities = []

    while start < 100000:
        params = {
            "maxfeatures": maxfeatures,
            "start": start
        }

        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        values = data.get("values", [])

        # Plus de données → on arrête
        if not values:
            break

        availabilities.extend(values)
        # On avance de la taille réellement récupérée
        start += len(values)
        
    return availabilities

print(len(get_velov_availabilities("https://data.grandlyon.com/fr/datapusher/ws/timeseries/jcd_jcdecaux.historiquevelov/all.json")))