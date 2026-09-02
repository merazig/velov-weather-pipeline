"""Collect, load et insert data."""

import calendar
from datetime import date, timedelta

from src.extraction.collect_velov import (
    get_velov_stations,
    get_velov_availabilities,
)

from src.extraction.collect_meteo import (
    collect_meteo,
)

from src.extraction.load_mongo import (
    insert_data_to_mongodb,
    get_last_date,
)


coordonnees = {
    "Albigny-sur-Saône": (45.874994, 4.833002),
    "Bron": (45.733925, 4.912376),
    "Caluire-et-Cuire": (45.788408, 4.840143),
    "Champagne-au-Mont-d'Or": (45.794851, 4.792018),
    "Chassieu": (45.738192, 4.969525),
    "Collonges-au-Mont-d'Or": (45.826654, 4.843424),
    "Couzon-au-Mont-d'Or": (45.845998, 4.832412),
    "Décines-Charpieu": (45.771873, 4.955767),
    "Fontaines-sur-Saône": (45.833936, 4.847240),
    "La Mulatière": (45.727369, 4.814635),
    "Lyon 1er Arrondissement": (45.768952, 4.830784),
    "Lyon 2e Arrondissement": (45.752383, 4.828261),
    "Lyon 3e Arrondissement": (45.754844, 4.863078),
    "Lyon 4e Arrondissement": (45.777987, 4.825779),
    "Lyon 5e Arrondissement": (45.758478, 4.809764),
    "Lyon 6e Arrondissement": (45.770945, 4.851613),
    "Lyon 7e Arrondissement": (45.741779, 4.840139),
    "Lyon 8e Arrondissement": (45.737008, 4.869250),
    "Lyon 9e Arrondissement": (45.779867, 4.807047),
    "Neuville-sur-Saône": (45.873743, 4.839111),
    "Oullins": (45.717045, 4.806313),
    "Pierre-Bénite": (45.703256, 4.821200),
    "Rillieux-la-Pape": (45.810883, 4.886695),
    "Saint-Cyr-au-Mont-d'Or": (45.799791, 4.819602),
    "Saint-Didier-au-Mont-d'Or": (45.791574, 4.808535),
    "Saint-Fons": (45.709265, 4.857221),
    "Saint-Genis-Laval": (45.699345, 4.799343),
    "Saint-Germain-au-Mont-d'Or": (45.887543, 4.803977),
    "Saint-Priest": (45.710196, 4.916000),
    "Sainte-Foy-lès-Lyon": (45.749565, 4.800896),
    "Tassin-la-Demi-Lune": (45.763256, 4.778610),
    "Vaulx-en-Velin": (45.771718, 4.921055),
    "Villeurbanne": (45.769355, 4.884227),
    "Vénissieux": (45.709458, 4.872295),
    "Écully": (45.775089, 4.778673),
}


def main():
    """Fonction main."""
    # ============================================================
    # STATIONS VELOV
    # ============================================================

    url_stations = (
        "https://data.grandlyon.com/fr/datapusher/ws/"
        "grandlyon/pvo_patrimoine_voirie.pvostationvelov/all.json"
    )

    print("====== Collect stations Velov ======")

    data = get_velov_stations(url_stations)
    result = insert_data_to_mongodb(data, "velov_stations")

    print(f"{result} stations insérées")

    # ============================================================
    # DATE DE DEBUT
    # ============================================================

    derniere_date = get_last_date("velov_availabilities")

    if derniere_date:
        date_debut = date.fromisoformat(derniere_date)
    else:
        date_debut = date(2023, 1, 1)

    date_aujourd_hui = date.today()

    print(f"Date de début : {date_debut}")
    print(f"Date d'aujourd'hui : {date_aujourd_hui}")

    # ============================================================
    # URL VELOV
    # ============================================================

    maxfeatures = 10000

    url_availabilities = (
        "https://data.grandlyon.com/fr/datapusher/ws/"
        "timeseries/jcd_jcdecaux.historiquevelov/all.json"
    )

    # ============================================================
    # RÉCUPÉRATION MOIS PAR MOIS
    # ============================================================

    date_courante = date_debut

    while date_courante <= date_aujourd_hui:

        annee = date_courante.year
        mois = date_courante.month

        # Premier jour du mois
        debut_mois = date(annee, mois, 1)

        # Dernier jour du mois
        fin_mois = date(
            annee,
            mois,
            calendar.monthrange(annee, mois)[1],
        )

        # On ne dépasse pas aujourd'hui
        if fin_mois > date_aujourd_hui:
            fin_mois = date_aujourd_hui

        # Si on est dans le premier mois,
        # on commence à la dernière date connue.
        debut_requete = max(date_courante, debut_mois)

        print()
        print("========================================")
        print(f"Collect Velov : {debut_requete} → {fin_mois}")
        print("========================================")

        # ========================================================
        # PAGINATION POUR LE MOIS
        # ========================================================

        start = 1

        while True:

            data = get_velov_availabilities(
                url_availabilities,
                maxfeatures,
                start,
                str(debut_requete),
                str(fin_mois),
            )

            # Plus de données
            if not data:
                break

            result = insert_data_to_mongodb(
                data,
                "velov_availabilities",
            )

            print(
                f"start={start} | "
                f"{result} disponibilités insérées"
            )

            # Moins de données que la limite :
            # on est arrivé à la fin du mois.
            if len(data) < maxfeatures:
                break

            start += maxfeatures

        # ========================================================
        # MÉTÉO DU MOIS
        # ========================================================

        for commune, coordinates in coordonnees.items():

            print(
                f"Collect météo : {commune} | "
                f"{debut_requete} → {fin_mois}"
            )

            data = collect_meteo(
                commune,
                coordinates,
                str(debut_requete),
                str(fin_mois),
            )

            result = insert_data_to_mongodb(
                data,
                "lyon_meteo",
            )

            print(
                f"{commune} : "
                f"{result} données météo insérées"
            )

        # ========================================================
        # PASSAGE AU MOIS SUIVANT
        # ========================================================

        date_courante = fin_mois + timedelta(days=1)


if __name__ == "__main__":
    main()
