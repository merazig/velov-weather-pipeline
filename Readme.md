# Pipeline Data Engineering — Vélo'v & Météo
## 1. Présentation

Ce projet met en place un pipeline Data Engineering permettant de collecter, stocker, transformer et exporter des données historiques de disponibilité des stations Vélo'v ainsi que des données météorologiques des communes de la métropole de Lyon.

L'architecture repose sur :

Python pour la collecte des données ;
MongoDB pour le stockage des données brutes ;
Apache Spark pour le traitement et les transformations ;
MinIO pour le stockage des données transformées au format Parquet ;
Docker / Docker Compose pour l'orchestration des différents services.

Le pipeline traite plusieurs années de données historiques.

## 2. Architecture
```
                         APIs externes
                       /              \
                      /                \
             API Vélo'v              API météo
                  |                       |
                  v                       v
             Extraction Python       Extraction Python
                  |                       |
                  +----------+------------+
                             |
                             v
                        MongoDB
                    données historiques
                             |
                             v
                    Apache Spark
                             |
            +----------------+----------------+
            |                |                |
        Nettoyage          Typage       Transformations
            |                |                |
            +----------------+----------------+
                             |
                             v
                    Agrégations Vélo'v
                             |
                             v
                  Jointure avec météo
                             |
                             v
                       DataFrame final
                             |
                             v
                    Parquet partitionné
                             |
                             v
                         MinIO
```

## 3. Technologies utilisées
| Technologie   | Utilisation                         |
|---------------|-------------------------------------|
| Python        | Extraction et ingestion             |
| Requests      | Appels aux APIs                     |
| MongoDB       | Stockage des données brutes        |
| PyMongo       | Communication avec MongoDB         |
| Apache Spark  | Transformation et agrégation       |
| PySpark       | API Python de Spark                 |
| MinIO         | Stockage objet compatible S3        |
| Parquet       | Format de stockage analytique       |
| Docker        | Conteneurisation                    |
| Docker Compose| Orchestration                       |

## 4. Données collectées
### Vélo'v

Les données historiques Vélo'v contiennent notamment :

- l'identifiant de la station ;
- la date et l'heure de l'observation ;
- le statut de la station ;
- la capacité ;
- le nombre de vélos disponibles ;
- le nombre de places disponibles.

Les données sont récupérées depuis l'API historique Vélo'v.

### Stations

Les informations concernant les stations permettent notamment de rattacher une station à une commune.

### Météo

Les données météorologiques sont collectées pour Lyon et les communes voisines.

Les variables collectées comprennent notamment :

- température ;
- température ressentie ;
- humidité ;
- précipitations ;
- pluie ;
- neige ;
- code météo ;
- vitesse du vent ;
- rafales ;
- visibilité ;
- indicateur jour/nuit.
## 5. Extraction

L'extraction historique est réalisée semaine par semaine.

Cette approche permet de limiter la quantité de données récupérée à chaque appel et de gérer la pagination de l'API Vélo'v.

Les données sont ensuite insérées dans MongoDB dans les collections :

`velov_stations`
`velov_availabilities`
`lyon_meteo`


Le pipeline est conçu pour pouvoir reprendre l'extraction à partir de la dernière date disponible dans MongoDB.

## 6. Lancement du projet
### Prérequis

Le projet nécessite :

- Docker ;
- Docker Compose ;
- un fichier .env.

Les identifiants et paramètres de connexion sont configurés via les variables d'environnement.

## 7. Démarrage avec Docker Compose

Pour construire les images et démarrer les services :
```Bash
docker compose -f docker/docker-compose.yml --env-file .env up -d --build
```

Les principaux services utilisés sont notamment :

- MongoDB
- MinIO
- Spark Master
- Spark Worker
- Spark Driver
- Extraction


L'état des services peut être vérifié avec :
```Bash
docker compose -f docker/docker-compose.yml --env-file .env ps
```
## 8. Lancer / suivre l'extraction

Le service d'extraction peut être suivi avec :
```bash
docker compose -f docker/docker-compose.yml --env-file .env logs -f extraction
```

L'extraction récupère les données historiques et les insère dans MongoDB.

## 9. Transformation avec Apache Spark

Le traitement des données est réalisé par le job :

`src/jobs/process_velov.py`


Le job Spark peut être lancé avec :
```bash
docker exec -it spark-driver bash -c "/opt/spark/bin/spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0 --conf spark.jars.ivy=/tmp/.ivy2 --master spark://spark-master:7077 /app/src/jobs/process_velov.py"
```

Le job réalise les principales étapes suivantes :

- lecture des données Vélo'v depuis MongoDB ;
- suppression des doublons ;
- suppression des lignes invalides ;
- conversion des types ;
- ajout des variables temporelles ;
- calcul des indicateurs d'utilisation des stations ;
- lecture des stations ;
- jointure avec les stations ;
- filtrage des stations ouvertes ;
- agrégation de l'activité par commune et par tranche de 15 minutes ;
- lecture des données météo ;
- nettoyage et typage des données météo ;
- jointure activité Vélo'v / météo ;
- export au format Parquet vers MinIO.
## 10. Stockage dans MinIO

Les données transformées sont stockées dans le bucket :

`velov`


Le job produit une structure partitionnée par année et par mois :
```
velov/
└── data/
    └── year=2023/
        └── month=01/
            ├── part-....parquet
            ├── part-....parquet
            └── ...

```
Le format Parquet est utilisé afin de disposer d'un format adapté aux traitements analytiques et à Spark.

## 11. Optimisation des performances MongoDB

Le volume de données Vélo'v est important : la collection velov_availabilities contient environ **177 millions** de documents.

Une première lecture d'une semaine de données nécessitait environ :

**250 secondes**


La création d'un index sur le champ horodate a considérablement amélioré les performances de lecture.
```mongodb
db.velov_availabilities.createIndex({
    horodate: 1
})
```

Après création de l'index, la lecture d'une semaine de données est passée à environ :

**2 secondes**


Soit une accélération d'environ **×125**.

Cette optimisation est particulièrement importante pour le pipeline car les données sont récupérées par périodes temporelles.

La requête utilisée par le traitement exploite une plage de dates :
```
{
    "$match": {
        "horodate": {
            "$gte": "...",
            "$lt": "..."
        }
    }
}
```

L'index permet ainsi à MongoDB de rechercher directement la période concernée au lieu de parcourir l'ensemble des documents.

## 12. Organisation du projet

Une organisation simplifiée du projet est la suivante :
```
.
├── docker/
│   └── docker-compose.yml
│
├── src/
│   ├── extraction/
│   │   ├── collect_velov.py
│   │   ├── collect_meteo.py
│   │   └── load_mongo.py
│   │
│   ├── jobs/
│   │   └── process_velov.py
│   │
│   ├── transformations/
│   │   ├── clean.py
│   │   ├── typing.py
│   │   ├── features.py
│   │   └── aggregations.py
│   │
│   └── utils/
│       ├── mongo.py
│       ├── minio.py
│       └── spark_session.py
│
├── .env
├── .gitignore
└── README.md
```
## 13. Reprise de l'extraction

L'extraction utilise la dernière date disponible dans MongoDB afin de déterminer à partir de quelle période poursuivre la collecte.

Lorsqu'aucune donnée de disponibilité n'est présente, les stations sont d'abord récupérées.

Ensuite, l'historique est parcouru semaine par semaine jusqu'à la date actuelle.

Cette stratégie permet notamment de reprendre une extraction interrompue sans recommencer systématiquement tout l'historique.

## 14. Transformations

Les transformations sont organisées par responsabilité.

### Nettoyage

Les données sont nettoyées afin de :

- supprimer les doublons ;
- supprimer les lignes Vélo'v sans informations essentielles ;
- supprimer les lignes météo invalides ;
- supprimer les doublons de stations.
### Typage

Les champs temporels et numériques sont convertis vers les types Spark appropriés.

### Features temporelles

Des informations temporelles sont ajoutées aux données Vélo'v afin de faciliter les analyses.

### Agrégation

L'activité Vélo'v est agrégée par commune et par intervalle de `15 minutes`.

Les données météo sont ensuite jointes à cette activité afin de produire le jeu de données final.

## 15. Points forts du projet
- Architecture entièrement conteneurisée avec Docker Compose.
- Séparation entre extraction, stockage et transformation.
- Stockage des données brutes dans MongoDB.
- Traitement distribué avec Apache Spark.
- Stockage analytique au format Parquet dans MinIO.
- Partitionnement des données Parquet par année et par mois.
- Extraction historique réalisée par périodes afin de limiter la charge.
- Gestion de la pagination de l'API Vélo'v.
- Reprise de l'extraction à partir de la dernière date disponible.
- Optimisation MongoDB adaptée à un volume d'environ 177 millions de documents.
- Réduction du temps de lecture d'une semaine d'environ 250 secondes à 2 secondes grâce à l'index horodate.
## 16. Améliorations possibles

Plusieurs améliorations pourraient être apportées :

- rendre l'année et le mois du job Spark configurables plutôt que définis directement dans le code ;
- automatiser davantage l'exécution du pipeline ;
- ajouter des tests automatisés sur les transformations ;
- ajouter des contrôles de qualité des données ;
- améliorer la gestion des erreurs et des reprises ;
- mettre en place des métriques de suivi du pipeline ;
- optimiser la gestion du nombre de fichiers Parquet produits par Spark ;
- documenter davantage les performances du pipeline complet.
## 17. Résumé du pipeline
```
API Vélo'v
     +
API météo
     ↓
Extraction Python
     ↓
MongoDB
     ↓
Filtrage temporel optimisé par index
     ↓
Apache Spark
     ↓
Nettoyage + typage
     ↓
Features + agrégations
     ↓
Jointure Vélo'v / stations / météo
     ↓
Parquet
     ↓
MinIO
```

Le projet permet ainsi de construire une chaîne complète de traitement de données historiques Vélo'v et météo, depuis la collecte jusqu'au stockage analytique, tout en prenant en compte les contraintes liées à un volume important de données.