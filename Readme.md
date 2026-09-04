# Vélo'v Weather Pipeline

Pipeline Data Engineering permettant de collecter, stocker, transformer et analyser les données de disponibilité des stations Vélo'v de la Métropole de Lyon en les enrichissant avec des données météorologiques.

Le projet utilise notamment **MongoDB**, **Apache Spark / PySpark**, **MinIO**, **Docker** et le format **Parquet**.

---

## 1. Objectif du projet

L'objectif est de construire un pipeline de données permettant de croiser :

- les disponibilités historiques des stations Vélo'v ;
- le référentiel des stations Vélo'v ;
- les données météorologiques de Lyon et des communes voisines.

Le pipeline permet ensuite de produire des données préparées et des indicateurs décisionnels permettant notamment d'étudier la disponibilité des vélos et l'influence des conditions météorologiques.

La chaîne de traitement suit les principales étapes suivantes :

```text
Collecte API
    ↓
MongoDB
    ↓
Transformation PySpark
    ↓
MinIO Silver
    ↓
Agrégations PySpark
    ↓
MinIO Gold
```

---

## 2. Sources de données

### Vélo'v

Les données Vélo'v sont récupérées depuis les services Open Data de la Métropole de Lyon.

Deux types de données sont utilisés :

- le référentiel des stations ;
- l'historique des disponibilités Vélo'v.

Les informations utiles conservées pour les disponibilités sont notamment :

- `station_id`
- `horodate`
- `status`
- `capacity`
- `bikes_available`
- `stands_available`

Le référentiel des stations permet notamment de récupérer :

- l'identifiant de la station ;
- le nom ;
- la commune ;
- la latitude ;
- la longitude ;
- le nombre de bornettes ;
- l'état d'ouverture.

### Météo

Les données météorologiques sont collectées depuis **Open-Meteo**.

La collecte est effectuée avec une granularité de **15 minutes** pour Lyon et les communes voisines couvertes par les stations Vélo'v.

Les variables récupérées comprennent notamment :

- température ;
- température ressentie ;
- humidité relative ;
- précipitations ;
- pluie ;
- neige ;
- code météo ;
- vitesse du vent ;
- rafales ;
- visibilité ;
- jour / nuit.

---

## 3. Architecture du pipeline

```text
          API Grand Lyon
          Vélo'v / Stations
                 │
                 │
                 ▼
            Extraction
                 │
                 │
                 ▼
              MongoDB
          Landing / stockage
                 │
                 │
                 ▼
        Apache Spark / PySpark
                 │
       ┌─────────┴─────────┐
       │                   │
       │ Nettoyage         │
       │ Déduplication     │
       │ Jointures         │
       │ Enrichissement    │
       │ Features          │
       │                   │
       └─────────┬─────────┘
                 │
                 ▼
               MinIO
              SILVER
                 │
                 ▼
            analysis.py
                 │
       Agrégations / KPI
                 │
                 ▼
               MinIO
               GOLD


          API Open-Meteo
                 │
                 └──────► MongoDB
```

MongoDB constitue ici la zone de stockage des données collectées avant leur traitement Spark.

MinIO est utilisé comme Data Lake pour stocker les données transformées et analytiques.

---

## 4. Architecture du dépôt

```text
velov-weather-pipeline/
│
├── docker/
│   ├── docker-compose.yml
│   └── Dockerfile
│
├── src/
│   │
│   ├── extraction/
│   │   ├── __init__.py
│   │   ├── collect_meteo.py
│   │   ├── collect_velov.py
│   │   ├── load_mongo.py
│   │   └── main.py
│   │
│   ├── jobs/
│   │   ├── read_sources.py
│   │   ├── transform_sources.py
│   │   ├── analysis.py
│   │   ├── check_duplicates.py
│   │   └── read_gold.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── spark_session.py
│       └── minio_config.py
│
├── tests/
│   ├── __init__.py
│   └── test_transform_sources.py
│
├── .env.example
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── README.md
```

---

## 5. Technologies utilisées

| Technologie | Utilisation |
|---|---|
| Python | Collecte et orchestration |
| Requests | Appels des API |
| MongoDB | Stockage des données collectées |
| PyMongo | Communication Python / MongoDB |
| Apache Spark | Traitement distribué |
| PySpark | Transformations et analyses |
| MinIO | Data Lake compatible S3 |
| Parquet | Format de stockage analytique |
| Docker | Conteneurisation |
| Docker Compose | Orchestration de l'infrastructure |
| Pytest | Tests unitaires |
| Ruff | Qualité et formatage du code |

---

## 6. Infrastructure Docker

L'environnement Docker contient les services suivants :

```text
mongodb
minio
spark-master
spark-worker
velov-extraction
```

### MongoDB

MongoDB reçoit les données collectées depuis les API.

Les principales collections utilisées sont :

```text
velov_stations
velov_availabilities
lyon_meteo
```

### Apache Spark

Le cluster Spark est constitué de :

```text
spark-master
    │
    └── spark-worker
```

Le Worker est configuré avec :

```text
4 CPU
4 Go de mémoire
```

### MinIO

MinIO fournit un stockage objet compatible avec l'API S3.

Le bucket utilisé par le projet est :

```text
datalake
```

---

## 7. Configuration

Créer un fichier `.env` à la racine du projet à partir du fichier :

```text
.env.example
```

Exemple :

```env
MONGO_HOST=mongodb
MONGO_PORT=27017
MONGO_USERNAME=<mongo_username>
MONGO_PASSWORD=<mongo_password>
MONGO_DATABASE=<mongo_database>

MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=<minio_access_key>
MINIO_SECRET_KEY=<minio_secret_key>
```

Le fichier `.env` contient les informations sensibles et ne doit pas être versionné.

Le fichier `.env.example`, sans secrets réels, peut être ajouté au dépôt Git.

---

## 8. Démarrage de l'infrastructure

Depuis la racine du projet :

```powershell
docker compose -f docker/docker-compose.yml --env-file .env up -d
```

Vérifier les conteneurs :

```powershell
docker compose -f docker/docker-compose.yml --env-file .env ps
```

Pour reconstruire les services :

```powershell
docker compose -f docker/docker-compose.yml --env-file .env up -d --build
```

Pour arrêter l'environnement :

```powershell
docker compose -f docker/docker-compose.yml --env-file .env down
```

---

## 9. Interfaces disponibles

### Spark Master

```text
http://localhost:8080
```

### Spark Worker

```text
http://localhost:8081
```

### MinIO Console

```text
http://localhost:9001
```

Les identifiants MinIO correspondent aux variables :

```text
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
```

### MongoDB

MongoDB est exposé sur :

```text
localhost:27017
```

---

## 10. Étape 1 — Extraction

L'extraction est gérée dans :

```text
src/extraction/
```

### `collect_velov.py`

Ce module collecte :

- les stations Vélo'v ;
- l'historique des disponibilités.

La récupération des disponibilités utilise une pagination afin de traiter les volumes importants.

### `collect_meteo.py`

Ce module collecte les données météorologiques avec une granularité de 15 minutes.

La collecte est réalisée pour Lyon et les communes voisines nécessaires au rapprochement avec les stations Vélo'v.

### `load_mongo.py`

Ce module centralise l'accès à MongoDB.

Il permet notamment :

- d'insérer les données ;
- de récupérer la dernière date déjà présente afin de reprendre la collecte.

### `main.py`

Le fichier :

```text
src/extraction/main.py
```

orchestre la collecte.

Le traitement est effectué semaine par semaine afin de limiter le volume de chaque appel API.

Le principe est :

```text
Déterminer la dernière date disponible
        ↓
Collecter la météo
        ↓
Insérer la météo dans MongoDB
        ↓
Collecter les disponibilités Vélo'v
        ↓
Pagination
        ↓
Insérer dans MongoDB
        ↓
Passer à la semaine suivante
```

---

## 11. Étape 2 — Lecture des sources

Le job :

```text
src/jobs/read_sources.py
```

permet d'explorer les collections MongoDB avec Spark.

Il permet notamment de vérifier :

- le schéma ;
- quelques lignes ;
- les partitions Spark.

Ce job est principalement utilisé pour l'exploration et la validation des sources.

---

## 12. Étape 3 — Transformation Silver

Le job principal de transformation est :

```text
src/jobs/transform_sources.py
```

Il lit les trois collections MongoDB :

```text
velov_availabilities
velov_stations
lyon_meteo
```

### Nettoyage Vélo'v

Les principales opérations sont :

```text
suppression des doublons
        ↓
suppression des station_id NULL
        ↓
suppression des horodate NULL
        ↓
conversion horodate en timestamp
        ↓
sélection des colonnes utiles
```

### Nettoyage des stations

Le référentiel est nettoyé avec notamment :

```text
déduplication sur idstation
        ↓
suppression des identifiants NULL
        ↓
suppression des communes NULL
        ↓
contrôle latitude / longitude
```

### Nettoyage météo

Les données météo sont :

```text
dédupliquées
        ↓
contrôlées sur commune et datetime
        ↓
réduites aux variables nécessaires
```

---

## 13. Enrichissement Vélo'v avec les stations

Une première jointure permet d'ajouter les informations géographiques aux disponibilités.

Clé de jointure :

```text
station_id = idstation
```

Les données obtenues contiennent alors notamment :

```text
station_id
horodate
bikes_available
stands_available
capacity
status
nom
commune
lat
lon
nbbornettes
ouverte
```

Les lignes pour lesquelles aucune commune n'est retrouvée sont ensuite exclues du dataset analytique.

Lors du contrôle initial de la jointure :

```text
Nombre total de lignes Vélo'v : 45 966 665
Nombre de lignes sans station : 521 937
Pourcentage sans station      : 1,14 %
```

Environ **98,86 %** des observations Vélo'v trouvent donc une station correspondante dans le référentiel utilisé.

---

## 14. Alignement temporel

Les disponibilités Vélo'v ne sont pas nécessairement enregistrées exactement toutes les 15 minutes.

Exemple :

```text
2023-01-04 11:26:30
```

est ramené à :

```text
2023-01-04 11:15:00
```

La fonction :

```python
add_15_min_bucket()
```

crée donc une variable :

```text
datetime_15m
```

correspondant au quart d'heure inférieur.

La météo étant disponible toutes les 15 minutes, cette variable permet d'effectuer la jointure temporelle.

---

## 15. Jointure Vélo'v + météo

La jointure finale est effectuée sur deux dimensions :

```text
commune
+
datetime_15m
```

La logique est donc :

```text
Vélo'v
   +
Stations
   ↓
Vélo'v géolocalisé
   ↓
bucket temporel 15 min
   +
Météo 15 min
   ↓
JOIN commune + datetime_15m
```

Le résultat contient à la fois les informations Vélo'v, géographiques et météorologiques.

---

## 16. Création des features

Le pipeline ajoute ensuite plusieurs variables analytiques :

```text
year
month
day
hour
day_of_week
is_weekend
availability_rate
```

Le taux de disponibilité est calculé par :

```text
availability_rate =
bikes_available / capacity
```

uniquement lorsque :

```text
capacity > 0
```

Ces variables facilitent ensuite les agrégations et analyses décisionnelles.

---

## 17. Écriture Silver

Le dataset enrichi est écrit dans MinIO au format **Parquet**.

Organisation :

```text
datalake/
└── silver/
    └── velov_weather/
        ├── year=2022/
        ├── year=2023/
        │   ├── month=1/
        │   ├── month=2/
        │   ├── ...
        │   └── month=12/
        └── year=2024/
```

Les données sont donc partitionnées par :

```text
year
month
```

Cette organisation permet à Spark de limiter les données lues lorsqu'une analyse concerne une période spécifique.

---

## 18. Lancer la transformation Silver

Depuis PowerShell :

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.mongodb.spark:mongo-spark-connector_2.13:11.1.0,org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/transform_sources.py
```

La transformation complète peut prendre du temps en raison du volume important de données historiques.

---

## 19. Contrôle des données Silver

Le job :

```text
src/jobs/check_duplicates.py
```

permet de contrôler les doublons après transformation.

Deux contrôles sont réalisés.

### Doublons sur la clé métier

```text
station_id + horodate
```

Résultat après correction :

```text
Nombre de clés station_id + horodate en doublon : 0
```

### Doublons exacts

Toutes les colonnes sont utilisées afin de rechercher des lignes strictement identiques.

Résultat :

```text
Nombre de groupes de doublons exacts : 0
```

Ces contrôles permettent de vérifier que les agrégations Gold ne sont pas faussées par des duplications dans le dataset Silver.

---

## 20. Étape 4 — Analyse et couche Gold

Le job :

```text
src/jobs/analysis.py
```

ne relit pas les données depuis les API ou MongoDB.

Il utilise directement le dataset Parquet Silver :

```text
MinIO Silver
      ↓
analysis.py
      ↓
agrégations
      ↓
MinIO Gold
```

Cette séparation permet d'éviter de rejouer toute la collecte et toute la transformation lorsqu'une analyse doit être recalculée.

---

## 21. Indicateurs décisionnels

Les données Silver sont utilisées pour calculer des agrégations et indicateurs permettant d'analyser le comportement des stations Vélo'v.

Les résultats sont matérialisés dans deux datasets Gold principaux :

```text
velov_weather_metrics
weather_impact
```

### `velov_weather_metrics`

Ce dataset contient les métriques agrégées issues des données Vélo'v enrichies avec la météo.

### `weather_impact`

Ce dataset est destiné à l'analyse de l'influence des conditions météorologiques, notamment des précipitations, sur la disponibilité des vélos.

---

## 22. Organisation Gold

Les données analytiques sont stockées dans :

```text
datalake/
└── gold/
    ├── velov_weather_metrics/
    │   ├── year=2022/
    │   ├── year=2023/
    │   └── year=2024/
    │
    └── weather_impact/
        ├── year=2022/
        ├── year=2023/
        └── year=2024/
```

Les datasets Gold sont également stockés au format Parquet.

---

## 23. Lancer l'analyse Gold

```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --executor-memory 3g `
  --executor-cores 4 `
  --conf spark.jars.ivy=/tmp/ivy `
  --packages org.apache.hadoop:hadoop-aws:3.4.2 `
  /app/src/jobs/analysis.py
```

Cette étape lit directement les données Silver présentes dans MinIO.

---

## 24. Vérification des données MinIO

Afficher le contenu du bucket :

```powershell
docker exec minio mc ls local/datalake
```

Afficher la couche Silver :

```powershell
docker exec minio mc ls local/datalake/silver/velov_weather
```

Afficher les partitions de 2023 :

```powershell
docker exec minio mc ls local/datalake/silver/velov_weather/year=2023
```

Afficher la couche Gold :

```powershell
docker exec minio mc ls local/datalake/gold
```

Afficher les métriques :

```powershell
docker exec minio mc ls local/datalake/gold/velov_weather_metrics
```

Afficher l'analyse météo :

```powershell
docker exec minio mc ls local/datalake/gold/weather_impact
```

---

## 25. Tests unitaires

Les tests sont situés dans :

```text
tests/test_transform_sources.py
```

Ils utilisent de petits DataFrames Spark créés spécialement pour les tests et ne nécessitent pas de relire les millions de lignes du dataset réel.

Les transformations testées sont :

```text
transform_velov
transform_stations
transform_meteo
add_15_min_bucket
add_time_features
```

Lancer les tests :

```powershell
docker exec -it spark-master sh -c `
  "cd /app && \
  PYTHONPATH=/app/src:/opt/spark/python:\
/opt/spark/python/lib/py4j-0.10.9.9-src.zip \
  python3 -m pytest \
  tests/test_transform_sources.py \
  -v \
  -p no:cacheprovider"
```

Résultat attendu :

```text
test_transform_velov PASSED
test_transform_stations PASSED
test_transform_meteo PASSED
test_add_15_min_bucket PASSED
test_add_time_features PASSED

5 passed
```

---

## 26. Qualité du code

Le projet utilise **Ruff** pour contrôler la qualité et le style du code Python.

Exemple de configuration dans `pyproject.toml` :

```toml
[tool.ruff]
line-length = 88
target-version = "py310"

[tool.ruff.lint]
select = ["E", "F", "D"]

[tool.ruff.lint.pydocstyle]
convention = "google"
```

Lancer Ruff :

```powershell
ruff check .
```

Formater le projet :

```powershell
ruff format .
```

Puis vérifier :

```powershell
ruff check .
```

---

## 27. Data Quality

Plusieurs contrôles sont intégrés au pipeline :

- suppression des doublons ;
- contrôle des identifiants de station ;
- contrôle des dates ;
- suppression des lignes sans clés nécessaires ;
- contrôle des stations sans correspondance ;
- contrôle des doublons après création du Silver ;
- protection du calcul de `availability_rate` lorsque la capacité est nulle ;
- tests unitaires des principales transformations.

La qualité est contrôlée avant l'utilisation des données pour les agrégations Gold.

---

## 28. Difficultés rencontrées

### Volumétrie importante

Les données historiques Vélo'v représentent plusieurs dizaines de millions d'observations.

La transformation complète peut donc être relativement longue.

Pour éviter de recalculer l'ensemble du pipeline à chaque analyse, les données transformées sont matérialisées dans la couche Silver de MinIO.

Ainsi :

```text
MongoDB
   ↓
transformation lourde
   ↓
Silver
```

n'est pas rejoué lorsque seule une analyse doit être recalculée.

Le job `analysis.py` peut repartir directement de :

```text
Silver
```

### Alignement des temporalités

Les données Vélo'v et météo n'ont pas exactement le même timestamp.

Une granularité commune de 15 minutes a donc été créée :

```text
horodate
    ↓
datetime_15m
```

La jointure peut ensuite être effectuée sur :

```text
commune + datetime_15m
```

### Correspondance des stations

Certaines observations Vélo'v ne possèdent pas de station correspondante dans le référentiel utilisé.

Le contrôle effectué a montré environ :

```text
98,86 % avec correspondance
1,14 % sans correspondance
```

Les observations sans commune exploitable sont exclues avant la jointure météo.

### Doublons

Des contrôles spécifiques ont été ajoutés afin de s'assurer qu'une observation ne soit pas comptabilisée plusieurs fois dans les analyses.

Après correction et reconstruction du Silver :

```text
Doublons station_id + horodate : 0
Doublons exacts                : 0
```

---

## 29. Architecture Medallion utilisée

Le projet s'inspire d'une architecture **Medallion** simplifiée.

### Landing

```text
MongoDB
```

MongoDB stocke les données collectées depuis les API avant transformation analytique.

### Silver

```text
MinIO
└── silver/
    └── velov_weather/
```

Cette couche contient les données :

- nettoyées ;
- dédupliquées ;
- typées ;
- enrichies avec les stations ;
- alignées temporellement ;
- jointes avec la météo ;
- enrichies avec des features temporelles.

### Gold

```text
MinIO
└── gold/
    ├── velov_weather_metrics/
    └── weather_impact/
```

Cette couche contient les données agrégées destinées à l'analyse et aux indicateurs décisionnels.

---

## 30. Logique globale du projet

```text
┌─────────────────────┐
│ API Grand Lyon      │
│ Vélo'v              │
└──────────┬──────────┘
           │
           │
           ▼
┌─────────────────────┐
│ collect_velov.py    │
└──────────┬──────────┘
           │
           │
           ▼
┌─────────────────────┐
│                     │
│      MongoDB        │
│                     │
│ velov_stations      │
│ velov_availabilities│
│ lyon_meteo          │
│                     │
└──────────┬──────────┘
           ▲
           │
┌──────────┴──────────┐
│ collect_meteo.py    │
└──────────▲──────────┘
           │
           │
┌──────────┴──────────┐
│ API Open-Meteo      │
└─────────────────────┘


MongoDB
   │
   ▼
read_sources.py
   │
   ▼
transform_sources.py
   │
   ├── nettoyage
   ├── déduplication
   ├── jointure stations
   ├── bucket 15 minutes
   ├── jointure météo
   └── features
   │
   ▼
MinIO SILVER
   │
   ├── check_duplicates.py
   │
   ▼
analysis.py
   │
   ├── agrégations
   ├── KPI
   └── analyse météo
   │
   ▼
MinIO GOLD
```

---

## 31. Démonstration du projet

Pour une démonstration, il n'est pas nécessaire de rejouer toute la collecte historique et la transformation de plusieurs dizaines de millions de lignes.

### 1. Démarrer l'infrastructure

```powershell
docker compose -f docker/docker-compose.yml --env-file .env up -d
```

Vérifier :

```powershell
docker compose -f docker/docker-compose.yml --env-file .env ps
```

### 2. Montrer Spark

Ouvrir :

```text
http://localhost:8080
```

et :

```text
http://localhost:8081
```

### 3. Montrer MongoDB

Présenter les collections :

```text
velov_stations
velov_availabilities
lyon_meteo
```

### 4. Présenter la transformation

Expliquer le rôle de :

```text
transform_sources.py
```

et la chaîne :

```text
MongoDB
→ nettoyage
→ stations
→ 15 minutes
→ météo
→ features
→ Silver
```

La transformation historique complète n'a pas besoin d'être relancée pendant la démonstration.

### 5. Montrer MinIO Silver

Ouvrir :

```text
http://localhost:9001
```

puis :

```text
datalake
└── silver
    └── velov_weather
```

Montrer les partitions :

```text
year
└── month
```

### 6. Lancer l'analyse

Lancer `analysis.py` à partir du Silver.

Cette étape démontre que le traitement analytique est indépendant de la collecte et de la transformation initiale.

### 7. Montrer Gold

Dans MinIO :

```text
datalake
└── gold
    ├── velov_weather_metrics
    └── weather_impact
```

### 8. Lancer les tests

Terminer la démonstration par les tests PySpark et montrer :

```text
5 passed
```

Le scénario de démonstration est donc :

```text
Docker
  ↓
MongoDB
  ↓
Spark
  ↓
Silver
  ↓
Analyse
  ↓
Gold
  ↓
Tests
```

---

## 32. Reproductibilité

Le projet a été conçu afin de séparer les responsabilités :

```text
Extraction
    ↓
Stockage
    ↓
Transformation
    ↓
Analyse
```

Chaque étape peut ainsi être exécutée indépendamment.

Cette séparation permet notamment :

- d'éviter de rappeler les API inutilement ;
- d'éviter de recalculer les transformations lourdes ;
- de relancer uniquement les analyses ;
- de faciliter les tests ;
- de simplifier le débogage ;
- de rendre le pipeline plus maintenable.

---

## 33. Résumé

Le pipeline met en œuvre une chaîne Data Engineering complète :

```text
API
 ↓
Extraction Python
 ↓
MongoDB
 ↓
PySpark
 ↓
Nettoyage
 ↓
Déduplication
 ↓
Jointures
 ↓
Feature Engineering
 ↓
Parquet
 ↓
MinIO Silver
 ↓
Agrégations
 ↓
MinIO Gold
 ↓
Indicateurs décisionnels
```

Le projet combine ainsi ingestion de données, stockage NoSQL, traitement distribué, Data Lake, format Parquet, partitionnement, contrôle qualité, tests et analyse de données.