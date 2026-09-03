### build les images
```Bash
docker compose -f docker/docker-compose.yml --env-file .env up -d --build
```
### Run service extraction
```Bash
docker compose -f docker/docker-compose.yml --env-file .env logs -f extraction
```
### accéder à mongodb
```Bash
docker exec -it mongodb mongosh -u user -p password --authenticationDatabase admin
```
#### afficher les databases
```Bash
show dbs
```
#### acceder à une base
```Bash
use db_name
```
#### afficher les collections 
```Bash
show collections
```
#### afficher une ligne
```Bash
db.velov_availabilities.findOne()
```

## 1. Faire un backup MongoDB
Le plus simple est d'utiliser `mongodump` depuis le conteneur MongoDB.

Commence par identifier ton conteneur :
```Bash
docker compose -f docker/docker-compose.yml --env-file .env ps
```
Puis :
```Bash
docker exec <nom_conteneur_mongo> mongodump \
  --username user \
  --password password \
  --authenticationDatabase admin \
  --db velov_weather \
  --out /tmp/backup
```
Mais ce backup est à l'intérieur du conteneur. Il faut ensuite le copier sur ta machine :
```
docker cp <nom_conteneur_mongo>:/tmp/backup ./backup
```
Tu auras quelque chose comme :
```text
backup/
└── velov_weather/
    ├── velov_availabilities.bson
    ├── velov_availabilities.metadata.json
    ├── lyon_meteo.bson
    ├── lyon_meteo.metadata.json
    └── ...
```
## 2. Encore mieux : mettre le backup directement sur ta machine
Tu peux faire :
```Bash
docker exec <nom_conteneur_mongo> mongodump \
  --username user \
  --password password \
  --authenticationDatabase admin \
  --db velov_weather \
  --archive \
  --gzip > backup_velov.gz
```
Tu obtiens :

`backup_velov.gz`

sur ta machine, pas dans le volume Docker.

C'est ce que je privilégierais dans ton cas.

## 3. Restaurer si tu supprimes le volume
Tu recrées MongoDB puis :
```Bash
docker exec -i <nom_conteneur_mongo> mongorestore \
  --username user \
  --password password \
  --authenticationDatabase admin \
  --archive \
  --gzip < backup_velov.gz
```
Tu récupères alors tes collections.

## 4. Attention à docker `compose down -v`
Cette commande :
```Bash
docker compose down -v
```
supprime les volumes associés au Compose.

Donc dans ton cas :
```text
MongoDB
   ↓
volume Docker
   ↓
100+ millions de documents
```
`down -v` peut supprimer le volume et donc tes données.
