

```Bash
docker compose -f docker/docker-compose.yml --env-file .env down
```
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