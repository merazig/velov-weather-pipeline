docker compose -f docker/docker-compose.yml --env-file .env up -d --build

# vérifier:
docker compose -f docker/docker-compose.yml --env-file .env ps

docker compose -f docker/docker-compose.yml --env-file .env logs -f extraction

# accéder à db:
docker exec -it mongodb mongosh -u user -p password --authenticationDatabase admin
show dbs 

# test spark
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/src/extraction/jobs/test_spark.py