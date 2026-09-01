docker compose -f docker/docker-compose.yml --env-file .env up -d --build
docker compose -f docker/docker-compose.yml --env-file .env logs -f extraction
docker exec -it mongodb mongosh -u user -p password --authenticationDatabase admin