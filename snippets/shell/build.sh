docker-compose build --no-cache
docker-compose up -d --force-recreate
docker exec -it ehrqc-app-1 bash
source .venv/bin/activate
