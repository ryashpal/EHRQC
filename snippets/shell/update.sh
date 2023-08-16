git pull origin master
docker-compose build --no-cache
docker-compose up -d --force-recreate
docker exec -it ehrqc_app_1 bash
source .venv/bin/activate
