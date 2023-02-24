docker-build:
	docker-compose build
docker-run:
	docker-compose --env-file .env up -d
docker-exec:
	docker exec -it etl_airflow-webserver_1 /bin/bash
docker-stop:
	docker-compose --env-file .env down