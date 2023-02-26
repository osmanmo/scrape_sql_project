docker-build:
	docker-compose build
docker-run:
	docker-compose --env-file .env.docker up -d
docker-exec:
	docker exec -it scrape_project_airflow-webserver_1 /bin/bash
docker-stop:
	docker-compose --env-file .env.docker down