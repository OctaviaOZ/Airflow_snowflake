## Prerequisites

- Git
- Docker
- Docker Compose

## Instructions

Clone the repository: Clone the repository containing the `dag_rate_to_showflake.py` to your local machine.

Build the Docker image: In the root directory of the repository, run the following command to build the Docker image:

```
docker compose up airflow-init
```

Start the Docker containers: Run the following command to start the Docker containers:

```
docker-compose up
```

Access the Airflow UI: Open a web browser and navigate to `http://localhost:8080` to access the Airflow UI.

Stop the Docker containers: To stop the Docker containers, run the following command in the root directory of the repository:

```
docker-compose down
```

By following these instructions, you should be able to run the `dag_rate_to_showflake.py` DAG using Docker Compose.