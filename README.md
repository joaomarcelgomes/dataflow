## Versions

Project developed using:

```
# Python 3.12.3
```

## Commands

### Starting the Environment

To start all required containers (e.g., database, MinIO, services), run:

```bash
docker compose up -d
```

```airflow
docker exec airflow airflow scheduler
```