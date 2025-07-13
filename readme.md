#Running kafka
`docker-compose -f docker-compose.kafka.yaml up -d`

# Stopping kafka
`docker-compose -f docker-compose.kafka.yaml down --volumes`

# Running Airflow
First, build the custom Airflow image to ensure compatibility with your machine's architecture (especially for Apple Silicon Macs):
`docker-compose -f docker-compose.airflow.yaml build`

Then, start the services:
`docker-compose -f docker-compose.airflow.yaml --env-file .env up -d`

# Stopping Airflow without teardown volumes
`docker-compose -f docker-compose.airflow.yaml down`

# Stopping Airflow with teardown volumes
`docker-compose -f docker-compose.airflow.yaml down -v`

# Starting the Databases
`docker-compose -f docker-compose.db.yaml up -d`

# Stopping the Databases
`docker-compose -f docker-compose.db.yaml down --volumes`