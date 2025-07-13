# Running kafka
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

# Running Generator
 cd into `ingestion_layer`
 First run `metadata` as fake sales data needs a category to be generated
 `python data_generators/metadata_collector.py`
 Run Sales Data generator
 `python data_generators/sales_data_collector.py`
 Run External Data generator
 `python data_generators/external_data_collector.py`


# Running python data pre-processing
`python -m ingestion_layer.preprocessors.process_sales`
`python -m ingestion_layer.preprocessors.process_metadata`
`python -m ingestion_layer.preprocessors.process_external`


# Running Forecasting Engine locally
`uvicorn app.main:app --reload`