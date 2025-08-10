from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# DAG Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# DAG Definition
with DAG(
    dag_id='check_load_csv_to_bigquery',
    default_args=default_args,
    description='Check for CSV file in GCS and load to BigQuery',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:
    
    #task to check if the file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='sadhika-1-central1',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke', # Use 'poke' mode for synchronous checking
    )

    #task to load csv from gcs to bigquery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='sadhika-1-central1',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table='silken-forest-466023-a2.staging_dataset.global_data',
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE', 
        skip_leading_rows=1, # skip header row
        field_delimiter=',',
        autodetect=True,
    )

    check_file_exists >> load_csv_to_bigquery
