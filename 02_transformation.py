from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# DAG Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

#Define project,dataset, and table details
project_id = 'silken-forest-466023-a2'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'

source_table = f'{project_id}.{dataset_id}.global_data'
countries = ['USA', 'India', 'Germany', 'Japan', 'Italy', 'France', 'Canada', 'Australia']

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

    #task 3
    for country in countries:
        BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query" : f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False, #use standard sql syntax
                }
            },
        ).set_upstream(load_csv_to_bigquery)

    check_file_exists >> load_csv_to_bigquery
