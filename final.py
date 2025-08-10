from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.empty import EmptyOperator  # Airflow 3.x replacement DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Project, dataset, and table details
project_id = 'silken-forest-466023-a2'
dataset_id = 'staging_dataset'
transform_dataset_id = 'transform_dataset'
reporting_dataset_id = 'reporting_dataset'
source_table = f'{project_id}.{dataset_id}.global_data'

# List of countries
countries = ['USA', 'India', 'Germany', 'Japan', 'Italy', 'France', 'Canada', 'Australia']

with DAG(
    dag_id='load_and_transform_view',
    default_args=default_args,
    description='Load CSV from GCS to BigQuery and create country-specific tables and views',
    schedule=None,  # manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'csv'],
) as dag:

    # Task 1: Check if file exists in GCS
    check_file_exists = GCSObjectExistenceSensor(
        task_id='check_file_exists',
        bucket='sadhika-1-central1',
        object='global_health_data.csv',
        timeout=300,
        poke_interval=30,
        mode='poke',
    )

    # Task 2: Load CSV to BigQuery
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bigquery',
        bucket='sadhika-1-central1',
        source_objects=['global_health_data.csv'],
        destination_project_dataset_table=source_table,
        source_format='CSV',
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
    )

    # Task lists
    create_table_tasks = []
    create_view_tasks = []

    # Task 3: Create country-specific tables and views
    for country in countries:
        table_task = BigQueryInsertJobOperator(
            task_id=f'create_table_{country.lower()}',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_table` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,
                }
            },
        )

        view_task = BigQueryInsertJobOperator(
            task_id=f'create_view_{country.lower()}_table',
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        SELECT 
                            `Year` AS year,
                            `Disease Name` AS disease_name,
                            `Disease Category` AS disease_category,
                            `Prevalence Rate` AS prevalence_rate,
                            `Incidence Rate` AS incidence_rate
                        FROM `{project_id}.{transform_dataset_id}.{country.lower()}_table`
                        WHERE `Availability of Vaccines Treatment` = FALSE
                    """,
                    "useLegacySql": False,
                }
            },
        )

        create_table_tasks.append(table_task)
        create_view_tasks.append(view_task)

    # Final success task
    success_task = EmptyOperator(task_id='success_task')

    # Dependencies
    check_file_exists >> load_csv_to_bigquery
    for t, v in zip(create_table_tasks, create_view_tasks):
        load_csv_to_bigquery >> t >> v >> success_task
