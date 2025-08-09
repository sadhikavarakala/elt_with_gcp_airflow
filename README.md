# Load and Transform Global Health Data with Airflow & GCP

This project builds an **ELT (Extract, Load, Transform)** pipeline using **Apache Airflow** and **Google Cloud Platform** to process a global health dataset.  
The pipeline loads data from **Google Cloud Storage (GCS)** into **BigQuery**, then creates **country-specific tables and views** for reporting and analysis.

---

## Features
- Check for file existence in GCS before processing.
- Load CSV data into a BigQuery staging table.
- Create country-specific tables in a transform dataset.
- Generate reporting views for each country.
- Orchestrated fully with Apache Airflow.

---

## Workflow
1. **Extract** – Verify the CSV file in GCS.
2. **Load** – Load raw data into BigQuery (staging dataset).
3. **Transform** – Create per-country tables and filtered reporting views.

---

## Requirements
- **Google Cloud Platform**:
  - BigQuery
  - Cloud Storage
- **Apache Airflow** with Google Cloud providers installed
- Service account with:
  - Storage Object Viewer
  - BigQuery Data Editor
  - BigQuery Job User

---

## End Result
### Airflow Pipeline
<img width="1894" height="1450" alt="load_and_transform_view-graph" src="https://github.com/user-attachments/assets/434fe7bf-fbd9-4589-a4c6-f6af11820de0" />

### Sample Looker Studio Report
<img width="900" height="900" alt="USA_Health_Data" src="https://github.com/user-attachments/assets/f3bc764c-d196-4142-8bdc-16efbf2e6453" />

