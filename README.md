# Data_Engineering_Final_project
# File comparison and Data Quality Framework

Data Engineering Pipeline using Azure + Databricks + ADF

##  Overview

This project implements a scalable data pipeline using the Medallion Architecture (Bronze, Silver, Gold) to process and validate delivery/logistics data.

##  Architecture

* Bronze Layer: Raw data ingestion from CSV
* Silver Layer: Data cleaning, null handling, transformations
* Gold Layer: Aggregated business insights
* Validation Layer: Data quality checks (row count, nulls, statistics)

##  Tech Stack

* Azure Data Lake (ADLS Gen2)
* Azure Databricks
* PySpark
* Delta Lake

##  Key Features

* Data validation framework (row count, distinct count, null checks)
* Schema evolution handling
* Incremental processing (Delta Lake)
* Medallion architecture implementation

## Sample Insights

* Average delivery time by city
* Rating distribution analysis


##  How to Run

1. Upload data to ADLS
2. Run Bronze notebook
3. Run Validation notebook
4. Run Silver notebook
5. Run Gold notebook

<img width="1727" height="864" alt="image" src="https://github.com/user-attachments/assets/02ed782e-e5fb-478f-9833-40cb91d1d935" />
<img width="1919" height="1079" alt="Screenshot 2026-04-06 114933" src="https://github.com/user-attachments/assets/476f5516-4a06-47d5-81d7-d8a9cc98e54c" />



##  Future Improvements

* Add Airflow orchestration
* Implement streaming pipeline
* Add monitoring & alerts

##  Author
Aditya Gadekar
