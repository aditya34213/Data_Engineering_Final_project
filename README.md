# Data_Engineering_Final_project
# File comparison and Data Quality Framework

Data Engineering Pipeline using Azure + Databricks

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


##  Future Improvements

* Add Airflow orchestration
* Implement streaming pipeline
* Add monitoring & alerts

##  Author
Aditya Gadekar
