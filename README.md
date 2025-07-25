# ETL with Apache Spark Streaming on Databricks

## Overview

This project demonstrates how to perform ETL (Extract, Transform, Load) operations using Apache Spark Structured Streaming on the Databricks platform. The focus is on ingesting and processing customer data in a scalable, real-time manner using Databricks’ capabilities.

## Key Features

- **Spark Structured Streaming:** Utilizes Spark's powerful streaming engine to process customer data continuously and efficiently.
- **Databricks Auto Loader:** Employs Databricks’ Auto Loader to detect and ingest new files from cloud storage automatically, simplifying data ingestion pipelines.
- **CloudFiles Integration:** Leverages Databricks' cloudFiles` feature for optimized file detection and schema inference when loading customer data.
- **ADLS Gen2 Integration:** Uses Azure Data Lake Storage Gen2 (ADLS Gen2) as the data source for customer files.
- **Access Connector & Managed Identity:** Implements Databricks Access Connector to securely create storage credentials, enabling access to ADLS Gen2 via Managed Identity.
- **Scalable ETL:** Designed to handle large volumes of customer data, supporting both batch and streaming workloads.

## Data Source Details

- **Customers Data Location:** Stored in the `gizmobox` container within ADLS Gen2.
- **File Format:** All customer files are in `.json` format.

## Technologies Used

- **Databricks Platform**
- **Apache Spark Structured Streaming**
- **Databricks Auto Loader (`cloudFiles`)**
- **Azure Data Lake Storage Gen2 (ADLS Gen2)**
- **Databricks Access Connector & Managed Identity**
- **JSON Data Format**

## Workflow

1. **Configure Storage Credential:** Set up Databricks Access Connector and Managed Identity to access the `gizmobox` container in ADLS Gen2 securely.
2. **Data Source:** Customer.JSON files are stored in the ADLS Gen2 `gizmobox` container.
3. **Auto Loader Initialization:** Databricks Auto Loader monitors the container for new customer data files.
4. **Streaming ETL Pipeline:** As new `.json` files appear, Auto Loader ingests them, and Spark Structured Streaming processes the data in real-time.
5. **Transformation:** The streaming data undergoes necessary transformations and cleansing.
6. **Loading:** Transformed customer data is stored in Databricks tables or other target systems for analytics or further processing.

## How to Use

1. **Set Up Your Databricks Workspace:** Ensure you have access to a Databricks workspace.
2. **Configure Storage Credential:**
   - Use Databricks Access Connector to create a storage credential.
   - Attach a Managed Identity for secure access to ADLS Gen2.
3. **Update Data Path:** Point the Auto Loader to the `gizmobox` container containing customer `.json` files.
4. **Run the Notebooks/Scripts:** Follow the provided notebooks or scripts to initialize the Auto Loader and set up the Spark Structured Streaming pipeline.
5. **Monitor the Pipeline:** Use Databricks’ UI to monitor streaming jobs and data ingestion.

## Example Notebook Steps

- Configure access to ADLS Gen2 using Access Connector and Managed Identity.
- Set the path to the `gizmobox` container for customer `.json` files.
- Initialize Spark session and Auto Loader.
- Define the schema for customer data.
- Set up streaming transformations.
- Write processed data to Delta tables or other sinks.

## Benefits

- **Real-Time Insights:** Ingest and process customer data as soon as it arrives.
- **Secure Cloud Integration:** Seamless, secure access to ADLS Gen2 using Managed Identity.
- **Low Maintenance:** Auto Loader reduces manual file management and schema handling.
- **Scalable & Reliable:** Built on Databricks and Spark for robust, enterprise-grade ETL.

## Contact

For questions or support, please open an issue or contact [sambitdan](https://github.com/sambitdan).
