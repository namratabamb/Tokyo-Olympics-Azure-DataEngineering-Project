# Tokyo-Olympics-Azure-DataEngineering-Project
 
# Overview

This Data Engineering project aims at securely ingest, tranform and load Tokyo Olympics data and perform analytics on top of it. The dataset comprises over 11,000 athletes, with 47 disciplines, along with 743 Teams taking part in the 2021(2020) Tokyo Olympics.
The major aim of this project is to gain a deeper understanding of Azure Cloud services in the field of data engineering by developing ETL pipeline for the Tokyo Olympics data nd perform data analytics on it.

# Azure Cloud Services Utilized

1] Azure Data Lake Gen 2 - It is aq storage system service for Blob storage providing file system semantics, file-level security and scale. Because these capabilities are built on Blob storage, you also get low-cost, tiered storage, with high availability/disaster recovery capabilities.

2] Azure Data Factory - It is the cloud based ETL and data integration service that allows to create data-driven workflows for orchestrating data movement and transformation at scale. 

3] Synapse Analytics - Azure Synapse Analytics is a limitless analytics service that brings together enterprise SQL data warehousing and big data analytics services.

4] Azure Databricks - Azure Databricks provides tools that help you connect your sources of data to one platform to process, store, share, analyze, model, and monetize datasets with solutions from BI to generative AI.

# Goals

The goal of this project is to understand the Azure cloud services to understand the Tokyo Olympics data and perform ETL (Extract, Transform and Load) process so as to derive meaning of the data and insights from it. For this purposes, there are multiple stages inviolved in the project as below:

1] Data Ingestion - This is the Extract phase of the ETL where the data is ingested from different sources.

2] Data Transformation - There is certain transformation done on the raw ingested csv data inorder to further perform analytics on it.

3] Data Loading - The cleansed and transformed data is loaded back in Azure data lake.

4] Analytics - The Synapse analytics is used to perform analytics on this data lake data.

# Dataset

For this project,I have made use of the Kaggle dataset - https://www.kaggle.com/datasets/arjunprasadsarkhel/2021-olympics-in-tokyo