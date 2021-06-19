# Spark_Test
Repository for WCD Spark Test.

### Step 1 - Data Gathering and Ingestion

Included are 2 ways of ingesting data to an S3 bucket:
1. Spark code that reads the data and then writes a csv file to the S3 bucket.
2. An Airflow DAG that moves the data from a folder to an S3 bucket.

The first method is done as I believe the test asks for Spark code.  The second method covers automating the the ingestion in the future.

### Step 2 - Prepare & Cleanse the data in memory

### Step 3 - Gather insights from the data