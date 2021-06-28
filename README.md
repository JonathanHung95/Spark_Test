# Spark_Test
Repository for WCD Spark Test.

### Step 1 - Data Gathering and Ingestion

Included are 2 ways of ingesting data to an S3 bucket:
1. Spark code that reads the data and then writes a csv file to the S3 bucket.
2. An Airflow DAG that moves the data from a folder to an S3 bucket.

The first method is done as I believe the test asks for Spark code.  The second method covers automating the the ingestion in the future.

### Step 2 - Prepare & Cleanse the data in memory

### Step 3 - Gather insights from the data

#### The president of company wants to understand which provinces and stores are performing well and how much are the top stores in each province performing compared with the average store of the province:

+----------------+------------------+------------------+-----------------+
|        province|store_location_key|       sales_total|        avg_sales|
+----------------+------------------+------------------+-----------------+
|BRITISH COLUMBIA|              7167|44512.810000000056|19823.40333333335|
|    SASKATCHEWAN|              7317|           46439.7|        25463.475|
|         ALBERTA|              9807|226059.89000000042|36074.87727272729|
|        MANITOBA|              4823| 55826.76000000021|18701.93000000007|
|         ONTARIO|              8142| 378059.2800000014|55228.75555555575|
+----------------+------------------+------------------+-----------------+

The province gives us the province of the store.
The store_location_key gives us the key of the store.
The sales_total column gives us the total sales of each store.  
The avg_sales column shows the average sales in the province.

The stores in the table are the ones with the highest sales from each province from the transaction data given.  This is shown against the average sales as requested.

#### The president further wants to know how customers in the loyalty program are performing compared to non-loyalty customers:

+-------+------------------+
|loyalty|         avg_sales|
+-------+------------------+
|      1| 22.33717102045659|
|      0|19.794947792546285|
+-------+------------------+

Loyalty represents whether the customer is in the loyalty program or not.  1 means they are and 0 means they are not.
avg_sales is the average sales generated from each loyalty segment.

The table shows the loyalty segments against each other.  Unsurprisingly, the customers in the loyalty program generate higher sales on average.

#### what category of products is contributing to most of ACME’s sales

I'm not sure how to interpret this question, so I have provided the product category with the highest total sales and the product category with the highest count of sales.

From the data, fe148072 has the highest total sales at 24035.56.  On the other hand, d5a0a65d has the highest count of sales at 2298.