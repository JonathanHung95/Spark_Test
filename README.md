# Spark_Test
Repository for WCD Spark Test.

## Step 1 - Data Gathering and Ingestion

We use boto3 in the pyspark environment to upload the data to an S3 bucket without modifying them.

To automate the process, I would probably not build it as a part of the spark job and instead as a python script that gets called by an AirFlow dag.  This would allow us to have AirFlow check a folder for files every hour or so and upload anything in there to the S3 bucket.  

To ensure data security, files would be stored on a company server and any AWS credentials required would be saved there as well.

## Step 2 - Prepare & Cleanse the data in memory

We import all the csv data into a single data frame and replace all the nulls in the sales and units columns with 0.  

Considerations to extend to another retail store:
1. Check that the null data needs to be handled the same way (for example, maybe they would prefer the data deleted instead)
2. Transaction data follows the same schema
3. Should negative sales and units stay in the data

## Step 3 - Gather insights from the data

### Top store by province vs average provincial sales

|        province|store_location_key|       sales_total|        avg_sales|
|----------------|------------------|------------------|-----------------|
|BRITISH COLUMBIA|              7167|44512.810000000056|19823.40333333335|
|    SASKATCHEWAN|              7317|           46439.7|        25463.475|
|         ALBERTA|              9807|226059.89000000042|36074.87727272729|
|        MANITOBA|              4823| 55826.76000000021|18701.93000000007|
|         ONTARIO|              8142| 378059.2800000014|55228.75555555575|

The province gives us the province of the store.
The store_location_key gives us the key of the store.
The sales_total column gives us the total sales of each store.  
The avg_sales column shows the average sales in the province.

The stores in the table are the ones with the highest sales from each province from the transaction data given.  This is shown against the average sales as requested.

### Loyalty vs Non-Loyalty Customers

|loyalty|         avg_sales|
|-------|------------------|
|      1| 22.33717102045659|
|      0|19.794947792546285|

Loyalty represents whether the customer is in the loyalty program or not.  1 means they are and 0 means they are not.
avg_sales is the average sales generated from each loyalty segment.

The table shows the loyalty segments against each other.  Unsurprisingly, the customers in the loyalty program generate higher sales on average.

### Product category contributing to most sales

I'm not sure how to interpret this question, so I have provided the product category with the highest total sales and the product category with the highest count of sales.

From the data, fe148072 has the highest total sales at 24035.56.  On the other hand, d5a0a65d has the highest count of sales at 2298.

### Top 5 stores by province

|store_location_key|       sales_total|        province|rank|
|------------------|------------------|----------------|----|
|              7167|44512.810000000056|BRITISH COLUMBIA|   1|
|              7175|14096.529999999997|BRITISH COLUMBIA|   2|
|              7102| 860.8700000000001|BRITISH COLUMBIA|   3|
|              7317|           46439.7|    SASKATCHEWAN|   1|
|              7309| 4487.249999999997|    SASKATCHEWAN|   2|
|              9807|226059.89000000042|         ALBERTA|   1|
|              7296| 93925.69999999984|         ALBERTA|   2|
|              9802|          55884.25|         ALBERTA|   3|
|              7226| 9994.969999999994|         ALBERTA|   4|
|              7287|           3124.22|         ALBERTA|   5|
|              4823| 55826.76000000021|        MANITOBA|   1|
|              4861|            257.69|        MANITOBA|   2|
|              7403|             21.34|        MANITOBA|   3|
|              8142| 378059.2800000014|         ONTARIO|   1|
|              6973| 315484.4600000015|         ONTARIO|   2|
|              1396|112464.72000000074|         ONTARIO|   3|
|              1891|  76338.6899999998|         ONTARIO|   4|
|              6979|50069.720000000125|         ONTARIO|   5|

The store_location_key gives us the key of the store.
The sales_total column gives us the total sales of each store.
The province column provides the province of the corresponding store.
Rank gives the rank within the province of the store.

The table shows the top 5 stores by province.  Note that some stores have no transaction data so they aren't shown here (ie. Saskatchewan only has 2 entries as there's only data for 2 stores).  This is likely due to null data.

### Top 10 products by department

|category|       total_sales|department|rank|
|--------|------------------|----------|----|
|687ed9e3|13126.849999999964|   a461091|   1|
|1578f747|               0.0|  34a2a7e0|   1|
|a4d52407|           3016.91|  5bffa719|   1|
|6c504249| 2528.580000000001|  5bffa719|   2|
|fa3a1bd8|1921.9699999999993|  5bffa719|   3|
|a121fb78|           1034.87|  5bffa719|   4|
|3ae24ac2|            746.65|  5bffa719|   5|
|8f610228| 455.6399999999999|  5bffa719|   6|
|999b3a55|260.82000000000005|  5bffa719|   7|
|108c2838|            174.53|  5bffa719|   8|
|2dc2366a|113.36000000000001|  5bffa719|   9|
|3f1695bd|56.769999999999996|  5bffa719|  10|
|cef3760b|11924.249999999985|  7569cb40|   1|
| 382cf3a| 6740.889999999998|  b947a4a9|   1|
|8b4f9982|           3895.72|  b947a4a9|   2|
|3b380e17|           3327.67|  b947a4a9|   3|
| 6761045|1015.0399999999997|  b947a4a9|   4|
|29ecadc0|             722.2|  b947a4a9|   5|
|6023eeb7| 587.3199999999999|  b947a4a9|   6|
|f2672c8c| 476.2399999999999|  b947a4a9|   7|
|9db5a1ff|226.95000000000002|  b947a4a9|   8|
|50c418ce|             19.08|  24d07cc8|   1|
|e0b38f5b|11482.360000000004|   435ca98|   1|
|21b19a94| 3634.510000000001|   435ca98|   2|
|640d751d|2845.4099999999994|   435ca98|   3|
|2836e915|           1639.85|   435ca98|   4|
|e8eeb80f|            572.54|   435ca98|   5|
| a05bcbb|             88.98|   435ca98|   6|
|fe148072| 24035.55999999999|  1a34cbb9|   1|
|ffcec4a7|19108.690000000002|  1a34cbb9|   2|
|e49d14f1|15540.739999999993|  1a34cbb9|   3|
|11566ced|10550.520000000004|  1a34cbb9|   4|
|e934fcda|10538.889999999996|  1a34cbb9|   5|
|7703921f| 9029.830000000004|  1a34cbb9|   6|
|a97ccc2c| 8115.559999999998|  1a34cbb9|   7|
|d18e3df7| 7053.729999999999|  1a34cbb9|   8|
|fbe05f0d| 5190.739999999999|  1a34cbb9|   9|
|7b703ee1| 4625.830000000001|  1a34cbb9|  10|
|511e5c1b|176.21999999999997|  89d0c9d1|   1|
|c280daf5|1936.7900000000004|  3ad17de9|   1|
|caddeda1| 562.4000000000001|  3ad17de9|   2|
|43caffcc|            371.73|  3ad17de9|   3|
|ecc023a5|             37.38|  3ad17de9|   4|
|901c49d4|             21.88|  3ad17de9|   5|
|14a2b392|             -62.3|  3ad17de9|   6|
|7aaa7a34| 4796.410000000003|  651b1068|   1|
|e5475024|2991.7100000000028|  651b1068|   2|
|ed1a6770|           2616.44|  651b1068|   3|
|c35a9e20|2082.2800000000007|  651b1068|   4|
|5cb7b430|1738.3200000000004|  651b1068|   5|
|2c1872bf|1381.8500000000001|  651b1068|   6|
| 7ac5490|1268.2900000000002|  651b1068|   7|
|b7dbc305|           1117.06|  651b1068|   8|
|bba86ffb| 747.8100000000001|  651b1068|   9|
|7f5ea54b| 453.8999999999999|  651b1068|  10|
|d5a0a65d|10829.389999999976|  c81ba571|   1|
|5530c7b1|10004.750000000007|  c81ba571|   2|
|ddfd9109| 4937.980000000007|  c81ba571|   3|
|54ea8364|3828.4599999999987|  c81ba571|   4|
|374ba2e9|              35.5|  c81ba571|   5|
|65d731c8|          12228.38|  4b8d7c31|   1|
|190b5bb9|10144.530000000002|  4b8d7c31|   2|
| 2588bef| 8778.100000000002|  4b8d7c31|   3|
|206bde41|           6747.12|  4b8d7c31|   4|
|1d25c013| 6011.230000000001|  4b8d7c31|   5|
| ff163c4| 3707.900000000001|  4b8d7c31|   6|
|259f013e|           3365.19|  4b8d7c31|   7|
|4a4d8c4d|           3249.41|  4b8d7c31|   8|
|f793f3f3|2697.1700000000023|  4b8d7c31|   9|
|69a49d36|2513.1600000000008|  4b8d7c31|  10|
|5d6df0e5|              17.8|  144711b0|   1|
|32bf7d96| 4734.799999999998|  2aa3a6c1|   1|

Category gives the product's category.
The total_sales column gives the total sales of the product from the transaction data.
Department gives the department that the product belongs to.
Rank gives the rank of each category within the department.

The table gives the top 10 products by department.  It should be noted that since there is negative sales data, some of the total_sales are negative.  