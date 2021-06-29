from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import IntegerType, LongType
from pyspark.sql import Window

import os
from glob import glob

# we want to find out:
# which provinces and stores are performing well -> top store of each province vs the average of the province
# customers in the loyalty program vs non-loyalty program
# what categories are performing the best
# determine the top 5 stores by province + top 10 product categories by department

if __name__ == "__main__":

    spark = SparkSession.builder.\
                        .appName("spark_step_3").\
                        .getOrCreate()

    # read all the csv files into memory

    data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    paths = glob(data_folder + "/trans*.csv", recursive = True)

    csv_df = spark.read.csv(paths, header = True)

    # replace null sales and null units with zero

    csv_df = csv_df.na.fill(value = "0", subset = ["sales", "units"])

    # Question 1

    # get the average of each province

    location_df = spark.read.csv(os.path.join(data_folder, "location.csv"), header = True)
    product_df = spark.read.csv(os.path.join(data_folder, "product.csv"), header = True)

    join_csv_location = csv_df.join(location_df, csv_df.store_location_key == location_df.store_location_key).select(csv_df["*"], location_df["province"])
    average_province_df = join_csv_location.groupBy("province").agg((F.sum("sales")/F.countDistinct("store_location_key")).alias("avg_sales"))

    # top store by province

    sales_total_df = join_csv_location.groupBy("store_location_key").agg(F.sum("sales").alias("sales_total"))
    sales_total_df = sales_total_df.join(location_df, sales_total_df.store_location_key == location_df.store_location_key).select(sales_total_df["*"], location_df["province"])

    # get what we want (ie. Top store by province vs average provincial sales)

    final_1_df = sales_total_df.groupBy("province").agg(F.max("sales_total").alias("sales_total"))
    final_1_df = final_1_df.join(sales_total_df, final_1_df.sales_total == sales_total_df.sales_total).select(final_1_df["*"], sales_total_df["store_location_key"])
    final_1_df = final_1_df.join(average_province_df, final_1_df.province == average_province_df.province, "inner")\
                            .select(final_1_df["*"], average_province_df["avg_sales"])\
                            .select("province", "store_location_key", "sales_total", "avg_sales")

    final_1_df = final_1_df.join(average_province_df, final_1_df.province == average_province_df.province, "inner").select(final_1_df["*"], average_province_df["avg_sales"]).select("province", "store_location_key", "sales_total", "avg_sales")

    # Question 2
    # note, for the first part we will get the mean to normalize the data ie. average sales per transaction

    # encode our collector data
    # then get the average sales of those with loyalty and those without
    # use LongType because IntType results in integer overflow

    csv_collector_encode_df = csv_df.withColumn("loyalty", F.when(csv_df.collector_key.cast(LongType()) > 0, 1).otherwise(0))
    final_2_df = csv_collector_encode_df.groupBy("loyalty").agg(F.mean("sales").alias("avg_sales"))

    # get the product category that contributes the most sales
    # since I don't know exactly whats wanted, I'll give the sales total of the best category and the category with the highest count

    join_csv_product_df = csv_df.join(product_df, csv_df.product_key == product_df.product_key).select(csv_df.product_key, csv_df.sales, product_df.category)
    final_3_df = join_csv_product_df.groupBy("category").agg(F.sum("sales").alias("total_sales"))
    final_3_df = final_3_df.orderBy(final_3_df.total_sales.desc())
    
    final_4_df = join_csv_product_df.groupBy("category").count()
    final_4_df = final_4_df.orderBy(final_4_df["count"].desc())

    # Question 3
    # determine the top 5 stores by province

    w = Window.partitionBy(sales_total_df.province).orderBy(sales_total_df.sales_total.desc())

    final_5_df = sales_total_df.select("*", F.rank().over(w).alias("rank")).filter(F.col("rank") <= 5)

    # top 10 categories by department

    product_2_df = product_df.dropDuplicates(["category"]).select(product_df.department, product_df.category.alias("category_x"))
    final_6_df = final_3_df.join(product_2_df, final_3_df.category == product_2_df.category_x, "left")\
                            .select(final_3_df["*"], product_2_df.department)

    w = Window.partitionBy(final_6_df.department).orderBy(final_6_df.total_sales.desc())

    final_6_df = final_6_df.select("*", F.rank().over(w).alias("rank")).filter(F.col("rank") <= 10)