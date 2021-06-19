from pyspark.sql import SparkSession

# step 1: read csv files in the s3 bucket into memory
# step 2: combine csv files into 1 file
# step 3: replace null sales and null units with zero

if __name__ == "__main__":


    spark = SparkSession.builder.\
                        .appName("spark_step_1").\
                        .getOrCreate()

    # read all the csv files in S3 bucket into memory

    csv_df = spark.read.format("csv").option("header", "true").load("S3://path/to/bucket/*.csv")

    