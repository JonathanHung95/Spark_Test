from pyspark.sql import SparkSession
import boto3

import os

# put the csv files into our S3 bucket

if __name__ == "__main__":

    spark = SparkSession.builder.\
                        .appName("spark_step_1").\
                        .getOrCreate()

    # iterate over files into memory and load to S3 bucket

    session = boto3.session(aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    client = session.client("s3") 

    data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")

    for file in os.listdir(data_folder):
        if file.endswith(".csv"):
            response = client.put_object(Bucket = "spark-wcd-test", Body = os.path.join(data_folder, file), Key = file)
