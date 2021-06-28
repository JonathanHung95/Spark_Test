from pyspark.sql import SparkSession

import glob

# step 1: read csv files into memory
# step 2: combine csv files into 1 file
# step 3: replace null sales and null units with zero

if __name__ == "__main__":


    spark = SparkSession.builder.\
                        .appName("spark_step_2").\
                        .getOrCreate()

    # read all the csv files into memory

    data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    paths = glob.glob(data_folder + "/trans*.csv", recursive = True)

    csv_df = spark.read.csv(paths, header = True)

    # replace null sales and null units with zero

    csv_df = csv_df.na.fill(value = "0", subset = ["sales", "units"])