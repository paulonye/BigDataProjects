import time
import pyspark
from pyspark.sql import SparkSession


def test_spark():
    """This creates a temporary spark session, and kills it once the process exists"""
    spark = SparkSession.builder \
        .appName('test') \
        .getOrCreate()

    df = spark.read \
        .option("header", "true") \
        .parquet('gs://de-zoomcamp-paul/nyc_data.parquet') 
        #.repartition(4)

    #df = df.repartition(24)

    print(df.count())
    df = df.groupby('PULocationID').count()

    df.write.parquet('gs://de-zoomcamp-paul/results3/', mode='overwrite')

if __name__ == '__main__':
    start_time = time.time()
    test_spark()
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Elapsed time: {elapsed_time:.2f} seconds")
