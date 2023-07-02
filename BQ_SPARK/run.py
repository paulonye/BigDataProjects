import os
from pathlib import Path
import json
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from pyspark.sql import SparkSession


@task(retries = 3, tags = ['Extract'])
def get_data_from_bq(years: list) -> str:
    """Extracts data from BIGQUERY INTO SPARK"""
    bq_key = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.23.2"
    spark = SparkSession.builder \
        .appName("Read from BigQuery") \
        .config("spark.jars.packages", bq_key) \
        .getOrCreate()
        
    count = 0
    for year in years:
        df = spark.read.format("bigquery") \
        .option("table", f"bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_{year}") \
        .load()

        count = count + df.count()
        print(count)
    print(count)
    return json.dumps({"Length": count})
 
@task(retries = 3, tags = ['Transform'])
def transform_job(count: str) -> Path:
    #count the number of records
    data = json.loads(count)
    df = pd.DataFrame.from_records([data])
    df.to_csv("./testquidax.csv")
    path = Path("./testquidax.csv")
    return path

@task(retries = 3, tags = ['Load'])
def load_data_to_gcs(path: Path) -> None:
    """Upload the datafile into GCS"""

    #This is used to call the module for initializing GCS from prefect
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)

@flow()
def run_job(years: list):
    count  =  get_data_from_bq(years)
    path = transform_job(count)
    load_data_to_gcs(path)
    print("Done")

if __name__ == '__main__':
    years = [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]
    run_job(years)


    






