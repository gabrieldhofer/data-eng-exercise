import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import array_contains
from pyspark.sql import DataFrame
import re
from pyspark.sql.functions import current_timestamp
import schedule
from pytz import timezone
import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, BooleanType
from pyspark.sql.functions import col
import pandas


cols = [
    "@type", "access_level", "bureau_code", "contact_point", "description", 
    "distribution", "identifier", "issued", "keyword", "landing_page", 
    "modified", "program_code", "publisher", "released", "theme", "title", 
    "archive_exclude", "next_update_date"
]

schema = StructType([
    StructField('@type', StringType(), True), 
    StructField('access_level', StringType(), True), 
    StructField('bureau_code', StringType(), True), 
    StructField('contact_point', StringType(), True), 
    StructField('description', StringType(), True), 
    StructField('distribution', StringType(), True), 
    StructField('identifier', StringType(), True), 
    StructField('issued', StringType(), True), 
    StructField('keyword', StringType(), True), 
    StructField('landing_page', StringType(), True), 
    StructField('modified', StringType(), True), 
    StructField('program_code', StringType(), True), 
    StructField('publisher', StringType(), True), 
    StructField('released', StringType(), True), 
    StructField('theme', StringType(), True), 
    StructField('title', StringType(), True), 
    StructField('archive_exclude', BooleanType(), True), 
    StructField('next_update_date', StringType(), True)
])

schema2 = StructType([
    StructField('@type', StringType(), True), 
    StructField('accessLevel', StringType(), True), 
    StructField('bureauCode', StringType(), True), 
    StructField('contactPoint', StringType(), True), 
    StructField('description', StringType(), True), 
    StructField('distribution', StringType(), True), 
    StructField('identifier', StringType(), True), 
    StructField('issued', StringType(), True), 
    StructField('keyword', StringType(), True), 
    StructField('landingPage', StringType(), True), 
    StructField('modified', StringType(), True), 
    StructField('programCode', StringType(), True), 
    StructField('publisher', StringType(), True), 
    StructField('released', StringType(), True), 
    StructField('theme', StringType(), True), 
    StructField('title', StringType(), True), 
    StructField('archiveExclude', BooleanType(), True), 
    StructField('nextUpdateDate', StringType(), True)
])

def get_data() -> list:
  """ Retrieve data via HTTP GET request """
  URL = "http://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
  response = requests.get(URL)
  data_dict = {}
  if response.status_code == 200:
      try:
          return response.json()
      except ValueError:
          print("Response is not in JSON format")
  else:
      print(f"Request failed with status code {response.status.code}")


def filter_by_hospitals_theme(df) -> list:
  """ only return rows with theme containing 'Hospitals' """
  #return df.filter(array_contains(df.theme, 'Hospitals'))
  return df.filter(col("theme").contains("Hospitals"))


def cols_to_snake_case(df) -> None:
  """ convert column names to snake case """
  for col in df.columns:
      new_col = re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower()
      df = df.withColumnRenamed(col, new_col)
  return df


def read_tgt_df(spark, data_location="data.csv"):
  return spark.read.csv(
    data_location, 
    header=True, 
    schema=schema
    #inferSchema=True
  )
  
def write_tgt_df(tgt_df, data_location="data.csv"):
  pandas_df = tgt_df.toPandas()

  # Write to CSV file
  pandas_df.to_csv("data.csv", index=False)
  

def upsert(tgt_df, src_df):

  # Perform CDC: Identify new rows
  cdc_updates = src_df.alias("src")\
    .join(
        tgt_df.alias("tgt"), 
        on="identifier", 
        how="left_anti"
    )    

  # Identify updates
  cdc_existing = src_df.alias("src")\
    .join(tgt_df.alias("tgt"), on="identifier", how="inner")\
    .filter(col("src.modified") >= col("tgt.modified"))
  #  .withColumn("current_tsp", current_timestamp())  # Update timestamp for updates

  # Keep rows that weren't in the new df (no deletions)
  cdc_retain = tgt_df.alias("tgt").join(src_df.alias("src"), on="identifier", how="left_anti")

  print("cdc_updates: new records in SRC dataframe")
  cdc_updates.show()
  print("cdc_existing: records that were already in file, but are also in the SRC df and have a more recent 'modified' tsp")
  cdc_existing.show()
  print("cdc_retain: records that were already in TGT, and aren't being updated")
  cdc_retain.show()
  
  src_cols = ["src." + x for x in cols]
  tgt_cols = ["tgt." + x for x in cols]
  
  # Align schemas
  cdc_updates = cdc_updates.select(*src_cols)
  cdc_existing = cdc_existing.select(*src_cols)
  cdc_retain = cdc_retain.select(*tgt_cols)

  # Union for final CDC result
  cdc_result = cdc_updates\
    .union(cdc_existing)\
    .union(cdc_retain)
  return cdc_result



def job():
  """ author: Gabriel Hofer """
  spark = SparkSession.builder.getOrCreate()
  tgt_df = read_tgt_df(spark)
  print("1. TGT_DF")
  tgt_df.show()
  print("row count: " + str(tgt_df.count()))

  src_df = spark.createDataFrame(get_data(), schema2)
  print("2. SRC_DF")
  src_df.show()
  print("row count: " + str(src_df.count()))
  
  filtered = filter_by_hospitals_theme(src_df)
  print("3. filtered")
  filtered.show()

  case_converted = cols_to_snake_case(filtered)
  print("4. case_converted")
  case_converted.show()
  
  new_tgt_df = upsert(tgt_df, case_converted)
  print("5. new_tgt_df")
  new_tgt_df.show()

  print("row count: " + str(new_tgt_df.count()))

  write_tgt_df(new_tgt_df)
  spark.stop()


def main(data_location="data.csv"):

  # get new data every minute
  #schedule.every(1).minute.at("17:32:00", timezone("America/Chicago")).do(job)
  schedule.every(10).seconds.at("18:32:00", timezone("America/Chicago")).do(job)
  while True:
    schedule.run_pending()
    time.sleep(1)

#main()
job()
