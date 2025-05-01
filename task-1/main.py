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
import csv


cols = [
  "@type",
  "access_level",
  "bureau_code",
  "contact_point",
  "description",
  "distribution",
  "identifier",
  "issued",
  "keyword",
  "landing_page",
  "modified",
  "program_code",
  "publisher",
  "released",
  "theme",
  "title",
  "archive_exclude",
  "next_update_date"
]


schema_snake = StructType([
  StructField('@type', StringType(), True),
  StructField('access_level', StringType(), True),
  StructField('bureau_code', ArrayType(StringType(), True), True),
  StructField('contact_point', MapType(StringType(), StringType(), True), True),
  StructField('description', StringType(), True),
  StructField('distribution', ArrayType(MapType(StringType(), StringType(), True), True), True),
  StructField('identifier', StringType(), True), StructField('issued', StringType(), True),
  StructField('keyword', ArrayType(StringType(), True), True),
  StructField('landing_page', StringType(), True),
  StructField('modified', StringType(), True),
  StructField('program_code', ArrayType(StringType(), True), True),
  StructField('publisher', MapType(StringType(), StringType(), True), True),
  StructField('released', StringType(), True),
  StructField('theme', ArrayType(StringType(), True), True),
  StructField('title', StringType(), True),
  StructField('next_update_date', StringType(), True),
  StructField('archive_exclude', BooleanType(), True)
])


schema_camel = StructType([
  StructField('@type', StringType(), True),
  StructField('accessLevel', StringType(), True),
  StructField('bureauCode', ArrayType(StringType(), True), True),
  StructField('contactPoint', MapType(StringType(), StringType(), True), True),
  StructField('description', StringType(), True),
  StructField('distribution', ArrayType(MapType(StringType(), StringType(), True), True), True),
  StructField('identifier', StringType(), True), StructField('issued', StringType(), True),
  StructField('keyword', ArrayType(StringType(), True), True),
  StructField('landingPage', StringType(), True),
  StructField('modified', StringType(), True),
  StructField('programCode', ArrayType(StringType(), True), True),
  StructField('publisher', MapType(StringType(), StringType(), True), True),
  StructField('released', StringType(), True),
  StructField('theme', ArrayType(StringType(), True), True),
  StructField('title', StringType(), True),
  StructField('nextUpdateDate', StringType(), True),
  StructField('archiveExclude', BooleanType(), True)
])


def get_data(schema):
  """ Retrieve data via HTTP GET request """
  URL = "http://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
  response = requests.get(URL)
  if response.status_code == 200:
    try:
      return spark.createDataFrame(response.json(), schema)
    except ValueError:
      print("Response is not in JSON format")
  else:
    print(f"Request failed with status code {response.status.code}")


def filter_by_hospitals_theme(df) -> list:
  """ only return rows with theme containing 'Hospitals' """
  return df.filter(array_contains(df.theme, 'Hospitals'))


def cols_to_snake_case(df) -> None:
  """ convert column names to snake case """
  for col in df.columns:
    new_col = re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower()
    df = df.withColumnRenamed(col, new_col)
  return df


def read_tgt_df(spark, data_location="metadata.parquet"):
  return spark.read.schema(schema_snake).parquet(data_location)


def write_tgt_df(tgt_df, data_location="metadata.parquet"):
  tgt_df.write.parquet(data_location, mode="overwrite", compression="snappy")


def download(df):
  """ Author: Gabriel Hofer """
  print("downloading in 5 seconds...")
  time.sleep(5)
  for row in df.rdd.collect():
    print(row)
    downloadURL = row.distribution[0]['downloadURL']
    output_filepath = row.identifier + ".csv"
    with requests.Session() as s:
      download = s.get(downloadURL)
      decoded_content = download.content.decode('utf-8')
      cr = csv.reader(decoded_content.splitlines(), delimiter=',')
      with open(output_filepath, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        my_list = list(cr)
        for row in my_list:
          writer.writerow(row)
          print("writing row to file:" + str(row))


def upsert(tgt_df, src_df):
  """
    Author: Gabriel Hofer

    This function 'merges' the source data to the target data.

    The source dataframe is the data that was retrieved via the most recent
    HTTP request.

    The target dataframe is read from a file stored locally.

    If there are records not in the target dataframe but they do exist in the
    source dataframe, those new records are added to the target df.

    if there are records in the target and source dataframes with matching
    'identifiers' (column), then the record with the most recent 'modified'
    value is stored back into the resulting target dataframe.

    Records are not deleted from the target or the source, i.e. the
    target dataframe and number of rows stored locally is always the same
    or increasing.

  """
  inserts = src_df.alias("src")\
    .join(tgt_df.alias("tgt"), on="identifier", how="left_anti")

  updates = src_df.alias("src")\
    .join(tgt_df.alias("tgt"), on="identifier", how="inner")\
    .filter(col("src.modified") >= col("tgt.modified"))

  unchanged = tgt_df.alias("tgt")\
    .join(src_df.alias("src"), on="identifier", how="left_anti")

  src_cols = ["src." + x for x in cols]
  tgt_cols = ["tgt." + x for x in cols]

  inserts = inserts.select(*src_cols)
  updates = updates.select(*src_cols)
  unchanged = unchanged.select(*tgt_cols)

  download(inserts)
  download(updates)

  try:
    res = inserts.union(updates).union(unchanged)
  except Exception as e:
    print(e)
  return res


def job():
  """ Author: Gabriel Hofer """
  spark = SparkSession.builder.getOrCreate()
  tgt_df = read_tgt_df(spark)
  src_df = get_data(schema_camel)
  filtered = filter_by_hospitals_theme(src_df)
  case_converted = cols_to_snake_case(filtered)
  new_tgt_df = upsert(tgt_df, case_converted)
  write_tgt_df(new_tgt_df)
  spark.stop()


def main(data_location="metadata.parquet"):
  schedule.every().day.at("20:43:00", timezone("America/Chicago")).do(job)
  while True:
    schedule.run_pending()
    time.sleep(1)


#main()
job()
