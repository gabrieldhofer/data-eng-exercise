!pip install pyspark
!pip install schedule


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, MapType, BooleanType

metadata_schema = StructType([
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




from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import col
import time
import csv


spark = SparkSession.builder.getOrCreate()


def get_metadata():
  """ Retrieve data via HTTP GET request """
  url = "http://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
  response = requests.get(url)
  if response.status_code == 200:
    try:
      return spark.createDataFrame(response.json(), metadata_schema)
    except ValueError:
      print("Response is not in JSON format")
  else:
    print(f"Request failed with status code {response.status.code}")


def load_metadata(data_location: str):
  """ Read data frome file, inferring schema """
  try:
    print(f"LOADING {data_location}")
    return spark.read.schema(metadata_schema).parquet(data_location)
  except Exception as e: # file doesn't exist return empty DF
    print("ERROR LOADING METADATA_SCHEMA")
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), metadata_schema)


def filter_by_hospitals_theme(df) -> list:
  """ only return rows with theme containing 'Hospitals' """
  return df.filter(array_contains(df.theme, 'Hospitals'))


def update_metadata(df, data_location="metadata.parquet"):
  print("size of dataframe being written: " + str(df.count()))
  df.write.parquet(data_location, mode="overwrite", compression="snappy")


def download(df):
  """ Author: Gabriel Hofer """
  for row in df.rdd.collect():
    print(row)
    downloadURL = row.distribution[0]['downloadURL']
    print(f"downloadURL: {downloadURL}")
    output_filepath = row.identifier + ".csv"
    with requests.Session() as s:
      download = s.get(downloadURL)
      decoded_content = download.content.decode('utf-8')
      cr = csv.reader(decoded_content.splitlines(), delimiter=',')
      with open(output_filepath, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        for row in list(cr):
          writer.writerow(row)
          print("writing row to file:" + str(row))
    time.sleep(4)


def download_row(row):
  print("function: download_row")
  print(row)
  time.sleep(2)
  downloadURL = row.distribution[0]['downloadURL']
  print(f"downloadURL: {downloadURL}")
  output_filepath = row.identifier + ".csv"
  with requests.Session() as s:
    download = s.get(downloadURL)
    decoded_content = download.content.decode('utf-8')
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    with open(output_filepath, 'w', newline='') as outfile:
      writer = csv.writer(outfile)
      for row in list(cr):
        writer.writerow(row)
        print("writing row to file:" + str(row))
  time.sleep(4)


def parallel_download(df):
  """ Author: Gabriel Hofer """
  print("downloading in 1 second...")
  time.sleep(1)
  df.rdd.map(download_row).collect()


def get_inserts(new_metadata_df, metadata_df):
  src_cols = ["a." + x for x in cols]
  return new_metadata_df.alias("a")\
    .join(metadata_df.alias("b"), on="identifier", how="left_anti")\
    .select(*src_cols)


def get_updates(new_metadata_df, metadata_df):
  src_cols = ["a." + x for x in cols]
  return new_metadata_df.alias("a")\
    .join(metadata_df.alias("b"), on="identifier", how="inner")\
    .filter(col("a.modified") > col("b.modified"))\
    .select(*src_cols) #, "tgt.modified")


def get_unchanged(new_metadata_df, metadata_df):
  tgt_cols = ["a." + x for x in cols]
  return metadata_df.alias("a")\
    .join(new_metadata_df.alias("b"), on="identifier", how="inner")\
    .filter(col("a.modified") == col("b.modified"))\
    .select(*tgt_cols)


def preview_data():
  print("METADATA DF")
  metadata_df.show(4)
  print("NEW METADATA DF")
  new_metadata_df.show(4)
  print("INSERTS")
  inserts.show(4)
  print("UPDATES")
  updates.show(4)
  print("UNCHANGED")
  unchanged.show(4)


def union_dfs(inserts, updates, unchanged):
  unioned1 = inserts.unionByName(updates)
  unioned2 = unioned1.unionByName(unioned1)
  unioned3 = unioned2.unionByName(unchanged)
  unioned3.show()
  return unioned3


def job(data_location: str="metadata.parquet"):
  metadata_df = load_metadata(data_location)
  new_metadata_df = filter_by_hospitals_theme(get_metadata())
  inserts = get_inserts(new_metadata_df, metadata_df)
  updates = get_updates(new_metadata_df, metadata_df)
  unchanged = get_unchanged(new_metadata_df, metadata_df)
  preview_data()
  unioned = union_dfs
  parallel_download(inserts.union(updates))
  #df_clean = unioned2.distinct()
  update_metadata(unioned)


job()

