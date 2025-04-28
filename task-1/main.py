import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import array_contains
from pyspark.sql import DataFrame
import re

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
  return df.filter(array_contains(df.theme, 'Hospitals'))


def cols_to_snake_case(df) -> None:
  """ convert column names to snake case """
  for col in df.columns:
      new_col = re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower()
      df = df.withColumnRenamed(col, new_col)
  return df


def job():
  spark = SparkSession.builder.getOrCreate()
  df = spark.createDataFrame(get_data())
  filtered = filter_by_hospitals_theme(df)
  case_converted = cols_to_snake_case(filtered)
  case_converted.show()

# https://schedule.readthedocs.io/en/stable/examples.html
# https://schedule.readthedocs.io/en/stable/timezones.html

import schedule
from pytz import timezone
import datetime

#schedule.every(5).seconds.do(job)

def main():
  schedule.every().day.at("12:11:00", timezone("America/Chicago")).do(job)
  while True:
    schedule.run_pending()
    time.sleep(30)

def main2(data_location="data.csv"):
  # load dataframe from file
  df = spark.read.csv(
      data_location, 
      header=True, 
      inferSchema=True
  )

  # get new data every minute
  schedule.every(1).minute.at("8:20:00", timezone("America/Chicago")).do(job)
  while True:
    schedule.run_pending()
    time.sleep(1)

  # write updated dataframe to the same file 
  df.write.csv(data_location, header=True, mode="overwrite")
  spark.stop()

#main()
