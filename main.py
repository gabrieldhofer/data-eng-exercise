import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import array_contains
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


def main():
  spark = SparkSession.builder.getOrCreate()
  df = spark.createDataFrame(get_data())
  filtered = filter_by_hospitals_theme(df)
  case_converted = cols_to_snake_case(filtered)

  case_converted.show()

main()
