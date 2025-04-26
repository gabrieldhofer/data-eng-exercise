import requests
import json
import time
## from pyspark.sql import SparkSession
## from pyspark.sql import Row
## from pyspark.sql.functions import array_contains
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

#main()

import schedule

def job():
    print("**job**")

schedule.every(5).seconds.do(job)


## import airflow
## from datetime import datetime, timedelta
## #from airflow.operators.python import PythonOperator
## from airflow.providers.standard.operators.python import PythonOperator
## 
## 
## default_args = {
##     'owner': 'airflow',
##     'depends_on_past': False,
##     'start_date': datetime(year=2025, month=4, day=25),
##     'email': ['gabrieldhofer@gmail.com'],
##     'email_on_failure': False,
##     'email_on_retry': False,
##     'retries': 1,
##     'retry_delay': timedelta(minutes=5),
##     #'schedule_interval': '@daily',
##     'schedule_interval':'*/2 * * * *',
## }
## 
## dag = DAG(
##     'tutorial', 
##     description='Retrieving Hospital themed data daily',
##     catchup=False, 
##     default_args=default_args
## )
## 
## # Define Python task
## main_task = PythonOperator(
##     task_id='main_daily',  # Unique task ID
##     python_callable=main,  # Function to execute
##     dag=dag,  # Assign task to DAG
## )


