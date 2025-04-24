import requests
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row

URL = "http://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
response = requests.get(URL)

data_dict = {}
if response.status_code == 200:
    try:
        data_dict = response.json()
    except ValueError:
        print("Response is not in JSON format")
else:
    print(f"Request failed with status code {response.status.code}")

#print(json.dumps(data_dict, indent=4))

for obj in data_dict:
    print(obj)
    theme = obj['theme'][0]
    print(f"THEME: {theme}")
    time.sleep(1)
    # do something
    
