data-engineering-assessment
===========================

Description
-----------

Given the CMS provider data metastore, write a script that downloads all data sets related to the theme "Hospitals".

The column names in the csv headers are currently in mixed case with spaces and special characters. Convert all column names to snake_case. (Example: "Patients' rating of the faciilty linear mean score" becomes "patients_rating_of_the_facility_linear_mean_score").

The csv files should be downloaded and processed in parallel, and the job should be designed to run every day, but only download files that have been modified since the previous run (need to track runs/metadata).

https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items

Usage:
------

This program was developed in Google Colab.

Install dependencies:
```bash
    $ pip install -r ./requirements.txt

    $ make clean

    $ make init
```
Then, run the "make" command to run main.py.
```bash
    $ make
```

Scheduling
----------
```python
    def main(data_location="metadata.parquet"):
      schedule.every().day.at("20:43:00", timezone("America/Chicago")).do(job)
      while True:
        schedule.run_pending()
        time.sleep(1)
```
Job
---
```python
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
```
Reading & Writing DataFrames
----------------------------
```python
    def read_tgt_df(spark, data_location="metadata.parquet"):
      return spark.read.schema(schema_snake).parquet(data_location)
    
    
    def write_tgt_df(tgt_df, data_location="metadata.parquet"):
      tgt_df.write.parquet(data_location, mode="overwrite", compression="snappy")
```
Convert cols to Snake Case
--------------------------
```python    
    def cols_to_snake_case(df) -> None:
      """ convert column names to snake case """
      for col in df.columns:
        new_col = re.sub(r"(?<!^)(?=[A-Z])", "_", col).lower()
        df = df.withColumnRenamed(col, new_col)
      return df
```
