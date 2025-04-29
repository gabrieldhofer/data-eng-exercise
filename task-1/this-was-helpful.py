from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("CDC with Update").getOrCreate()

# Simulating old and new datasets
old_data = [
    ("1", "Alice", "2025-04-27 12:00:00"),
    ("2", "Bob", "2025-04-27 12:00:00"),
]
# new_data = [
#     ("1", "Alice", "2025-04-28 14:00:00"),  # Updated row
#     ("3", "Charlie", "2025-04-28 14:00:00"),  # New row
# ]

new_data = [
    ("1", "Alice2", "2025-04-28 14:00:00"),  # Updated row
    ("3", "Charlie", "2025-04-28 14:00:00"),  # New row
]

# Define schemas for DataFrames
schema = ["id", "name", "update_tsp"]
old_df = spark.createDataFrame(old_data, schema)
new_df = spark.createDataFrame(new_data, schema)

# Perform CDC: Identify updates and new rows
cdc_updates = (
    new_df.alias("new").join(
        old_df.alias("old"), 
        on="id", 
        how="left_anti"
    )
    # .withColumn("operation", lit("INSERT"))
)

cdc_existing = (
    new_df.alias("new").join(old_df.alias("old"), on="id", how="inner")
    .filter(col("new.update_tsp") > col("old.update_tsp"))
    # .withColumn("operation", lit("UPDATE"))
    .withColumn("current_tsp", current_timestamp())  # Update timestamp for updates
)

cdc_retain = old_df.join(new_df, on="id", how="left_anti")

cdc_updates.show()
cdc_existing.show()
cdc_retain.show()

# Align schemas
cdc_updates = cdc_updates.select("id", "name", "update_tsp")
cdc_existing = cdc_existing.select("new.id", "new.name", "new.update_tsp")
cdc_retain = cdc_retain.select("id", "name", "update_tsp")

# Union for final CDC result
cdc_result = cdc_updates\
  .union(cdc_existing)\
  .union(cdc_retain)

# Show results
cdc_result.show()
