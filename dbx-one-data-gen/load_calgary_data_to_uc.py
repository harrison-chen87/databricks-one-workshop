# Databricks notebook source
# MAGIC %md
# MAGIC # Load Calgary Property Data to Unity Catalog
# MAGIC
# MAGIC This notebook reads the split Calgary property assessment CSV files, aggregates them into a single DataFrame, and registers the data as a Unity Catalog table.
# MAGIC
# MAGIC **Parameters**:
# MAGIC - `catalog_name`: Unity Catalog name (default: calgary_real_estate)
# MAGIC - `schema_name`: Schema name (default: property_assessments)
# MAGIC - `table_name`: Table name (default: 2025_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Parameters

# COMMAND ----------

# Get current user's email dynamically
# This allows the notebook to work for any user who downloads this from GitHub
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
print(f"Current user: {current_user}")

# Create widgets for parameterization
dbutils.widgets.text("catalog_name", "calgary_real_estate", "Catalog Name")
dbutils.widgets.text("schema_name", "property_assessments", "Schema Name")
dbutils.widgets.text("table_name", "2025_data", "Table Name")
dbutils.widgets.text("data_path", f"/Workspace/Users/{current_user}/databricks-one-workshop/dbx-one-data-gen/data", "Data Path")

# Get parameter values
CATALOG_NAME = dbutils.widgets.get("catalog_name")
SCHEMA_NAME = dbutils.widgets.get("schema_name")
TABLE_NAME = dbutils.widgets.get("table_name")
DATA_PATH = dbutils.widgets.get("data_path")

FULL_TABLE_NAME = f"{CATALOG_NAME}.{SCHEMA_NAME}.{TABLE_NAME}"

# CSV file configuration
CSV_FILES = [
    f"{DATA_PATH}/calgary_property_data_full_part1.csv",
    f"{DATA_PATH}/calgary_property_data_full_part2.csv",
    f"{DATA_PATH}/calgary_property_data_full_part3.csv",
    f"{DATA_PATH}/calgary_property_data_full_part4.csv"
]

print(f"Target table: {FULL_TABLE_NAME}")
print(f"Number of CSV files to process: {len(CSV_FILES)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
print(f"✓ Catalog '{CATALOG_NAME}' is ready")

# Create schema if it doesn't exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
print(f"✓ Schema '{CATALOG_NAME}.{SCHEMA_NAME}' is ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volume and Copy Files from Workspace

# COMMAND ----------

# Workspace files cannot be directly read by Spark on Express, so we use Unity Catalog Volumes
VOLUME_NAME = "data_files"

# Create volume if it doesn't exist
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")
print(f"✓ Volume '{CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}' is ready\n")

# Define volume path
volume_path = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}"
print(f"Volume path: {volume_path}\n")

# Copy CSV files from Workspace to Volume
volume_csv_files = []
for i, workspace_csv_file in enumerate(CSV_FILES, 1):
    file_name = workspace_csv_file.split("/")[-1]
    volume_file_path = f"{volume_path}/{file_name}"

    print(f"Copying {file_name} to Volume...")
    # Use 'file:' prefix for workspace files
    dbutils.fs.cp(f"file:{workspace_csv_file}", volume_file_path, recurse=False)
    volume_csv_files.append(volume_file_path)

print(f"\n✓ Copied {len(volume_csv_files)} files to Volume successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Aggregate CSV Files

# COMMAND ----------

# Read all CSV parts and combine them
dfs = []

for i, csv_file in enumerate(volume_csv_files, 1):
    print(f"Reading part {i}/{len(volume_csv_files)}: {csv_file}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("escape", "\"") \
        .load(csv_file)

    dfs.append(df)
    print(f"  → Loaded {df.count():,} rows")

# Combine all DataFrames
print("\nCombining all CSV parts...")
combined_df = dfs[0]
for df in dfs[1:]:
    combined_df = combined_df.union(df)

# Get final record count
total_records = combined_df.count()
print(f"\n✓ Total records in combined dataset: {total_records:,}")

# Show schema
print("\nDataFrame Schema:")
combined_df.printSchema()

# Display sample data
print("\nSample Data:")
display(combined_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

# Check for null values in key columns
print("Data Quality Summary:")
print(f"Total rows: {total_records:,}")
print(f"Distinct addresses: {combined_df.select('address').distinct().count():,}")
print(f"Null assessed_values: {combined_df.filter(col('assessed_value').isNull()).count():,}")
print(f"Assessment years: {combined_df.select('roll_year').distinct().collect()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register as Unity Catalog Table

# COMMAND ----------

# Write DataFrame to Unity Catalog table
print(f"Writing data to Unity Catalog table: {FULL_TABLE_NAME}")

combined_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(FULL_TABLE_NAME)

print(f"\n✓ Successfully created table: {FULL_TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Table Creation

# COMMAND ----------

# Verify the table was created
print("Verifying table creation...")
verification_df = spark.table(FULL_TABLE_NAME)
verification_count = verification_df.count()

print(f"\n✓ Table '{FULL_TABLE_NAME}' contains {verification_count:,} records")
print(f"✓ Table location: {spark.sql(f'DESCRIBE DETAIL {FULL_TABLE_NAME}').select('location').first()[0]}")

# Show table properties
print("\nTable Properties:")
spark.sql(f"DESCRIBE EXTENDED {FULL_TABLE_NAME}").show(truncate=False)

# Display sample from the registered table
print("\nSample data from registered table:")
display(spark.table(FULL_TABLE_NAME).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup Volume Files (Optional)

# COMMAND ----------

# Optionally clean up CSV files from Volume
# Note: The files are kept in the Volume by default for future use
# Uncomment the lines below if you want to delete them after loading

# print(f"Cleaning up CSV files in Volume...")
# for csv_file in volume_csv_files:
#     dbutils.fs.rm(csv_file)
# print("✓ Volume CSV files cleaned up successfully")

print("✓ CSV files retained in Volume for future use")
print(f"  Location: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The Calgary property assessment data has been successfully loaded into Unity Catalog!
# MAGIC
# MAGIC You can now query this table using:
# MAGIC ```sql
# MAGIC SELECT * FROM {FULL_TABLE_NAME} LIMIT 100;
# MAGIC ```

# COMMAND ----------
