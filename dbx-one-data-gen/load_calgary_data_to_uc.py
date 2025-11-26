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
# MAGIC ## Copy Files from Workspace to DBFS

# COMMAND ----------

# Workspace files cannot be directly read by Spark, so we copy them to DBFS first
temp_dbfs_path = "/tmp/calgary_property_data"

# Create temp directory in DBFS
dbutils.fs.mkdirs(temp_dbfs_path)
print(f"Created temporary DBFS directory: {temp_dbfs_path}\n")

# Copy CSV files from Workspace to DBFS
dbfs_csv_files = []
for i, workspace_csv_file in enumerate(CSV_FILES, 1):
    file_name = workspace_csv_file.split("/")[-1]
    dbfs_file_path = f"{temp_dbfs_path}/{file_name}"

    print(f"Copying {file_name} to DBFS...")
    # Use 'file:' prefix for workspace files
    dbutils.fs.cp(f"file:{workspace_csv_file}", dbfs_file_path, recurse=False)
    dbfs_csv_files.append(dbfs_file_path)

print(f"\n✓ Copied {len(dbfs_csv_files)} files to DBFS successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read and Aggregate CSV Files

# COMMAND ----------

# Read all CSV parts and combine them
dfs = []

for i, csv_file in enumerate(dbfs_csv_files, 1):
    print(f"Reading part {i}/{len(dbfs_csv_files)}: {csv_file}")

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
# MAGIC ## Cleanup Temporary Files

# COMMAND ----------

# Clean up temporary DBFS files
print(f"Cleaning up temporary files in {temp_dbfs_path}...")
dbutils.fs.rm(temp_dbfs_path, recurse=True)
print("✓ Temporary files cleaned up successfully")

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
