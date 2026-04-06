# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion

# COMMAND ----------

def ingest_csv_from_adls(base_folder: str):
    """
    Ingests all CSV files from a given ADLS folder into Spark DataFrames.
    Parameters:
    base_folder : str
        ABFSS path to the folder containing CSV files.
        Example:
        abfss://<container>@<storage-account>.dfs.core.windows.net/raw_data/
    Function Workflow:
    1. Lists all files in the given ADLS folder
    2. Filters only CSV files
    3. Reads each CSV into a Spark DataFrame
    4. Displays the data
    5. Handles errors
    """

    print(f"\n Starting ingestion from: {base_folder}")
    
    dfs = {}  # dictionary to store dataframes
    try:
        files = dbutils.fs.ls(base_folder)
        print(f"Found {len(files)} files in the directory\n")
    except Exception as e:
        print(f"ERROR: Unable to access folder: {base_folder}")
        print(f"Reason: {str(e)}")
        return {}
    csv_files = [file for file in files if file.name.endswith(".csv")]
    #   if file is not csv then it will return an error
    if not csv_files:  
        print(" No CSV files found in the directory.")
        return {}
    print(f" CSV files to process: {len(csv_files)}\n")
    if len(csv_files) != 2:
        raise Exception("Exactly 2 CSV files are required for this process")

    for i, file in enumerate(csv_files, start=1):
        file_path = file.path
        print(f"Processing file: {file.name}")
        try:
            df = spark.read.format("csv") \
                .option("header", True) \
                .option("inferSchema", True) \
                .load(file_path)

            df_name = f"df{i}"   # will name files as df1 and df2 instead of hardcoding it for future 

            dfs[df_name] = df   
            display(df)
            print(f"{df_name} created for file: {file.name}")
        except Exception as e:
            print(f"ERROR processing file: {file.name}")
            print(f"Reason: {str(e)}\n")
            continue
    print("\n Ingestion process completed.\n")

    return dfs

# correct function call
base_folder = "abfss://project@adistore33.dfs.core.windows.net/raw_data/"
dfs = ingest_csv_from_adls(base_folder)

# COMMAND ----------

files = dbutils.fs.ls(base_folder)

print("FILES FOUND:", len(files))
for f in files:
    print(f.name)

# COMMAND ----------

# fetching datasets from the dictionary
df1 = dfs["df1"]
df2 = dfs["df2"]
dbutils.fs.ls(base_folder)

# COMMAND ----------

df1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true").saveAsTable("project_01.bronze.file1")
# option("delta.enableChangeDataFeed", "true") This feature allows the Delta Lake transaction log to record row-level changes (inserts, updates, deletes) for all data written into the table
# overwriteSchema helps to get overwrite the schema  as new file drops the delete the schema of the old file and takes new schema

df2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("delta.enableChangeDataFeed", "true").saveAsTable("project_01.bronze.file2")


# COMMAND ----------

# deleting all files after uploading

base_folder = "abfss://project@adistore33.dfs.core.windows.net/raw_data/"
archive_path = "abfss://project@adistore33.dfs.core.windows.net/processed/archieved_data/"

files = dbutils.fs.ls(base_folder)
# print(files)

# [FileInfo(path='abfss://project@adistore33.dfs.core.windows.net/raw_data/logistics_q2.csv', name='logistics_q2.csv', size=772404, modificationTime=1775064966000)] from this file.name id logistics_q2.csv and path is abfss one delete it after uploading

for file in files:
    if file.name.endswith(".csv"):   # optional filter
        
        source_path = file.path
        destination_path = archive_path + file.name
    
        dbutils.fs.mv(source_path, destination_path)

print(destination_path)