# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Report
# MAGIC

# COMMAND ----------

# firstly I have compared raw files without doing any transformations on it because
# Hid.e real issues
# Make bad data look correct
# Lose auditability
# I compared raw or bronze data first to identify discrepancies. Cleaning is done later in the silver layer to ensure that validation reflects true data quality issues.”

from pyspark.sql.functions import *
from pyspark.sql.types import *

df1=spark.read.table("project_01.bronze.file1")
df2=spark.read.table("project_01.bronze.file2")


row_count_1 = df1.count()
row_count_2 = df2.count()
print(row_count_1)
print(row_count_2)


# COMMAND ----------

# Column Count
df1_col_count = len(df1.columns)
df2_col_count = len(df2.columns)
print(df1_col_count)
print(df2_col_count)

# Also check which new column added in which file
df1_cols = set(df1.columns)
df2_cols = set(df2.columns)

only_in_df1 = df1_cols - df2_cols
only_in_df2 = df2_cols - df1_cols

common_cols = df1_cols.intersection(df2_cols)
print(common_cols)

# COMMAND ----------

# we have Nan, NONE, nan like values in the data instead of of directly nulls so we have to convert them to null
def normalize_nulls(df):
    null_values = ["none", "null", "nan", ""]

    for c in df.columns:
        df = df.withColumn(
            c,
            when(
                lower(trim(col(c))).isin(null_values),
                None
            ).otherwise(col(c))
        )
    return df

df1 = normalize_nulls(df1)
df2= normalize_nulls(df2)

display(df1)
display(df2) 

# alternate

# # Define the list of values that should be treated as NULL
# null_values = ["nan", "NAN", "Nan", "NONE", "none", ""]

# # Iterate through all columns and replace matches with a true NULL
# for column in df1.columns:
#     df1 = df1.withColumn(column, when(col(column).isin(null_values), lit(None)).otherwise(col(column)))

# for column in df2.columns:
#     df2 = df2.withColumn(column, when(col(column).isin(null_values), lit(None)).otherwise(col(column)))
# this is done for future data

df1_nulls = df1.select([
    sum(when(col(c).isNull() | (col(c) == ""), 1).otherwise(0)).alias(c)
    if isinstance(df1.schema[c].dataType, StringType)
    else sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df1.columns
]).first().asDict()

df2_nulls = df2.select([
    sum(when(col(c).isNull() | (col(c) == ""), 1).otherwise(0)).alias(c)
    if isinstance(df2.schema[c].dataType, StringType)
    else sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df2.columns
]).first().asDict()

print(df1_nulls)
print(df2_nulls)

# COMMAND ----------

display(df1)

# COMMAND ----------

# count of distnct values in each column
df1_distinct = {}
df2_distinct = {}

for c in common_cols:
    df1_distinct[c] = df1.select(countDistinct(c)).first()[0]
    df2_distinct[c] = df2.select(countDistinct(c)).first()[0]

print(df1_distinct)
print(df2_distinct)

# COMMAND ----------

# Uniqueness ratio of each column
df1_uniqueness = {}
df2_uniqueness = {}

for c in common_cols:
    df1_uniqueness[c] = (df1_distinct[c] / row_count_1) * 100 if row_count_1 else 0
    df2_uniqueness[c] = (df2_distinct[c] / row_count_2) * 100 if row_count_2 else 0

print(df1_uniqueness)
print(df2_uniqueness)

# COMMAND ----------

# for statistical differnces
from pyspark.sql.functions import mean

numeric_cols_df1 = [f.name for f in df1.schema.fields if "Double" in str(f.dataType) or "Integer" in str(f.dataType) or "Long" in str(f.dataType)]

numeric_cols_df2 = [f.name for f in df2.schema.fields if "Double" in str(f.dataType) or "Integer" in str(f.dataType) or "Long" in str(f.dataType)]


df1_mean = {}
df2_mean = {}

for c in numeric_cols_df1:
    df1_mean[c] = df1.select(mean(c)).first()[0]

for c in numeric_cols_df2:
    df2_mean[c] = df2.select(mean(c)).first()[0]

print(df1_mean)
print(df2_mean)

# COMMAND ----------

# Calculating the standard deviation for each numeric column
df1_stddev = df1.select([stddev(c).alias(c) for c in numeric_cols_df1]).first().asDict()
df2_stddev = df2.select([stddev(c).alias(c) for c in numeric_cols_df2]).first().asDict()

print(df1_stddev)
print(df2_stddev)

# COMMAND ----------

# Calculating the min value for each column
df1_min = df1.select([min(c).alias(c) for c in numeric_cols_df1]).first().asDict()
df2_min = df2.select([min(c).alias(c) for c in numeric_cols_df2]).first().asDict()

print(df1_min)
print(df2_min)

# COMMAND ----------

# Calculating the max value for each column
df1_max = df1.select([max(c).alias(c) for c in numeric_cols_df1]).first().asDict()
df2_max = df2.select([max(c).alias(c) for c in numeric_cols_df2]).first().asDict()

print(df1_max)
print(df2_max)

# COMMAND ----------

# Calculate all medians at once and convert to a dictionary
from pyspark.sql.functions import median

df1_median = df1.select([
    median(c).alias(c) for c in numeric_cols_df1
]).first().asDict()

print(df1_median)

# COMMAND ----------

metrics = {
    "row_count": {"df1": row_count_1, "df2": row_count_2},
    "column_count": {"df1": df1_col_count, "df2": df2_col_count},
    "mean": {"df1": df1_mean, "df2": df2_mean},
    "stddev": {"df1": df1_stddev, "df2": df2_stddev},
    "min": {"df1": df1_min, "df2": df2_min},
    "max": {"df1": df1_max, "df2": df2_max},
    "null_count": {"df1": df1_nulls, "df2": df2_nulls},
    "distinct_count": {"df1": df1_distinct, "df2": df2_distinct},
    "uniqueness_pct": {"df1": df1_uniqueness, "df2": df2_uniqueness}
}

print(metrics)

# COMMAND ----------

# Validation Report
# If values gets matched then it would return pass otherwise Fail

report = []

for metric_name, values in metrics.items():
    val1 = values["df1"]
    val2 = values["df2"]

    # Case 1: Scalar values (row_count, column_count) we dont have these values in dicrionary format
    if not isinstance(val1, dict):
        diff = val1 - val2
        status = "PASS" if diff == 0 else "FAIL"
        
        report.append((metric_name, None, float(val1), float(val2), float(diff), status))
    
    # Case 2: Dictionary values (column-level metrics)
    # would give zero if there is no value for that column
    else:
        all_cols = set(val1.keys()).union(set(val2.keys()))
        
        for col_name in all_cols:
            v1 = float(val1.get(col_name, 0) or 0)
            v2 = float(val2.get(col_name, 0) or 0)
            
            diff = v1 - v2
            status = "PASS" if diff == 0 else "FAIL"
            report.append((metric_name, col_name, v1, v2, diff, status))

# print(report)
report_df = spark.createDataFrame(
    report,
    ["metric_name", "column_name", "df1_value", "df2_value", "difference", "status"]
)

display(report_df)


# COMMAND ----------

# path = "abfss://project@adistore33.dfs.core.windows.net/validation_table"

# report_df.coalesce(1).write.mode("overwrite").option("header", "true").option("overwriteSchema", "true").option("path", path).saveAsTable("project_01.validation.report_table") This would get stored as delta table


# The issue is that saveAsTable() expects a Unity Catalog table name (like project_01.bronze.validation_table), not an ABFSS path. To write directly to ADLS, so we have used save() instead

report_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("project_01.validation.report_table")  # This would get stored as delta table


# This will create a single csv file in adls coalesce to avoid multiple partitions
report_df.coalesce(1) \
    .write.mode("overwrite") \
    .option("header", True) \
    .csv("abfss://project@adistore33.dfs.core.windows.net/validation_table/report_csv")


# COMMAND ----------

# MAGIC %md
# MAGIC