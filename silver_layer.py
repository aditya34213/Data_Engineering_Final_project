# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleaning and Transformation
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *

# creating data frames for silver_layer
df1_silver = spark.sql("select * from project_01.bronze.file1")
df2_silver = spark.sql("select * from project_01.bronze.file2")

# COMMAND ----------

display(df1_silver.limit(10))
display(df2_silver.limit(10))

# COMMAND ----------

df1_silver.printSchema()

# COMMAND ----------

df2_silver.printSchema()

# COMMAND ----------

# dropping the duplicates from the both datasets to avoid duplicates
try:
  df1_silver = df1_silver.dropDuplicates()
  df2_silver = df2_silver.dropDuplicates()

except Exception as e:
    print(f"Error:{e}")

print(df1_silver.count())
print(df2_silver.count())

# COMMAND ----------

# Cleaning the column names 
def clean_column_names(df):
    return df.toDF(*[
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ])

try:
    df1_silver = clean_column_names(df1_silver)
    df2_silver= clean_column_names(df2_silver)
except Exception as e:
    print(f"Error:{e}")


# COMMAND ----------

# Iterates through all columns: for c in df.columns: loops through every column name in the provided DataFrame.

from pyspark.sql.functions import *
try:
     def standardize_columns(df):
         for c in df.columns:
             df = df.withColumn(c, trim(col(c)))   #trim all whitespaces after and before the column values
         return df
     
     df1_silver = standardize_columns(df1_silver)
     df2_silver = standardize_columns(df2_silver)

except Exception as e:
    print(f"Error:{e}")



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
    
try:
    df1_silver = normalize_nulls(df1_silver)
    df2_silver = normalize_nulls(df2_silver)    

except Exception as e:
    print(f"Error:{e}")

# COMMAND ----------

try:
    # Drop rows where critical IDs are missing
    df1_silver = df1_silver.fillna({
    "id": "unknown",
    "delivery_person_id": "unknown"})

    df2_silver = df2_silver.fillna({
    "id": "unknown",
    "delivery_person_id": "unknown"})

    # Finding medians (0.5 represents the 50th percentile)
    df1_silver = df1_silver.withColumn(
    "delivery_person_age",
    expr("try_cast(delivery_person_age as int)"))

    df1_silver = df1_silver.withColumn(
    "delivery_person_ratings", 
    expr("try_cast(delivery_person_ratings as decimal(10,2))"))

    # this code will convert the None, nan,Null to null
    # basically try to convert the column values in int and decimal if not able to convert then it will convert to null
    # Automatically converts bad values (none, nan) → NULL
    # Had nan, Nan, NAN instead of null values so converted with trycast if its not int it will convert to null

    df2_silver = df2_silver.withColumn(
    "delivery_person_age",
    expr("try_cast(delivery_person_age as int)"))


    df2_silver = df2_silver.withColumn(
    "delivery_person_ratings", 
    expr("try_cast(delivery_person_ratings as decimal(10,2))"))

    res1 = df1_silver.approxQuantile(["delivery_person_age", "delivery_person_ratings"], [0.5], 0.01)
    res2 = df2_silver.approxQuantile(["delivery_person_age", "delivery_person_ratings"], [0.5], 0.01)
    
    # 4. Extract values safely (Checking if list is not empty to avoid IndexErrors)
    m1_age = res1[0][0] if res1[0] else 30.0
    m1_rat = res1[1][0] if res1[1] else 4.5
    
    m2_age = res2[0][0] if res2[0] else 30.0
    m2_rat = res2[1][0] if res2[1] else 4.5

    # 5. Fillna with the extracted scalars
    df1_silver = df1_silver.fillna({"delivery_person_age": m1_age, "delivery_person_ratings": m1_rat})
    df2_silver = df2_silver.fillna({"delivery_person_age": m2_age, "delivery_person_ratings": m2_rat})
#  in the column of age and ratings will update with medians this how we dealt with null here

except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

from pyspark.sql.functions import *

try:
    df1_silver = df1_silver.fillna({
        "city": "Unknown",
        "road_traffic_density": "Unknown",
        "festival19": "No",
        "festival17": "No"
    })

except Exception as e:
    print(f"Error: {e}")

try:
    df2_silver = df2_silver.fillna({
        "city": "Unknown",
        "road_traffic_density": "Unknown",
    })

except Exception as e:
    print(f"Error: {e}")



# COMMAND ----------

try:
             
             # 1. Cast the column to string so it can hold the word "Unknown"
             df1_silver = df1_silver.withColumn("time_orderd", col("time_orderd").cast("string"))
             df2_silver = df2_silver.withColumn("time_orderd", col("time_orderd").cast("string"))
             
             # 2. Replace actual NULLs or empty strings with "Unknown"
             df1_silver = df1_silver.withColumn(
                 "time_orderd", 
                 when(col("time_orderd").isNull() | (col("time_orderd") == ""), lit("Unknown"))
                 .otherwise(col("time_orderd"))
             )
             
             df2_silver = df2_silver.withColumn(
                 "time_orderd", 
                 when(col("time_orderd").isNull() | (col("time_orderd") == ""), lit("Unknown"))
                 .otherwise(col("time_orderd"))
             )

except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

try:   
   def lowercase_values(df):
       for c, dtype in df.dtypes:
           if dtype == "string":
               df = df.withColumn(c, lower(col(c)))
       return df
   
   df1_silver = lowercase_values(df1_silver)
   df2_silver = lowercase_values(df2_silver)

except Exception as e:
   print(f"Error: {e}")

# COMMAND ----------

try:
   
    df1_nulls = df1_silver.select([
        sum(when(col(c).isNull() | (col(c) == ""), 1).otherwise(0)).alias(c)
        if isinstance(df1_silver.schema[c].dataType, StringType)
        else sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df1_silver.columns
    ]).collect()[0].asDict()
    
    df2_nulls = df2_silver.select([
        sum(when(col(c).isNull() | (col(c) == ""), 1).otherwise(0)).alias(c)
        if isinstance(df2_silver.schema[c].dataType, StringType)
        else sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df2_silver.columns
    ]).collect()[0].asDict()
    
    print(df1_nulls)
    print(df2_nulls)

except Exception as e:
    print(f"Error: {e}")


# COMMAND ----------

# writing table in adls
try:   
   df1_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").saveAsTable("project_01.silver.file1")
   
   df2_silver.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").saveAsTable("project_01.silver.file2")
   display(df1_silver)
   display(df2_silver)

except Exception as e:
    print(f"Error: {e}")


