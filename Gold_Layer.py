# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# I have data of two quarters of the same domain from two consecutive months 
from pyspark.sql.functions import *
try:
    df_q1=spark.read.table("project_01.silver.file1")
    df_q2=spark.read.table("project_01.silver.file2")
    # I want to merge the two dataframes into one dataframe
    df_q1 = df_q1.withColumn("quarter", lit("Q1"))

    df_q2 = df_q2.withColumn("quarter", lit("Q2"))
   
    df_all = df_q1.unionByName(df_q2, allowMissingColumns=True)
    display(df_all) 


except Exception as e:
    print( f"Error :{e}")


# COMMAND ----------

# Quarter wise count 
try:
    df_all.groupBy("quarter").count().show()
except Exception as e:
    print("Error in total orders comparison:", e)

# COMMAND ----------

# Avg rating comparison
try:
    df_all.groupBy("quarter").agg(avg("delivery_person_ratings")).alias("average_rating").show()
except Exception as e:
    print("Error in average rating comparison:", e)

# COMMAND ----------

# 5. City-wise Comparison (how much orders are we getting from each type of city)
try:
    df_all.groupBy("quarter", "city").count().show()
except Exception as e:
    print("Error in city comparison:", e)

# COMMAND ----------

# Traffic comparison

try:
    df_all.groupBy("quarter", "road_traffic_density").count().show()
except Exception as e:
    print("Error in traffic comparison:", e)

# COMMAND ----------

# Vehicle comparison
try:
    df_all.groupBy("quarter", "type_of_vehicle").count().show()
except Exception as e:
    print("Error in vehicle comparison:", e)

# COMMAND ----------

# pivot comparison

try:
    df_all.groupBy("city") \
        .pivot("quarter") \
        .count() \
        .show()
except Exception as e:
    print("Error in pivot comparison:", e)

# COMMAND ----------

# Order type comparison (snack, meal, buffet, drinks)
try:
    df_all.groupBy("quarter", "type_of_order").count() \
        .orderBy("quarter", "type_of_order").show()
except Exception as e:
    print("Error in order type comparison:", e)

# COMMAND ----------

# Weather impact on average ratings and order count
try:
    df_all.groupBy("quarter", "weatherconditions") \
        .agg(
            count("*").alias("order_count"),
            round(avg("delivery_person_ratings"), 2).alias("avg_rating")
        ) \
        .orderBy("quarter", "weatherconditions").show(truncate=False)
except Exception as e:
    print("Error in weather impact analysis:", e)

# COMMAND ----------

# Merging the data for the consecutive two months as a one dataset
try:
    df_all.write.format("delta").mode("overwrite").option("mergeSchema", "true").option("overwriteSchema", "true").saveAsTable("project_01.gold.merged_table_of_quarter1_and_quarter2")

except Exception as e:
    print(f"Error: {e}")