-- Databricks notebook source
create external location if not exists ext_loc
URL 'abfss://project@adistore33.dfs.core.windows.net/raw_data'
with (storage credential adi_cred)

-- COMMAND ----------

desc external location ext_loc

-- COMMAND ----------

create external location if not exists ext_loc1
URL 'abfss://project@adistore33.dfs.core.windows.net/processed'
with (storage credential adi_cred)

-- COMMAND ----------

desc external location ext_loc1

-- COMMAND ----------

create external location if not exists ext_loc2
URL 'abfss://project@adistore33.dfs.core.windows.net/validation_table'
with (storage credential adi_cred)

-- COMMAND ----------

