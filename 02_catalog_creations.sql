-- Databricks notebook source
create catalog if not exists project_01

-- COMMAND ----------

use catalog project_01

-- COMMAND ----------

DROP SCHEMA IF EXISTS bronze CASCADE;
create schema if not exists bronze 
managed location 'abfss://project@adistore33.dfs.core.windows.net/processed/bronze'
-- managed location given in order to save the table there otherwise it would get saved in adls path of the unity metastore

-- COMMAND ----------

DROP SCHEMA if exists silver CASCADE;
create schema if not exists silver
managed location 'abfss://project@adistore33.dfs.core.windows.net/processed/silver'

-- COMMAND ----------

DROP SCHEMA if exists gold CASCADE;
create schema if not exists gold 
managed location 'abfss://project@adistore33.dfs.core.windows.net/processed/gold'

-- COMMAND ----------

DROP SCHEMA if exists validation CASCADE;
create schema if not exists validation

-- COMMAND ----------

SHOW SCHEMAS