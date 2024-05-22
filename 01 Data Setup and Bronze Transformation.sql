-- Databricks notebook source
-- MAGIC %md ## Azure Blob Storage Setup

-- COMMAND ----------

-- MAGIC %md Setting the SAS connection parameters for Azure Blob Storage
-- MAGIC - The SAS token should be granted on the container level
-- MAGIC - The SAS token should provide Read and List permissions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # TODO: Modify lines 3 and 4 with the correct Azure Storage details
-- MAGIC az_conf_account = "jomoadls1" # Azure Storage account name
-- MAGIC az_conf_container = "sandbox" # Azure Blob Storage container name
-- MAGIC
-- MAGIC # TODO: Modify lines 7 and 8 with the correct Databricks Secret details
-- MAGIC db_conf_scope = "sandbox" # Databricks Secret Scope name
-- MAGIC db_conf_key = "key_netflix" # Databricks Secret Key name
-- MAGIC
-- MAGIC # TODO: Set mount point name
-- MAGIC db_mnt = "/mnt/netflix_data"
-- MAGIC spark.conf.set("env.db_mnt", db_mnt)
-- MAGIC
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{az_conf_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{az_conf_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{az_conf_account}.dfs.core.windows.net", dbutils.secrets.get(scope=az_conf_container, key=db_conf_key))
-- MAGIC
-- MAGIC # for path shortening
-- MAGIC spark.conf.set("env.azureblob", f"abfss://{az_conf_container}@jomoadls1.dfs.core.windows.net")
-- MAGIC
-- MAGIC # mount the data directory on Azure Blob Storage to DBFS (unmount first if already mounted)
-- MAGIC mounts = [*map(lambda x: x.mountPoint, dbutils.fs.mounts())]
-- MAGIC if db_mnt in mounts:
-- MAGIC     dbutils.fs.unmount(db_mnt)
-- MAGIC
-- MAGIC dbutils.fs.mount(
-- MAGIC     source = f"wasbs://{az_conf_container}@{az_conf_account}.blob.core.windows.net/netflix/",
-- MAGIC     mount_point = db_mnt,
-- MAGIC     extra_configs = {
-- MAGIC         f"fs.azure.sas.{az_conf_container}.{az_conf_account}.blob.core.windows.net" : dbutils.secrets.get(scope=az_conf_container, key=db_conf_key)
-- MAGIC     }
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md Check connection - reading netflix_titles.csv from Azure Blob Storage

-- COMMAND ----------

SELECT COUNT(1) FROM read_files('${env.db_mnt}/netflix_titles.csv')

-- COMMAND ----------

SELECT * FROM read_files('${env.db_mnt}/netflix_titles.csv')
LIMIT 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv(f'{spark.conf.get("env.db_mnt")}/netflix_titles.csv')
-- MAGIC df.head(5)

-- COMMAND ----------

-- MAGIC %md ## Data Ingestion

-- COMMAND ----------

-- MAGIC %md Ingest raw data (CSV) to an external table

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS netflix_data;

-- COMMAND ----------

USE SCHEMA netflix_data;

-- COMMAND ----------

DROP TABLE IF EXISTS raw_titles_ext;
CREATE TABLE raw_titles_ext (
  show_id STRING,
  `type` STRING,
  title STRING,
  director STRING,
  `cast` STRING,
  country STRING,
  date_added STRING,
  release_year INT,
  rating STRING,
  duration STRING,
  listed_in STRING,
  `description` STRING,
  _corrupt_row STRING
) USING CSV
  OPTIONS (
    mode="PERMISSIVE", --Permissive mode to handle malformed records
    header="true",
    delimiter=",",
    multiLine="true", --Use the multiLine option as some records span across multiple lines
    quote="\"",
    escape="\"",
    columnNameOfCorruptRecord="_corrupt_row" --Bucket for the malformed record
  )
  LOCATION '${env.db_mnt}/netflix_titles.csv';

SELECT * FROM raw_titles_ext;

-- COMMAND ----------

SELECT COUNT(1) FROM raw_titles_ext;

-- COMMAND ----------

SELECT * FROM raw_titles_ext
WHERE `_corrupt_row` IS NOT NULL;

-- COMMAND ----------

-- MAGIC %md Ingest the data from an external table into a delta table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS bronze_titles (
  show_id STRING,
  `type` STRING,
  title STRING,
  director ARRAY<STRING>,
  `cast` ARRAY<STRING>,
  country ARRAY<STRING>,
  date_added DATE,
  release_year INT,
  rating STRING,
  duration STRING,
  listed_in ARRAY<STRING>,
  `description` STRING
);

-- COMMAND ----------

TRUNCATE TABLE bronze_titles;

-- COMMAND ----------

INSERT INTO bronze_titles
SELECT
  show_id,
  `type`,
  title,
  array_remove(transform(split(director, ","), x-> trim(x)), ""),
  array_remove(transform(split(`cast`, ","), x -> trim(x)), ""),
  array_remove(transform(split(country, ","), x-> trim(x)), ""),
  to_date(date_added, 'MMMM d, yyyy'),
  release_year,
  rating,
  duration,
  array_remove(transform(split(listed_in, ","), x-> trim(x)), ""),
  `description`
FROM raw_titles_ext
WHERE `_corrupt_row` IS NULL;

-- COMMAND ----------

DESCRIBE DETAIL bronze_titles;

-- COMMAND ----------

SELECT * FROM bronze_titles;
