-- Databricks notebook source
-- MAGIC %md ## Additional Queries

-- COMMAND ----------

USE SCHEMA netflix_data;

-- COMMAND ----------

-- MAGIC %md List the casts that have titles released for at least consecutive 3 years

-- COMMAND ----------

WITH tb AS
(
  SELECT
    cast_name,
    release_year,
    row_number() OVER (PARTITION BY cast_name ORDER BY release_year) rn,
    (release_year - rn) rn2
  FROM silver_titles_cast
)
SELECT DISTINCT cast_name
FROM tb
GROUP BY cast_name, rn2
HAVING COUNT(1) >= 3
ORDER BY cast_name
