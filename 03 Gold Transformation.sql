-- Databricks notebook source
-- MAGIC %md ## Transformation
-- MAGIC
-- MAGIC Tasks:
-- MAGIC 1. Count the number of titles in each category
-- MAGIC 2. List of titles of each cast
-- MAGIC 3. List the longest and shortest titles in each month added

-- COMMAND ----------

USE SCHEMA netflix_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Count the number of titles in each category

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold_aggr_category (
  category STRING,
  count INT
);

-- COMMAND ----------

TRUNCATE TABLE gold_aggr_category;

-- COMMAND ----------

INSERT INTO gold_aggr_category
SELECT category, COUNT(DISTINCT show_id) AS count
FROM
  (SELECT show_id, title, explode(listed_in) AS category FROM silver_titles_base)
GROUP BY category;

-- COMMAND ----------

SELECT * FROM gold_aggr_category
ORDER BY count DESC;
