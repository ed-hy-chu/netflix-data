-- Databricks notebook source
-- MAGIC %md ## Silver Transformation
-- MAGIC
-- MAGIC - Split `duration` into `duration` and `num_seasons` (Number of seasons)
-- MAGIC - List of titles sorted by each cast

-- COMMAND ----------

USE SCHEMA netflix_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **The "Base" silver Titles table**
-- MAGIC
-- MAGIC Split `duration` into `duration` and `num_seasons` (Number of seasons)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver_titles_base (
  show_id STRING,
  `type` STRING,
  title STRING,
  director ARRAY<STRING>,
  `cast` ARRAY<STRING>,
  country ARRAY<STRING>,
  date_added DATE,
  release_year INT,
  rating STRING,
  duration SMALLINT,
  num_seasons SMALLINT,
  listed_in ARRAY<STRING>,
  `description` STRING
);

-- COMMAND ----------

TRUNCATE TABLE silver_titles_base;

-- COMMAND ----------

INSERT INTO silver_titles_base
SELECT
  show_id,
  `type`,
  title,
  director,
  `cast`,
  country,
  date_added,
  release_year,
  rating,
  CASE WHEN duration RLIKE "\\d+ min$" THEN INT(regexp_replace(duration, " min$", "")) END AS dur,
  CASE WHEN duration RLIKE "\\d+ [Ss](eason)s*$" THEN INT(regexp_replace(duration, " [Ss](eason)s*$", "")) END AS sea,
  listed_in,
  `description`
FROM bronze_titles;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC List of titles sorted by each cast

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver_titles_cast (
  cast_name STRING,
  show_id STRING,
  `type` STRING,
  title STRING,
  director ARRAY<STRING>,
  country ARRAY<STRING>,
  date_added DATE,
  release_year INT,
  rating STRING,
  duration SMALLINT,
  num_seasons SMALLINT,
  listed_in ARRAY<STRING>,
  `description` STRING
);

-- COMMAND ----------

TRUNCATE TABLE silver_titles_cast;

-- COMMAND ----------

INSERT INTO silver_titles_cast
SELECT * FROM
  (SELECT
    explode(`cast`) AS cast_name,
    show_id,
    type,
    title,
    director,
    country,
    date_added,
    release_year,
    rating,
    duration,
    num_seasons,
    listed_in,
    description
  FROM silver_titles_base)
ORDER BY cast_name

-- COMMAND ----------

SELECT * FROM silver_titles_cast
