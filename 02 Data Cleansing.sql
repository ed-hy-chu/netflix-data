-- Databricks notebook source
-- MAGIC %md ## Data Cleansing
-- MAGIC
-- MAGIC - Character encoding issues
-- MAGIC - Format `date_added` into a DATE
-- MAGIC - Explode multiple values of `director`
-- MAGIC - Explode multiple values of `cast`
-- MAGIC - Explode multiple values of `country`
-- MAGIC - Explode multiple `listed_in`
-- MAGIC - Split `duration` into `duration` and `number of seasons`

-- COMMAND ----------

USE SCHEMA netflix_data;
