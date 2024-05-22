# Mini Project on Netflix Titles Data

## System Environment
- Developed and tested on Databricks Runtime 14.3 LTS ML

## Data Source
Shivam Bansal on Kaggle (https://www.kaggle.com/datasets/shivamb/netflix-shows)

## Setup of Secret Scope and Secret
This project assumes that the raw data file (CSV) is stored on Azure Blob Storage. SAS token authentication is used in this project. To complete the authentication setup, create a SAS token for the container that contains the raw data file. "Read" and "List" permissions are required. Afterwards, go through the following steps.
1. Using Databricks CLI, run:
> `databricks secrets create-scope sandbox`

> Note: Replace `sandbox` with another scope name of your choice when preferred.

2. Prepare a JSON file containing the Azure Blob Storage SAS token (Replace `%sas-token%` with the actual SAS token). Content of the JSON file:
> `{ "scope": "sandbox", "key": "key1", "string_value": "%sas-token%" }`

> Note: Replace `sandbox` with the scope name used in Step 1. In notebook **01 Data Setup**, modify line 7 if the scope name is not `sandbox`.

> Note: Replace `key1` with another key name of your choice when preferred. In notebook **01 Data Setup**, modify line 8 if the scope name is not `key1`.

3. Add the JSON file as a secret to the secret scope by running:
> `databricks secrets put-secret --json @/path/to/file.json`

4. In notebook **01 Data Setup**, modify lines 3 and 4 accordingly with the actual name of the container used on Azure Blob Storage.

## Data Files
The data file is provided in the `Data` directory. The notebook assumes the following files are stored on Azure Blob Storage, accessed through **01 Data Setup**.
- `netflix_titles.csv` containing the raw data to be transformed

## Notebooks and Tasks
- `01 Data Setup and Bronze Transformation` includes the configuration for obtaining data files from Azure Blob Storage and preparing them for further consumption in the pipeline. Flags any corrupted row during ingestion. Transforms the ingested raw data into a more consumable format (e.g. converting comma separated strings into ARRAYs).
- `02 Silver Transformation` transforms the data by splitting `duration` and `number of seasons` into 2 columns (originally 1 column in the raw data). Transforms further by row explosion.
- `03 Gold Transformation` prepares aggregations for further consumption.
- `04 Additional Queries` includes queries to answer certain questions about the data.
