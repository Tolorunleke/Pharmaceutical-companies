-- Databricks notebook source
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import seaborn as sns
-- MAGIC
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.types import *
-- MAGIC
-- MAGIC import calendar
-- MAGIC import csv
-- MAGIC import os
-- MAGIC from io import StringIO

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def sql_preprocessor(file_name=None):
-- MAGIC     if file_name is None:
-- MAGIC         file_name = input('Kindly enter the file you wish to clean: ')
-- MAGIC     
-- MAGIC     # Read the file into an RDD
-- MAGIC     rdd = sc.textFile(f'FileStore/tables/{file_name}')
-- MAGIC     sample_line = rdd.first()
-- MAGIC     
-- MAGIC     # Check for '|' or '\t' delimiter
-- MAGIC     if '|' in sample_line:
-- MAGIC         delimiter = '|'
-- MAGIC         rdd_pipeline = rdd.map(lambda line: line.replace(',,','').replace('"','')).map(lambda x: x.split(delimiter))
-- MAGIC     elif '\t' in sample_line:
-- MAGIC         delimiter = '\t'
-- MAGIC         rdd_pipeline = rdd.map(lambda line: line.replace(',,','').replace('"','')).map(lambda x: x.split(delimiter))
-- MAGIC
-- MAGIC         #Adjust the length of each row in an RDD to a specified length by appending None values to the row until it reaches the desired length
-- MAGIC         def adjust_row_length(row, length):
-- MAGIC             return row + ([None] * (length - len(row)))
-- MAGIC         rdd_pipeline = rdd_pipeline.map(lambda row: adjust_row_length(row, 14))
-- MAGIC     else:
-- MAGIC         delimiter = rdd.map(lambda row: next(csv.reader(StringIO(row))))
-- MAGIC         rdd_pipeline = delimiter.map(lambda row: [item.strip('"') for item in row])
-- MAGIC     
-- MAGIC     # Replace missing values with 'NA' in each row of the RDD
-- MAGIC     rdd_pipeline = rdd_pipeline.map(lambda row: ['NA' if value is None or value == '' else value for value in row])
-- MAGIC        
-- MAGIC     # Convert RDD to DataFrame
-- MAGIC     header = rdd_pipeline.first()
-- MAGIC     data= rdd_pipeline.filter(lambda x: x != header)
-- MAGIC     dataFrame = spark.createDataFrame(data, schema= header)
-- MAGIC     
-- MAGIC     #create temp view to run SQL queries.
-- MAGIC     file_name= str(file_name)
-- MAGIC     if '2023' in file_name:
-- MAGIC         dataFrame.createOrReplaceTempView('cleaned_clinical_trial2023')
-- MAGIC     elif '2021' in file_name:
-- MAGIC         dataFrame.createOrReplaceTempView('cleaned_clinical_trial2021')
-- MAGIC     elif '2020' in file_name:
-- MAGIC         dataFrame.createOrReplaceTempView('cleaned_clinical_trial2020')
-- MAGIC     else:
-- MAGIC         dataFrame.createOrReplaceTempView('cleaned_pharm_data')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinicalTrial2023= sql_preprocessor('clinicaltrial_2023.csv')

-- COMMAND ----------


select * 
from cleaned_clinical_trial2023

-- COMMAND ----------

-- DBTITLE 1,Question 1
SELECT COUNT(DISTINCT `Study Title`) AS distinct_studies_count
FROM cleaned_clinical_trial2023

-- COMMAND ----------

-- DBTITLE 1,Question 2
SELECT Type, COUNT(*) AS count
FROM cleaned_clinical_trial2023
GROUP BY Type
order by count desc

-- COMMAND ----------

-- DBTITLE 1,Question 3
SELECT Condition, COUNT(*) AS count
FROM (SELECT explode(split(Conditions, '\\|')) AS Condition
  from cleaned_clinical_trial2023
  WHERE Conditions IS NOT NULL)
GROUP BY Condition
ORDER BY count DESC
LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_preprocessor('pharma.csv')

-- COMMAND ----------

-- DBTITLE 1,Question 4
SELECT c.Sponsor, COUNT(*) AS Clinical_Trials_Count
    FROM cleaned_clinical_trial2023 c
    LEFT JOIN (
        SELECT DISTINCT Parent_Company AS Sponsor
        FROM cleaned_pharm_data
    ) p
    ON c.Sponsor = p.Sponsor
    WHERE p.Sponsor IS NULL
    GROUP BY c.Sponsor
    ORDER BY Clinical_Trials_Count DESC
    LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,Question 5
SELECT 
    CAST(SUBSTRING(regexp_replace(Completion, ',', ''), 6, 2) AS INT) AS Completion_Month,
    COUNT(*) AS Study_Count
FROM 
    cleaned_clinical_trial2023
WHERE 
    Status = 'COMPLETED' 
    AND Completion LIKE '2023%'
GROUP BY 
    CAST(SUBSTRING(regexp_replace(Completion, ',', ''), 6, 2) AS INT)
ORDER BY 
    Completion_Month
