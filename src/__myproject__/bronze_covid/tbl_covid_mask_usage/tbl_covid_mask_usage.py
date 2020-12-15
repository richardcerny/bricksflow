# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/databricks icon.png?raw=true" width=100/> 
# MAGIC # Bricksflow examples
# MAGIC 
# MAGIC ## Create new table from CSV
# MAGIC 
# MAGIC Take a look how to organize cells and functions, use `@decorators`, pass variables from `config.yaml`.
# MAGIC 
# MAGIC #### Cells and functions
# MAGIC Each step/transformation has its own cell and function. By sorting cells and function in this way it significatly improves debuggability of each step.
# MAGIC 
# MAGIC We try to avoid complex dataframe manipulation within one function. Function name should briefly describe what it does.
# MAGIC 
# MAGIC #### @decorators
# MAGIC Decorators enable using software engineering approaches while using advantage of interactive notebook. For example it is possible to generate Lineage documenation based on order of transformations and other things.
# MAGIC - @dataFrameLoader 
# MAGIC - @transformation
# MAGIC - @dataFrameSaver
# MAGIC 
# MAGIC ##### Decorators parameters
# MAGIC `display=True/False`
# MAGIC 
# MAGIC #### Config overview config.yaml (not-editable in Databricks)
# MAGIC 
# MAGIC ##### Basic parameters
# MAGIC Common parameters accessible anywhere and passed through @decorator
# MAGIC **Define param**
# MAGIC ```
# MAGIC parameters:
# MAGIC   myparameter:
# MAGIC     myvalue: 'This is a sample string config value'
# MAGIC ```
# MAGIC **Use param in code**
# MAGIC ```
# MAGIC @dataFrameLoader("%myparameter.myvalue%", display=True)
# MAGIC def tbl_raw_trace_event(my_value_from_config: str, spark: SparkSession)
# MAGIC    print(my_value_from_config)
# MAGIC    ...
# MAGIC ```
# MAGIC 
# MAGIC ##### Datalake Bundle parameters
# MAGIC 
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Requirement for demo run of this notebook
# MAGIC 
# MAGIC ##### Environment variables defined on a cluster
# MAGIC ```
# MAGIC APP_ENV=dev
# MAGIC CONTAINER_INIT_FUNCTION=[ROOT_MODULE].ContainerInit.initContainer
# MAGIC ```
# MAGIC 
# MAGIC ##### Database `dev_bronze_covid`
# MAGIC 
# MAGIC If you want to run this notebook you need to create it using this command:
# MAGIC ```
# MAGIC %sql
# MAGIC create database dev_bronze_covid
# MAGIC ```
# MAGIC 
# MAGIC ##### Table `dev_bronze_covid.tbl_covid_mask_usage`
# MAGIC 
# MAGIC Typically the table is created automatically by Datalake Bundle (`console datalake:table:create-missing`). However it is possible to create the table manually with this command:
# MAGIC 
# MAGIC ```
# MAGIC %sql
# MAGIC CREATE TABLE `dev_bronze_covid`.`tbl_covid_mask_usage` 
# MAGIC ( `COUNTYFP` INT, `NEVER` DOUBLE, `RARELY` DOUBLE, `SOMETIMES` DOUBLE, `FREQUENTLY` DOUBLE, `ALWAYS` DOUBLE, `INSERT_TS` TIMESTAMP) 
# MAGIC USING delta OPTIONS ( `overwriteSchema` 'true') 
# MAGIC LOCATION 'dbfs:/dev/bronze/covid/tbl/tbl_covid_mask_usage.delta'
# MAGIC ```
# MAGIC 
# MAGIC 
# MAGIC __NOTE:__ Tested on a cluster running Databricks 7.3.

# COMMAND ----------

# DBTITLE 1,This command loads Bricksflow framework and its dependencies
# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from logging import Logger
from datetime import datetime
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.pipeline.decorator.loader import dataFrameLoader, transformation, dataFrameSaver

# Datalake Bundle
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader("%datalakebundle.tables%", display=False)
def read_csv_mask_usage(parameters_datalakebundle, spark: SparkSession, logger: Logger):
    source_csv_path = parameters_datalakebundle['bronze_covid.tbl_covid_mask_usage']['source_csv_path']
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true') # it might be better idea to define schema
            .load(source_csv_path)
    )

# COMMAND ----------

@transformation(read_csv_mask_usage, display=False)
def add_column_insert_ts(df: DataFrame, logger: Logger):
    logger.info("Adding Insert timestamp")
    return df.withColumn('INSERT_TS', F.lit(datetime.now()))
    

# COMMAND ----------

@dataFrameSaver(add_column_insert_ts)
def save_table(df: DataFrame, logger: Logger, tableNames: TableNames):
    outputTableName = tableNames.getByAlias('bronze_covid.tbl_covid_mask_usage')
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select(
                'COUNTYFP',
                'NEVER',
                'RARELY',
                'SOMETIMES',
                'FREQUENTLY',
                'ALWAYS',
                'INSERT_TS'
            )
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )
    logger.info(f"Data successfully saved to: {outputTableName}")