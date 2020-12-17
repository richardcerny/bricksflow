# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/databricks_icon.png?raw=true" width=100/> 
# MAGIC # Bricksflow example 3.
# MAGIC 
# MAGIC ## Productionalizing notebook in Bricksflow
# MAGIC It always takse some time to productionalize notebook.
# MAGIC What is usually necesary to do:
# MAGIC - cleaning a code from testing part
# MAGIC - comments some part of code
# MAGIC - all code is in functions
# MAGIC - remove unnecesary comments
# MAGIC - resolve ToDos
# MAGIC - replace hardcoded variable with config parameters
# MAGIC - test that it still works the same after clean up
# MAGIC - use linting tools (pylint, black, flake8)
# MAGIC - ...
# MAGIC 
# MAGIC 
# MAGIC ### Write modes - append, overwrite
# MAGIC Todo

# COMMAND ----------

# MAGIC %run ../../app/install_master_package

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, SparkSession
from datetime import datetime
from databricksbundle.pipeline.decorator.loader import dataFrameLoader, pipelineFunction, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader(display=False)
def read_table_bronze_covid_tbl_template_1_mask_usage(spark: SparkSession, tableNames: TableNames):
    return (
        spark
            .read
            .table(tableNames.getByAlias('bronze_covid.tbl_template_1_mask_usage'))
    )

# COMMAND ----------

@transformation(read_table_bronze_covid_tbl_template_1_mask_usage, display=False)
def add_execution_datetime(df: DataFrame):
    return (
        df
             .withColumn('EXECUTE_DATETIME',F.lit(datetime.now()))
    )

# COMMAND ----------

@transformation("%myparameter.myvalue%", add_execution_datetime_df, display=True) # TODO bug  ... _df must be added while passing function name
def add_parameter_from_config(config_yaml_parameter, df: DataFrame):
    print(config_yaml_parameter)
    return (
        df
             .withColumn('CONFIG_YAML_PARAMETER',F.lit(config_yaml_parameter)) #todo  pipelineParams.randomVariable pipelineParams: Box
    )

# COMMAND ----------

@dataFrameSaver(add_parameter_from_config)
def save_table_silver_covid_tbl_template_3_mask_usage(df: DataFrame, logger: Logger, tableNames: TableNames):

    outputTableName = tableNames.getByAlias('silver_covid.tbl_template_3_mask_usage')
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
                'EXECUTE_DATETIME',
                'CONFIG_YAML_PARAMETER',
            )
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )