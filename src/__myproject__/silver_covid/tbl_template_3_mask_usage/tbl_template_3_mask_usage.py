# Databricks notebook source
# MAGIC %run ../../app/install_master_package

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

print(add_parameter_from_config_df.printSchema())

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