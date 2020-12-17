# Databricks notebook source
# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, SparkSession
from datetime import datetime
from databricksbundle.pipeline.decorator.loader import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

@dataFrameLoader(display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(spark: SparkSession, tableNames: TableNames):
    return (
        spark
            .read
            .table(tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases'))
            .select('countyFIPS','County_Name', 'State', 'stateFIPS')
            .dropDuplicates()
    )

# COMMAND ----------

@dataFrameLoader(display=True)
def read_table_silver_covid_tbl_template_3_mask_usage(spark: SparkSession, tableNames: TableNames):
    return (
        spark
            .read
            .table(tableNames.getByAlias('silver_covid.tbl_template_3_mask_usage'))
    )

# COMMAND ----------

@transformation(read_bronze_covid_tbl_template_2_confirmed_case, read_table_silver_covid_tbl_template_3_mask_usage, display=True)
def join_covid_datasets(df1: DataFrame, df2: DataFrame):
    return (
        df1.join(df2, df1.countyFIPS == df2.COUNTYFP, how='right')
    )

# COMMAND ----------

@transformation(join_covid_datasets, display=True)
def standardize_dataset(df: DataFrame):
    return (
        df.drop('countyFIPS')
    )

# COMMAND ----------

@dataFrameSaver(standardize_dataset)
def save_table_gold_tbl_template_4_mask_usage_per_count(df: DataFrame, logger: Logger, tableNames: TableNames):

    outputTableName = tableNames.getByAlias('gold.tbl_template_4_mask_usage_per_county')
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select(
                 'County_Name',
                 'State',
                 'stateFIPS',
                 'COUNTYFP',
                 'NEVER',
                 'RARELY',
                 'SOMETIMES',
                 'FREQUENTLY',
                 'ALWAYS',
                 'EXECUTE_DATETIME',
                 'CONFIG_YAML_PARAMETER'
            )
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )