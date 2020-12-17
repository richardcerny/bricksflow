# Databricks notebook source
# MAGIC %run ../../app/install_master_package

# COMMAND ----------

from logging import Logger
from pyspark.sql import functions as F, SparkSession
from pyspark.sql.dataframe import DataFrame
from databricksbundle.pipeline.decorator.loader import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# def flatten_table
from pyspark.sql import Row
from functools import partial

# COMMAND ----------

@dataFrameLoader("%datalakebundle.tables%", display=False)
def read_csv_covid_confirmed_usafacts(parameters_datalakebundle, spark: SparkSession, logger: Logger):
    source_csv_path = parameters_datalakebundle['bronze_covid.tbl_template_2_confirmed_cases']['source_csv_path']
    logger.info(f"Reading CSV from source path: `{source_csv_path}`.")
    return (
        spark
            .read
            .format('csv')
            .option('header', 'true')
            .option('inferSchema', 'true')
            .load(source_csv_path)
    )

# COMMAND ----------

@transformation(read_csv_covid_confirmed_usafacts, display=True)
def rename_columns(df: DataFrame):
    return (
        df
            .withColumnRenamed('County Name', 'County_Name')
    )

# COMMAND ----------

@dataFrameSaver(rename_columns)
def save_table_ronze_covid_tbl_template_2_confirmed_cases(df: DataFrame, logger: Logger, tableNames: TableNames):
    outputTableName = tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases')
    logger.info(f"Saving data to table: {outputTableName}")
    (
        df
            .select(
                 'countyFIPS',
                 'County_Name',
                 'State',
                 'stateFIPS',
                 '1/22/20',
                 '1/23/20',
                 '1/24/20',
                 '1/25/20',
                 '1/26/20',
                 '1/27/20',
                 '1/28/20',
                 '1/29/20',
                 '1/30/20',
                 '1/31/20',
                 '2/1/20',
                 '2/2/20',
                 '2/3/20',
                 '2/4/20',
                 '2/5/20',
                 '2/6/20',
                 '2/7/20',
                 '2/8/20',
                 '2/9/20',
                 '2/10/20',
                 '2/11/20',
                 '2/12/20',
                 '2/13/20',
                 '2/14/20',
                 '2/15/20',
                 '2/16/20',
                 '2/17/20',
                 '2/18/20',
                 '2/19/20',
                 '2/20/20',
                 '2/21/20',
                 '2/22/20',
                 '2/23/20',
                 '2/24/20',
                 '2/25/20',
                 '2/26/20',
                 '2/27/20',
                 '2/28/20',
                 '2/29/20',
                 '3/1/20',
                 '3/2/20',
                 '3/3/20',
                 '3/4/20',
                 '3/5/20',
                 '3/6/20',
                 '3/7/20',
                 '3/8/20',
                 '3/9/20',
                 '3/10/20',
                 '3/11/20',
                 '3/12/20',
                 '3/13/20',
                 '3/14/20',
                 '3/15/20',
                 '3/16/20',
                 '3/17/20',
                 '3/18/20',
                 '3/19/20',
                 '3/20/20',
                 '3/21/20',
                 '3/22/20',
                 '3/23/20',
                 '3/24/20',
                 '3/25/20',
                 '3/26/20',
                 '3/27/20',
                 '3/28/20',
                 '3/29/20',
                 '3/30/20',
                 '3/31/20',
                 '4/1/20',
                 '4/2/20',
                 '4/3/20',
                 '4/4/20',
                 '4/5/20',
                 '4/6/20',
                 '4/7/20',
                 '4/8/20',
                 '4/9/20',
                 '4/10/20',
                 '4/11/20',
                 '4/12/20',
                 '4/13/20',
                 '4/14/20',
                 '4/15/20',
                 '4/16/20',
                 '4/17/20',
                 '4/18/20',
                 '4/19/20',
                 '4/20/20',
                 '4/21/20',
                 '4/22/20',
                 '4/23/20',
                 '4/24/20',
                 '4/25/20',
                 '4/26/20',
                 '4/27/20',
                 '4/28/20',
                 '4/29/20',
                 '4/30/20',
                 '5/1/20',
                 '5/2/20',
                 '5/3/20',
                 '5/4/20',
                 '5/5/20',
                 '5/6/20',
                 '5/7/20',
                 '5/8/20',
                 '5/9/20',
                 '5/10/20',
                 '5/11/20',
                 '5/12/20',
                 '5/13/20',
                 '5/14/20',
                 '5/15/20',
                 '5/16/20',
                 '5/17/20',
                 '5/18/20',
                 '5/19/20',
                 '5/20/20',
                 '5/21/20',
                 '5/22/20',
                 '5/23/20',
                 '5/24/20',
                 '5/25/20',
                 '5/26/20',
                 '5/27/20',
                 '5/28/20',
                 '5/29/20',
                 '5/30/20',
                 '5/31/20',
                 '6/1/20',
                 '6/2/20',
                 '6/3/20',
                 '6/4/20',
                 '6/5/20',
                 '6/6/20',
                 '6/7/20',
                 '6/8/20',
                 '6/9/20',
                 '6/10/20',
                 '6/11/20',
                 '6/12/20',
                 '6/13/20',
                 '6/14/20',
                 '6/15/20',
                 '6/16/20',
                 '6/17/20',
                 '6/18/20',
                 '6/19/20',
                 '6/20/20',
                 '6/21/20',
                 '6/22/20',
                 '6/23/20',
                 '6/24/20',
                 '6/25/20',
                 '6/26/20',
                 '6/27/20',
                 '6/28/20',
                 '6/29/20',
                 '6/30/20',
                 '7/1/20',
                 '7/2/20',
                 '7/3/20',
                 '7/4/20',
                 '7/5/20',
                 '7/6/20',
                 '7/7/20',
                 '7/8/20',
                 '7/9/20',
                 '7/10/20',
                 '7/11/20',
                 '7/12/20',
                 '7/13/20',
                 '7/14/20',
                 '7/15/20',
                 '7/16/20',
                 '7/17/20',
                 '7/18/20',
                 '7/19/20',
                 '7/20/20',
                 '7/21/20',
                 '7/22/20',
                 '7/23/20',
                 '7/24/20',
                 '7/25/20',
                 '7/26/20',
                 '7/27/20',
                 '7/28/20',
                 '7/29/20',
                 '7/30/20',
                 '7/31/20',
                 '8/1/20',
                 '8/2/20',
                 '8/3/20',
                 '8/4/20',
                 '8/5/20',
                 '8/6/20',
                 '8/7/20',
                 '8/8/20',
                 '8/9/20',
                 '8/10/20',
                 '8/11/20',
                 '8/12/20',
                 '8/13/20',
                 '8/14/20',
                 '8/15/20',
                 '8/16/20',
                 '8/17/20',
                 '8/18/20',
                 '8/19/20',
                 '8/20/20',
                 '8/21/20',
                 '8/22/20',
                 '8/23/20',
                 '8/24/20',
                 '8/25/20',
                 '8/26/20',
                 '8/27/20',
                 '8/28/20',
                 '8/29/20',
                 '8/30/20',
                 '8/31/20',
                 '9/1/20',
                 '9/2/20',
                 '9/3/20',
                 '9/4/20',
                 '9/5/20',
                 '9/6/20',
                 '9/7/20',
                 '9/8/20',
                 '9/9/20',
                 '9/10/20',
                 '9/11/20',
                 '9/12/20'
            )
            .write
            .option('partitionOverwriteMode', 'dynamic')
            .insertInto(outputTableName)
    )