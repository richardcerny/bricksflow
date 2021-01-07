# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/richardcerny/bricksflow/raw/rc-template-notebooks/docs/databricks_icon.png?raw=true" width=100/> 
# MAGIC # Bricksflow example 5.
# MAGIC 
# MAGIC Widgets, secrets, pipelinFuncitons
# MAGIC 
# MAGIC ## Widgets
# MAGIC Many people love widgets as they can easilly parametrize their notebook. It is possible to use widget with Bricksflow. Usage is demonstrated in this notebook. Don't forget to check [Widgets documentation](ttps://docs.databricks.com/notebooks/widgets.html) or run command `dbutils.widgets.help()` to see options you have while working with widget.

# COMMAND ----------

# MAGIC %run ../../../app/install_master_package

# COMMAND ----------

# DBTITLE 1,Databricks widget options
dbutils.widgets.help()

# COMMAND ----------

from logging import Logger
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F, SparkSession
from datetime import datetime
from databricksbundle.pipeline.decorator.loader import dataFrameLoader, transformation, dataFrameSaver
from datalakebundle.table.TableNames import TableNames

# COMMAND ----------

dbutils.widgets.text("widget_states", "CA", 'States') # Examples: CA, IL, IN,...
widgetStatesVariable = dbutils.widgets.get("widget_states")

# COMMAND ----------

# DBTITLE 1,Usage of widget variable
@dataFrameLoader(widgetStatesVariable, display=True)
def read_bronze_covid_tbl_template_2_confirmed_case(stateName: str, spark: SparkSession, logger: Logger, tableNames: TableNames):
    logger.info(f"States Widget value: {state}")
    return (
        spark
            .read
            .table(tableNames.getByAlias('bronze_covid.tbl_template_2_confirmed_cases'))
            .select('countyFIPS','County_Name', 'State', 'stateFIPS')
            .filter(F.col('State')==stateName) # Widget variable is used
    )

# COMMAND ----------

# next commands