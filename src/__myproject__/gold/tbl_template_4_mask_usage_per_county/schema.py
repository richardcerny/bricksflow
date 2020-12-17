import pyspark.sql.types as T
def getSchema():
    return T.StructType([
                T.StructField('County_Name', T.StringType(), True),
                T.StructField('State', T.StringType(), True),
                T.StructField('stateFIPS', T.IntegerType(), True),
                T.StructField('COUNTYFP', T.IntegerType(), True),
                T.StructField('NEVER', T.IntegerType(), True),
                T.StructField('RARELY', T.IntegerType(), True),
                T.StructField('SOMETIMES', T.IntegerType(), True),
                T.StructField('FREQUENTLY', T.IntegerType(), True),
                T.StructField('ALWAYS', T.IntegerType(), True),
                T.StructField('EXECUTE_DATETIME', T.TimestampType(), True),
                T.StructField('CONFIG_YAML_PARAMETER', T.StringType(), True)
            ])