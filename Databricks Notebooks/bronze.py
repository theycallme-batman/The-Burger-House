# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Function - get param
def get_params():
    params = pd.read_json('/params.json').squeeze().to_dict()
    return params

# COMMAND ----------

# DBTITLE 1,Bronze load logic
def bronze_load(params):
    startingOffsets = "latest"
    bootstrapservers = params['kafka.boostrapservers']
    topicpattern = params['kafka.topics']
    Kafka_groupID = params['kafka.group.id']
    tables = params["tables"]
    bronze_checkpointlocation = params['bronze.checkpointlocation']

    bronze_df =  (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapservers)
        .option("subscribePattern", topicpattern)
        .option("startingOffsets", startingOffsets)
        .option("kafka.group.id", Kafka_groupID)
        .load()
        .selectExpr("CAST(topic AS STRING) AS topic", "CAST(value AS STRING) AS value","timestamp AS producer_timestamp")
        )
    

    for table in tables:
        bronze_df.filter(bronze_df.topic == table).drop("topic")\
        .writeStream.format("delta")\
        .outputMode("append")\
        .option("checkpointLocation",bronze_checkpointlocation + table)\
        .option("mergeSchema", "true")\
        .trigger(processingTime = "20 seconds")\
        .table("bronze_"+table)


# COMMAND ----------

# DBTITLE 1,Main logic
params = get_params()
bronze_load(params)

