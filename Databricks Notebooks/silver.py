# Databricks notebook source
# DBTITLE 1,Imports
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,StringType,DoubleType,IntegerType

# COMMAND ----------

# DBTITLE 1,Function - get param
def get_params():
    params = pd.read_json('/params.json').squeeze().to_dict()
    return params

# COMMAND ----------

# DBTITLE 1,Silver load logic
def silver_load(params):
    silver_checkpointlocation = params['silver.checkpointlocation']
    startingOffsets = "latest"
    #Defining schema
    orders = StructType([
        StructField("OrderID",StringType(),True)
        ,StructField("OrderDate",StringType(),True)
        ,StructField("StoreID",StringType(),True)
        ,StructField("GlobalID",StringType(),True)
        ,StructField("Country",StringType(),True)
        ,StructField("PaymentMethod",StringType(),True)
        ,StructField("TotalAmount",DoubleType(),True)
    ])
    
    orderitems = StructType([
        StructField("OrderItemID",StringType(),True)
        ,StructField("OrderID",StringType(),True)
        ,StructField("ItemID",StringType(),True)
        ,StructField("ItemName",StringType(),True)
        ,StructField("Quantity",IntegerType(),True)
        ,StructField("UnitPrice",IntegerType(),True)
        ,StructField("TotalPrice",DoubleType(),True)
    ])

    silver_orders_df = spark.readStream\
    .format("delta")\
    .option("startingOffsets", startingOffsets)\
    .table("bronze_orders")\
    .withColumn("format_json",F.regexp_replace(F.col("value"), r'^"|"$|\\', ''))\
    .withColumn("parsed_json",F.from_json("format_json",orders))\
    .select("parsed_json.*")\

    silver_orders_df.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation",silver_checkpointlocation + "orders")\
    .table("silver_orders")\


    silver_orderitems_df = spark.readStream\
    .format("delta")\
    .option("startingOffsets", startingOffsets)\
    .table("bronze_orderitems")\
    .withColumn("format_json",F.regexp_replace(F.col("value"), r'^"|"$|\\', ''))\
    .withColumn("parsed_json",F.from_json("format_json",orderitems))\
    .select("parsed_json.*")\

    silver_orderitems_df.writeStream\
    .format("delta")\
    .outputMode("append")\
    .option("checkpointLocation",silver_checkpointlocation + "orderitems")\
    .table("silver_orderitems")\




# COMMAND ----------

params = get_params()
silver_load(params)
