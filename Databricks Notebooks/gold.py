# Databricks notebook source
# DBTITLE 1,Function - get param
def get_params():
    params = pd.read_json('/params.json').squeeze().to_dict()
    return params

# COMMAND ----------

# DBTITLE 1,Gold load logic
def gold_load(params):
    
    options = {
        "sfUrl": params["sfUrl"],
        "sfUser": params["sfUser"],
        "sfPassword": params["sfPassword"],
        "sfDatabase": params["sfDatabase"],
        "sfSchema": params["sfSchema"],
        "sfWarehouse": params["sfWarehouse"],
    }
    
    spark.table("silver_orders").write \
    .format("net.snowflake.spark.snowflake") \
    .options(**options) \
    .option("dbtable", "orders") \
    .save()


    spark.table("silver_orderitems").write\
    .format("net.snowflake.spark.snowflake") \
    .options(**options) \
    .option("dbtable", "orderitems") \
    .save()

# COMMAND ----------

# DBTITLE 1,Main logic
params = get_params()
sfUser = dbutils.secrets.get(scope="Snowflake-secrets",key="Snowflake-secrets-sfUser")
sfPassword = dbutils.secrets.get(scope="Snowflake-secrets",key="Snowflake-secrets-sfPassword")

params["sfUser"] = sfUser
params["sfPassword"] = sfPassword

gold_load(params)

