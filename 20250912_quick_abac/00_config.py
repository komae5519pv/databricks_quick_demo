# Databricks notebook source
# DBTITLE 1,初期設定
# カタログ情報
MY_CATALOG = "komae_demo_v4"              # ご自分のカタログ名に変更してください
MY_SCHEMA = "abac"
MY_VOLUME = "source"

# # カタログ情報
# catalog = "komae_demo_v2"
# schema = "bricksmart"
# volume = "csv"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {MY_CATALOG};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {MY_CATALOG}.{MY_SCHEMA}.{MY_VOLUME}")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {MY_CATALOG};")
spark.sql(f"USE SCHEMA {MY_SCHEMA};")

# 設定内容を表示
print(f"MY_CATALOG: {MY_CATALOG}")
print(f"MY_SCHEMA: {MY_SCHEMA}")
print(f"VOLUME パス: /Volumes/{MY_CATALOG}/{MY_SCHEMA}/{MY_VOLUME}")
