# Databricks notebook source
# カタログ情報
catalog = "komae_demo_v3"              # ご自分のカタログ名に変更してください
schema = "bricksmart"
volume = "csv"

# # スキーマ、ボリューム再作成(True=再作成, False=スキップ)
# recreate_schema = "True"
# recreate_volume = "True"

# ベクターサーチエンドポイント
# MY_VECTOR_SEARCH_ENDPOINT = "komae_vs_endpoint"
MY_VECTOR_SEARCH_ENDPOINT = "one-env-shared-endpoint-2"

# Embedding Model Endpoint
EMBEDDING_MODEL_ENDPOINT_NAME = "komae-text-embedding-3-small"
# EMBEDDING_MODEL_ENDPOINT_NAME = "aoai-text-embedding-3-large"

# COMMAND ----------

# カタログ、スキーマ、ボリューム作成
# spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")

# 設定内容を表示
print(f"catalog: {catalog}")
print(f"schema: {schema}")
print(f"recreate_schema: {recreate_schema}")
print(f"volume パス: /Volumes/{catalog}/{schema}/{volume}")
print(f"recreate_volume: {recreate_volume}")
print(f"MY_VECTOR_SEARCH_ENDPOINT: {MY_VECTOR_SEARCH_ENDPOINT}")
print(f"EMBEDDING_MODEL_ENDPOINT_NAME: {EMBEDDING_MODEL_ENDPOINT_NAME}")
