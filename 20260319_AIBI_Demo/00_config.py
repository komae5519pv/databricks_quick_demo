# Databricks notebook source
# MAGIC %md
# MAGIC # 設定ノートブック
# MAGIC
# MAGIC サーバレスコンピュートまたはSQLウェアハウスで実行してください

# COMMAND ----------

# DBTITLE 1,変数設定
catalog = "komae_demo_v4"      # カタログ名（既存）
schema = "aibi_superstore"     # スキーマ名（新規）
volume = "raw"                  # ボリューム名
workspace_path = "/Workspace/Users/konomi.omae@databricks.com/03_External_Work/20260318_AIBI_Dashboard"

# COMMAND ----------

# DBTITLE 1,リセット用（必要な場合のみコメント解除）
# spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema} CASCADE")

# COMMAND ----------

# DBTITLE 1,スキーマ・ボリューム作成（カタログは既存のkomae_demo_v4を使用）
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")

# 使うカタログ、スキーマを指定
spark.sql(f"USE CATALOG {catalog};")
spark.sql(f"USE SCHEMA {schema};")

# COMMAND ----------

# DBTITLE 1,設定内容の表示
print("=== スーパーストアダッシュボード設定 ===")
print(f"catalog        : {catalog}")
print(f"schema         : {schema}")
print(f"volume         : {volume}")
print(f"workspace_path : {workspace_path}")
print(f"CSVパス（Workspace）: {workspace_path}/Sample-Superstore_Japanese.csv")
print(f"CSVパス（Volume）   : /Volumes/{catalog}/{schema}/{volume}/Sample-Superstore_Japanese.csv")
