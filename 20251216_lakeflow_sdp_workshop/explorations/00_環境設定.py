# Databricks notebook source
# MAGIC %md
# MAGIC ## 自身の環境に合わせて要変更
# MAGIC データの格納場所として使用するカタログとスキーマを設定します。

# COMMAND ----------

# カタログ名を指定。ワークショップ参加者全員共通
# CATALOG_NAME = "workshop_all"
CATALOG_NAME = "komae_demo_v4"

# スキーマ名を指定。各参加者毎にユニークな名称にする。
SCHEMA_NAME = "lakeflow_workshop"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 以下のパラメータは変更不要

# COMMAND ----------

# MAGIC %md
# MAGIC ### 変更不要パラメータ

# COMMAND ----------

# ボリューム名の指定
VOLUME_NAME = "raw_data"

# COMMAND ----------

# スキーマとボリュームの作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")

# COMMAND ----------

# 出力ディレクトリの作成

# Volumeのルートディレクトリ
volume_dir = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/{VOLUME_NAME}/"

# 顧客データを格納するディレクトリ
users_dir = f"{volume_dir}/users/"
# 購買履歴のサンプルデータを格納するディレクトリ
transactions_east_dir = f"{volume_dir}/transactions_east/"
transactions_west_dir = f"{volume_dir}/transactions_west/"

# 加入者マスターのリカバリデータを格納するディレクトリ
users_backfill_dir = f"{volume_dir}/users_backfill/"

# 上記で指定したディレクトリの作成
dbutils.fs.mkdirs(users_dir)
dbutils.fs.mkdirs(users_backfill_dir)
dbutils.fs.mkdirs(transactions_east_dir)
dbutils.fs.mkdirs(transactions_west_dir)

# 基地局マスターのテーブル名 (上流データソース)
products_source_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.products_source"
