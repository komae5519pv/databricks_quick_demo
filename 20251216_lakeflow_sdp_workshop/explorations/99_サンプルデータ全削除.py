# Databricks notebook source
# MAGIC %md
# MAGIC ## 生成済みのサンプルデータを全て削除する
# MAGIC - **最初からやり直したい場合に、このノートブックを実行してデータを削除してください。**
# MAGIC - スキーマは削除されません。
# MAGIC - Declarative Pipelinesで作成したStreaming TableやMetarialized Viewは削除されません。

# COMMAND ----------

# MAGIC %run ./00_環境設定

# COMMAND ----------

# 直下のエントリを列挙
entries = dbutils.fs.ls(volume_dir)

# すべて再帰削除
for e in entries:
    dbutils.fs.rm(e.path, True)

# COMMAND ----------

spark.sql(f"DROP VOLUME IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.{VOLUME_NAME}")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG_NAME}.{SCHEMA_NAME}.products_source")
