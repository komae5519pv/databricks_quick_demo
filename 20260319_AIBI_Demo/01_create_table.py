# Databricks notebook source
# MAGIC %md
# MAGIC # テーブル作成ノートブック
# MAGIC
# MAGIC サーバレスコンピュートまたはSQLウェアハウスで実行してください

# COMMAND ----------

# DBTITLE 1,設定読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. CSVファイルをコピー

# COMMAND ----------

# DBTITLE 1,Workspace CSVをVolumeにコピー
import os
import shutil

# カレントディレクトリ取得
current_dir_path = os.getcwd()
src_no_scheme = f"{current_dir_path}/Sample-Superstore_Japanese.csv"

# csvをボリュームにコピー
dest_path = f"/Volumes/{catalog}/{schema}/{volume}/Sample-Superstore_Japanese.csv"

print(f"📄 CSVソース：{src_no_scheme}")
print(f"📁 CSVコピー先：{dest_path}")

shutil.copyfile(src_no_scheme, dest_path)

print(f"✅ CSVファイルをVolumeにコピーしました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Silverテーブル作成（sales_silver）

# COMMAND ----------

# DBTITLE 1,CSVを読み込んでテーブル作成
import pyspark.sql.functions as F

# CSV を Spark Dataframe に読み込み（Volumeから）
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(dest_path)
)

# 日本語列名を英語にリネーム
mapping = {
    "行 ID": "row_id",
    "オーダー ID": "order_id",
    "オーダー日": "order_date",
    "出荷日": "ship_date",
    "出荷モード": "ship_mode",
    "顧客 ID": "customer_id",
    "顧客名": "customer_name",
    "顧客区分": "customer_segment",
    "市区町村": "city",
    "都道府県": "prefecture",
    "国/領域": "country_region",
    "地域": "region",
    "製品 ID": "product_id",
    "カテゴリ": "category",
    "サブカテゴリ": "sub_category",
    "製品名": "product_name",
    "売上": "sales_amount",
    "数量": "quantity",
    "割引率": "discount_rate",
    "利益": "profit",
    "緯度": "latitude",
    "経度": "longitude",
    "地域マネージャー": "region_manager",
    "予算": "budget",
    "返品": "returned",
}

for jp, en in mapping.items():
    if jp in df.columns:
        df = df.withColumnRenamed(jp, en)

# 利益がマイナスの行を除外
df = df.filter(F.col("profit") >= 0)

# 返品フラグ列（返品: ○/NULL → returned: true/false）
df = df.withColumn(
    "returned",
    F.when(F.col("returned") == "○", F.lit(True)).otherwise(F.lit(False))
)

# カラム並び順を整える
df_silver = df.select(
    "order_id",         # オーダー ID
    "row_id",           # 行 ID
    "order_date",       # オーダー日
    "ship_date",        # 出荷日
    "ship_mode",        # 出荷モード
    "customer_id",      # 顧客 ID
    "customer_name",    # 顧客名
    "customer_segment", # 顧客区分
    "city",             # 市区町村
    "prefecture",       # 都道府県
    "country_region",   # 国/地域
    "region",           # 地域
    "region_manager",   # 地域マネージャー
    "product_id",       # 製品 ID
    "category",         # カテゴリ
    "sub_category",     # サブカテゴリ
    "product_name",     # 製品名
    "sales_amount",     # 売上
    "quantity",         # 数量
    "discount_rate",    # 割引率
    "profit",           # 利益
    "latitude",         # 緯度
    "longitude",        # 経度
    "budget",           # 予算
    "returned",         # 返品フラグ
)

# Silver テーブルとして保存
df_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.sales_silver")

print(f"✅ テーブル作成完了：{catalog}.{schema}.sales_silver")
print(f"📊 レコード数：{df_silver.count():,} 件")

df_silver.display()

# COMMAND ----------

# DBTITLE 1,主キー設定
TABLE_PATH = f'{catalog}.{schema}.sales_silver'
PK_CONSTRAINT_NAME = 'pk_sales_silver'

# NOT NULL 制約の追加
spark.sql(f"""
ALTER TABLE {TABLE_PATH}
ALTER COLUMN order_id SET NOT NULL;
""")

# 主キー設定
spark.sql(f'''
ALTER TABLE {TABLE_PATH} DROP CONSTRAINT IF EXISTS {PK_CONSTRAINT_NAME};
''')

spark.sql(f'''
ALTER TABLE {TABLE_PATH}
ADD CONSTRAINT {PK_CONSTRAINT_NAME} PRIMARY KEY (order_id);
''')

print(f"✅ 主キー設定完了：{PK_CONSTRAINT_NAME}")

# COMMAND ----------

# DBTITLE 1,コメント追加
table_name = f"{catalog}.{schema}.sales_silver"

# テーブルコメント
comment = """スーパーストア売上データ（Silver）：注文・顧客・商品・地域情報を含む分析用テーブル。UCメトリクスビューの基盤データとして使用。"""
spark.sql(f'COMMENT ON TABLE {table_name} IS "{comment}"')

# カラムコメント
column_comments = {
    "order_id":         "オーダー ID（主キー）",
    "row_id":           "行 ID",
    "order_date":       "オーダー日",
    "ship_date":        "出荷日",
    "ship_mode":        "出荷モード",
    "customer_id":      "顧客 ID",
    "customer_name":    "顧客名",
    "customer_segment": "顧客区分（消費者/企業/小規模オフィス）",
    "city":             "市区町村",
    "prefecture":       "都道府県",
    "country_region":   "国/地域",
    "region":           "地域（関東/関西/中部/九州）",
    "region_manager":   "地域マネージャー",
    "product_id":       "製品 ID",
    "category":         "カテゴリ（家具/電化製品/オフィス用品）",
    "sub_category":     "サブカテゴリ",
    "product_name":     "製品名",
    "sales_amount":     "売上金額",
    "quantity":         "数量",
    "discount_rate":    "割引率（0〜1）",
    "profit":           "利益",
    "latitude":         "緯度",
    "longitude":        "経度",
    "budget":           "予算金額",
    "returned":         "返品フラグ（true/false）",
}

for column, col_comment in column_comments.items():
    escaped_comment = col_comment.replace("'", "\\'")
    sql_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} COMMENT '{escaped_comment}'"
    spark.sql(sql_query)

print(f"✅ コメント設定完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. テーブル確認

# COMMAND ----------

# DBTITLE 1,テーブル統計
spark.sql(f"""
SELECT
  '売上合計' AS metric, FORMAT_NUMBER(SUM(sales_amount), '#,###') AS value
FROM {catalog}.{schema}.sales_silver
UNION ALL
SELECT
  '利益合計', FORMAT_NUMBER(SUM(profit), '#,###')
FROM {catalog}.{schema}.sales_silver
UNION ALL
SELECT
  'オーダー数', FORMAT_NUMBER(COUNT(DISTINCT order_id), '#,###')
FROM {catalog}.{schema}.sales_silver
UNION ALL
SELECT
  '顧客数', FORMAT_NUMBER(COUNT(DISTINCT customer_id), '#,###')
FROM {catalog}.{schema}.sales_silver
""").display()

# COMMAND ----------

# DBTITLE 1,カテゴリ別売上
spark.sql(f"""
SELECT
  category AS カテゴリ,
  FORMAT_NUMBER(SUM(sales_amount), '#,###') AS 売上合計,
  FORMAT_NUMBER(SUM(profit), '#,###') AS 利益合計,
  COUNT(*) AS オーダー数
FROM {catalog}.{schema}.sales_silver
GROUP BY category
ORDER BY SUM(sales_amount) DESC
""").display()
