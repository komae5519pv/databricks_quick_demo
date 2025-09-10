# Databricks notebook source
# MAGIC %md
# MAGIC サーバレスコンピュートをお使いください

# COMMAND ----------

# MAGIC %run ../00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. エリア、店舗ごとの販売ランキング
# MAGIC 概要：店舗IDごとの売上ランキングを表示（売上金額で降順）  
# MAGIC 関数：`get_store_sales_ranking()`  
# MAGIC 引数：`limit_rows`: `表示する行数（Option）`  
# MAGIC 入力例：店舗の売上ランキングを表示

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION get_store_sales_ranking(
  limit_rows INT DEFAULT 5   -- 表示する行数
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '店舗ごとの売上ランキングを表示（売上金額で降順）'
RETURN
    SELECT
      store_id,                                                                       -- 店舗ID
      total_sales,                                                                    -- 売上金額
      total_quantity,                                                                 -- 販売点数
      uq_user_cnt,                                                                    -- 購買顧客数
      avg_purchases_per_user                                                          -- 1人あたりの平均購買回数
    FROM (
        SELECT
          store_id,
          SUM(transaction_price * quantity) AS total_sales,                           -- 売上金額の集計
          SUM(quantity) AS total_quantity,                                            -- 販売点数の集計
          COUNT(DISTINCT user_id) AS uq_user_cnt,                                     -- ユニーク顧客数
          COUNT(transaction_id) / COUNT(DISTINCT user_id) AS avg_purchases_per_user,  -- 1人あたりの平均購買回数
          ROW_NUMBER() OVER (ORDER BY SUM(transaction_price * quantity) DESC) AS row_num
        FROM
            transactions
        WHERE
            quantity IS NOT NULL
            AND transaction_price IS NOT NULL
            AND user_id IS NOT NULL
        GROUP BY
            store_id
    ) ranked
    WHERE row_num <= limit_rows
    ORDER BY
        total_sales DESC
""")

# COMMAND ----------

# DBTITLE 1,お試し実行
result_df = spark.sql(f"""
    -- 引数1: 表示件数
    SELECT * FROM get_store_sales_ranking(5)
""")
store_id = result_df.collect()[0]['store_id']
display(result_df)

# COMMAND ----------

# DBTITLE 1,関数削除
# spark.sql(f'''
# DROP FUNCTION IF EXISTS get_store_sales_ranking;
# ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. 特定店舗の売れ筋商品ランキング
# MAGIC 概要：特定エリア・店舗の商品別販売数ランキングを表示（販売点数で降順）  
# MAGIC 関数：`get_store_item_sales_ranking()`  
# MAGIC 引数：`p_store_id`: `店舗ID`、`limit_rows`: `表示する行数（Option）`  
# MAGIC 入力例：店舗IDがxxの商品売上ランキング

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION get_store_item_sales_ranking(
  p_store_id INT DEFAULT NULL COMMENT '店舗ID',    -- 特定の店舗ID
  limit_rows INT DEFAULT 10 COMMENT '表示する行数'
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '指定された店舗IDの商品販売ランキング（売上金額で降順）'
RETURN
    SELECT
      store_id,                                                                     -- 店舗ID
      product_id,                                                                   -- 商品ID
      product_name,                                                                 -- 商品名
      total_sales,                                                                  -- 売上金額
      total_quantity,                                                               -- 販売点数
      uq_user_cnt,                                                                  -- 購買顧客数
      avg_purchases_per_user                                                        -- 1人あたりの平均購買回数
    FROM (
        SELECT
          t.store_id,
          p.product_id,
          p.product_name,
          SUM(t.transaction_price * t.quantity) AS total_sales,                       -- 売上金額の集計
          SUM(t.quantity) AS total_quantity,                                          -- 販売点数の集計
          COUNT(DISTINCT t.user_id) AS uq_user_cnt,                                   -- ユニーク顧客数
          COUNT(t.transaction_id) / COUNT(DISTINCT t.user_id) AS avg_purchases_per_user, -- 1人あたりの平均購買回数
          ROW_NUMBER() OVER (ORDER BY SUM(t.transaction_price * t.quantity) DESC) AS row_num
        FROM
            transactions t
        JOIN
            products p ON t.product_id = p.product_id
        WHERE
            t.quantity IS NOT NULL
            AND t.transaction_price IS NOT NULL
            AND t.user_id IS NOT NULL
            AND (
                CASE 
                    WHEN p_store_id IS NOT NULL THEN t.store_id = p_store_id  -- 店舗IDが指定されている場合はその店舗IDのみ
                    ELSE TRUE
                END
            )
        GROUP BY
          t.store_id, p.product_id, p.product_name
    ) ranked
    WHERE row_num <= limit_rows
    ORDER BY
        total_sales DESC
""")

# COMMAND ----------

# DBTITLE 1,お試し実行
result_df = spark.sql(f"""
    -- 引数1: 店舗ID、引数2: 表示件数
    SELECT * FROM get_store_item_sales_ranking({store_id},5)
""")
product_id = result_df.collect()[0]['product_id']
display(result_df)

# COMMAND ----------

# DBTITLE 1,関数削除
# spark.sql(f'''
# DROP FUNCTION IF EXISTS get_store_item_sales_ranking;
# ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 特定店舗x特定商品の在庫状況の詳細
# MAGIC 概要：特定店舗・特定商品の在庫状況  
# MAGIC 関数：`get_store_product_inventory()`  
# MAGIC 引数：`p_store_id`: `店舗ID`、`p_product_id`: `商品ID`  
# MAGIC 入力例：店舗IDがxx、商品IDがyyの在庫状況は？

# COMMAND ----------

# DBTITLE 1,関数作成
spark.sql(f"""
CREATE OR REPLACE FUNCTION get_store_product_inventory(
  p_store_id INT,        -- 店舗ID
  p_product_id INT       -- 商品ID
)
  RETURNS TABLE
  READS SQL DATA
  SQL SECURITY DEFINER
COMMENT '指定された店舗IDx商品IDの在庫状況を調査する'
RETURN
  WITH top_product AS (
    -- 最も売れている商品を特定
    SELECT
      t.store_id,
      t.product_id,
      SUM(t.transaction_price * t.quantity) AS total_sales,  -- 売上金額
      ROW_NUMBER() OVER (ORDER BY SUM(t.transaction_price * t.quantity) DESC) AS row_num
    FROM
      transactions t
    WHERE
      t.store_id = p_store_id  -- 特定店舗のフィルタリング
    GROUP BY
      t.store_id, t.product_id
  )
  , product_inventory AS (
    -- 最も売れている商品の在庫状況を取得
    SELECT
      p.product_id,
      p.product_name,
      p.category,
      p.subcategory,
      p.price,
      p.stock_quantity,
      p.cost_price
    FROM products p
    WHERE
      p.product_id = p_product_id  -- 商品IDをフィルタリング
  )
  SELECT
    tp.store_id,
    pi.product_name,
    pi.category,
    pi.subcategory,
    pi.price,
    pi.stock_quantity,
    pi.cost_price,
    tp.total_sales
  FROM product_inventory pi
  JOIN top_product tp
    ON tp.product_id = pi.product_id
  WHERE tp.row_num = 1
  ORDER BY tp.total_sales DESC
""")

# COMMAND ----------

display(
  spark.sql(f"""
    SELECT * FROM get_store_product_inventory({store_id}, {product_id})
  """)
)

# COMMAND ----------

# DBTITLE 1,関数削除
# spark.sql(f'''
# DROP FUNCTION IF EXISTS get_store_product_inventory;
# ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AI Agent Playgroundで実行
# MAGIC Playground へ移動してください<br>
# MAGIC 概要：特定店舗・商品を購入しているユーザーの属性傾向をAI agentに調べてもらいます<br>
# MAGIC
# MAGIC LLM：`Claude Sonnet 4`<br>
# MAGIC ツール：
# MAGIC * `<ご自身のカタログ名>.bricksmart.get_store_sales_ranking()`　引数：`出力行数`（Option）
# MAGIC * `<ご自身のカタログ名>.bricksmart.get_store_item_sales_ranking()`　引数：`店舗ID`、`商品ID`、`出力行数`（Option）  
# MAGIC * `<ご自身のカタログ名>.bricksmart.get_store_product_inventory()`　引数：`店舗ID`、`商品ID`  
# MAGIC
# MAGIC システムプロンプト：`あなたはスーパーの売上データの専門家です。質問に対して日本語で回答します。`<br>
# MAGIC 入力例：`一番繁盛しているお店で一番人気の商品の在庫状況を教えて`

# COMMAND ----------

# MAGIC %md
# MAGIC <img src='https://github.com/komae5519pv/databricks_quick_demo/blob/main/20250912_aibi_quickworkshop/images/Playground.gif?raw=true' width='88%'/>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 実行結果
# MAGIC <img src='https://github.com/komae5519pv/databricks_quick_demo/blob/main/20250912_aibi_quickworkshop/images/playground.png?raw=true' width='88%'/>
# MAGIC
