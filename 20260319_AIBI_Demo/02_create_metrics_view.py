# Databricks notebook source
# MAGIC %md
# MAGIC # メトリクスビュー作成ノートブック
# MAGIC
# MAGIC UCメトリクスビューを作成し、ダッシュボードで使用します。

# COMMAND ----------

# DBTITLE 1,設定読み込み
# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. メトリクスビューを作成
# MAGIC
# MAGIC **手順：**
# MAGIC 1. Catalog Explorerで `komae_demo_v4.aibi_superstore.sales_silver` を右クリック
# MAGIC 2. **Create** → **Metric View** を選択
# MAGIC 3. 名前：`sales_silver_metric_view`
# MAGIC 4. 下記のYAMLをコピー＆ペースト
# MAGIC 5. **Save**

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #### メトリックビュー YAML
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC ##### スーパーストア売上メトリクスビュー
# MAGIC <details>
# MAGIC   <summary>YAMLを展開</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">クリップボードにコピー</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC version: 1.1
# MAGIC
# MAGIC source: komae_demo_v4.aibi_superstore.sales_silver
# MAGIC
# MAGIC dimensions:
# MAGIC   - name: order_month
# MAGIC     expr: date_trunc('MONTH', source.order_date)
# MAGIC     comment: "オーダー月（月次集計用）"
# MAGIC     display_name: オーダー月
# MAGIC     format:
# MAGIC       type: date
# MAGIC       date_format: year_month_day
# MAGIC       leading_zeros: false
# MAGIC   - name: category
# MAGIC     expr: source.category
# MAGIC     comment: "カテゴリ（家具/電化製品/オフィス用品）"
# MAGIC     display_name: カテゴリ
# MAGIC   - name: sub_category
# MAGIC     expr: source.sub_category
# MAGIC     comment: "サブカテゴリ"
# MAGIC     display_name: サブカテゴリ
# MAGIC   - name: region
# MAGIC     expr: source.region
# MAGIC     comment: "地域（関東/関西/中部/九州）"
# MAGIC     display_name: 地域
# MAGIC   - name: prefecture
# MAGIC     expr: source.prefecture
# MAGIC     comment: "都道府県"
# MAGIC     display_name: 都道府県
# MAGIC   - name: customer_segment
# MAGIC     expr: source.customer_segment
# MAGIC     comment: "顧客区分（消費者/企業/小規模オフィス）"
# MAGIC     display_name: 顧客区分
# MAGIC
# MAGIC measures:
# MAGIC   - name: total_sales
# MAGIC     expr: sum(source.sales_amount)
# MAGIC     comment: "売上合計"
# MAGIC     display_name: 売上合計
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC       hide_group_separator: false
# MAGIC       abbreviation: none
# MAGIC     synonyms:
# MAGIC       - 売上
# MAGIC       - 総売上
# MAGIC   - name: total_profit
# MAGIC     expr: sum(source.profit)
# MAGIC     comment: "利益合計"
# MAGIC     display_name: 利益合計
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC       hide_group_separator: false
# MAGIC       abbreviation: none
# MAGIC     synonyms:
# MAGIC       - 利益
# MAGIC       - 総利益
# MAGIC   - name: total_quantity
# MAGIC     expr: sum(source.quantity)
# MAGIC     comment: "販売数量"
# MAGIC     display_name: 販売数量
# MAGIC     synonyms:
# MAGIC       - 数量
# MAGIC       - 売上個数
# MAGIC   - name: order_count
# MAGIC     expr: count(distinct source.order_id)
# MAGIC     comment: "オーダー数"
# MAGIC     display_name: オーダー数
# MAGIC     synonyms:
# MAGIC       - 注文数
# MAGIC       - オーダー件数
# MAGIC   - name: customer_count
# MAGIC     expr: count(distinct source.customer_id)
# MAGIC     comment: "顧客数"
# MAGIC     display_name: 顧客数
# MAGIC     synonyms:
# MAGIC       - ユニーク顧客数
# MAGIC   - name: profit_margin
# MAGIC     expr: (MEASURE(total_profit)) / NULLIF(MEASURE(total_sales), 0)
# MAGIC     comment: "利益率（利益÷売上）"
# MAGIC     display_name: 利益率
# MAGIC     format:
# MAGIC       type: percentage
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 1
# MAGIC     synonyms:
# MAGIC       - マージン
# MAGIC   - name: avg_order_value
# MAGIC     expr: MEASURE(total_sales) / NULLIF(MEASURE(order_count), 0)
# MAGIC     comment: "平均オーダー単価"
# MAGIC     display_name: 平均オーダー単価
# MAGIC     format:
# MAGIC       type: number
# MAGIC       decimal_places:
# MAGIC         type: exact
# MAGIC         places: 0
# MAGIC       hide_group_separator: false
# MAGIC       abbreviation: none
# MAGIC </code></pre>
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("クリップボードにコピーしました"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("クリップボードにコピーしました");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("コピーできませんでした。手動でコピーしてください。");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. メトリクスビューをクエリ

# COMMAND ----------

# DBTITLE 1,メトリクスビューの確認
spark.sql(f"""
SELECT
  order_month,
  MEASURE(total_sales) AS `売上合計`,
  MEASURE(total_profit) AS `利益合計`,
  MEASURE(order_count) AS `オーダー数`,
  MEASURE(profit_margin) AS `利益率`
FROM {catalog}.{schema}.sales_silver_metric_view
GROUP BY order_month
ORDER BY order_month
LIMIT 12
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ メトリクスビュー作成完了！
# MAGIC
# MAGIC 次のステップ：ダッシュボードを開いて、メトリクスが正常に表示されることを確認します。
