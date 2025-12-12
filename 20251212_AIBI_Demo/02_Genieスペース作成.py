# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Spaceを作る

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 1. Genie Spaceを作る
# MAGIC ![Genieスペース作成.gif](./_image/Genieスペース作成.gif "Genieスペース作成.gif")
# MAGIC
# MAGIC #### データ
# MAGIC - `sales_silver`
# MAGIC
# MAGIC #### 設定<br>
# MAGIC - Title: `サンプル_スーパーストアGenie`
# MAGIC - Description: `スーパーストアの注文を用いた分析アシスタント`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC - サンプル質問:
# MAGIC   - `2016年以降で、カテゴリとサブカテゴリ別の売上・利益・返品率を比較し、特に利益率が低いか返品率が高い組み合わせを教えて`
# MAGIC   - `地域マネージャーごとに、担当地域別の売上・利益・予算達成率を時系列で可視化して、直近1年間で成長が頭打ちになっているエリアを特定して`
# MAGIC   - `顧客区分と出荷モード別に、平均注文額・平均利益・リードタイム（日数）を集計し、どの組み合わせが最も収益性が高いかを示して`
# MAGIC
# MAGIC ##### <SQL式>
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`本日`
# MAGIC - コード：`from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE`
# MAGIC - 同義語：`今日, 本日`
# MAGIC - 指示：`現在を起点とする場合、必ず「SELECT from_utc_timestamp(CURRENT_TIMESTAMP(), 'Asia/Tokyo')::DATE;」で日時特定してください。`
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`受注月`
# MAGIC - コード：`DATE_TRUNC('month', sales_silver.order_date)`
# MAGIC - 同義語：`注文月, オーダー月`
# MAGIC - 指示：`月次集計用の時間ディメンションです。`
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`受注曜日（Weekday）`
# MAGIC - コード：`DATE_FORMAT(sales_silver.order_date, 'E')`
# MAGIC - 同義語：`曜日, day of week, weekday`
# MAGIC - 指示：
# MAGIC ```
# MAGIC このディメンションは、受注日を曜日（Mon / Tue / Wed…）に変換した値です。
# MAGIC 曜日別の売上・利益・注文数などの傾向を分析する際に使用します。
# MAGIC 並び順はカレンダー順（Mon → Sun）を想定してください。
# MAGIC 平日の傾向や週末需要の違いを理解したいときに役立ちます。
# MAGIC ```
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`受注年（Order Year）`
# MAGIC - コード：`YEAR(sales_silver.order_date)`
# MAGIC - 同義語：`年, year, 受注年`
# MAGIC - 指示：
# MAGIC ```
# MAGIC 受注日から抽出した暦年（YYYY）を示します。
# MAGIC 年度比較・前年同期比の分析に使用します。
# MAGIC ```
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`受注四半期（Quarter）`
# MAGIC - コード：`QUARTER(sales_silver.order_date)`
# MAGIC - 同義語：`四半期, quarter, Q1 Q2 Q3 Q4`
# MAGIC - 指示：
# MAGIC ```
# MAGIC 受注日を四半期（1〜4）に分類したディメンションです。
# MAGIC 四半期ごとの売上や利益の推移を分析する際に使用します。
# MAGIC Q1〜Q4 の順に並べる前提です。
# MAGIC ```
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`リードタイムカテゴリ（Lead Time Category）`
# MAGIC - コード：
# MAGIC ```
# MAGIC CASE
# MAGIC   WHEN DATEDIFF(sales_silver.ship_date, sales_silver.order_date) <= 1 THEN '即日・翌日'
# MAGIC   WHEN DATEDIFF(sales_silver.ship_date, sales_silver.order_date) <= 3 THEN '1〜3日'
# MAGIC   WHEN DATEDIFF(sales_silver.ship_date, sales_silver.order_date) <= 7 THEN '4〜7日'
# MAGIC   ELSE '1週間以上'
# MAGIC END
# MAGIC ```
# MAGIC - 同義語：`リードタイム区分, lead time, 配送速度カテゴリ`
# MAGIC - 指示：
# MAGIC ```
# MAGIC 注文から出荷までの日数をカテゴリ化した指標です。
# MAGIC 配送速度の違いによる顧客満足度や売上への影響を分析する際に使用します。
# MAGIC 数値として扱うのではなくカテゴリ（ラベル）として処理してください。
# MAGIC ```
# MAGIC
# MAGIC - タイプ：`ディメンション`
# MAGIC - 名前：`利益率カテゴリ（Margin Category）`
# MAGIC - コード：
# MAGIC ```
# MAGIC CASE
# MAGIC   WHEN sales_silver.profit / NULLIF(sales_silver.sales_amount, 0) >= 0.2 THEN '高利益'
# MAGIC   WHEN sales_silver.profit / NULLIF(sales_silver.sales_amount, 0) >= 0.1 THEN '中利益'
# MAGIC   ELSE '低利益'
# MAGIC END
# MAGIC ```
# MAGIC - 同義語：`マージン区分, profit margin level, 利益カテゴリ`
# MAGIC - 指示：
# MAGIC ```
# MAGIC 利益率の高さをカテゴリ分けした指標です。
# MAGIC 高利益・中利益・低利益の組み合わせを比較し、
# MAGIC どの顧客層や商品が収益に貢献しているかを把握するために使用します。
# MAGIC ```
# MAGIC
# MAGIC - タイプ：`メジャー`
# MAGIC - 名前：`平均利益率`
# MAGIC - コード：`AVG(sales_silver.profit / NULLIF(sales_silver.sales_amount, 0))`
# MAGIC - 同義語：`利益率, マージン, profit margin`
# MAGIC - 指示：`売上に対する利益の割合（平均）を表します。0〜1の値で、1に近いほど高い利益率です。`
# MAGIC
# MAGIC - タイプ：`メジャー`
# MAGIC - 名前：`平均リードタイム（日）`
# MAGIC - コード：`AVG(DATEDIFF(sales_silver.ship_date, sales_silver.order_date))`
# MAGIC - 同義語：`リードタイム, 平均配送日数, lead time`
# MAGIC - 指示：`受注日から出荷日までの日数の平均値です。数値が小さいほどリードタイムが短く、配送が早いことを意味します。`
# MAGIC
# MAGIC - タイプ：`メジャー`
# MAGIC - 名前：`返品率`
# MAGIC - コード：`AVG(CASE WHEN sales_silver.returned THEN 1 ELSE 0 END)`
# MAGIC - 同義語：`return rate, 返品割合`
# MAGIC - 指示：`返品された注文の割合（平均）です。0〜1の値で、1に近いほど返品が多いことを意味します。`
# MAGIC
# MAGIC
# MAGIC ##### <SQLクエリ>
# MAGIC SQL関数:<br>
# MAGIC `{catalog名}.{schema名}.segment_shipmode_performance`<br>
# MAGIC
# MAGIC SQL関数:<br>
# MAGIC 質問: `今後売上が伸びる顧客層はどこですか？`<br>
# MAGIC サンプルクエリ: ※カタログ名、スキーマ名は修正してください<br>
# MAGIC ```
# MAGIC -- 顧客セグメント × 月次の売上を集計
# MAGIC WITH monthly_sales AS (
# MAGIC   SELECT
# MAGIC     customer_segment,
# MAGIC     DATE_TRUNC('month', order_date) AS month,
# MAGIC     SUM(sales_amount) AS total_sales
# MAGIC   FROM komae_demo_v4.aibi.sales_silver
# MAGIC   GROUP BY customer_segment, month
# MAGIC ),
# MAGIC
# MAGIC -- 予測の終了時刻（最後の観測月から6ヶ月後）を計算
# MAGIC limits AS (
# MAGIC   SELECT
# MAGIC     MAX(month) AS max_month
# MAGIC   FROM monthly_sales
# MAGIC ),
# MAGIC
# MAGIC -- AI_FORECAST による未来予測（6か月先まで）
# MAGIC forecast AS (
# MAGIC   SELECT
# MAGIC     customer_segment,
# MAGIC     month,                     -- 予測対象の月
# MAGIC     total_sales_forecast,      -- 予測売上
# MAGIC     total_sales_upper,         -- 上側予測区間
# MAGIC     total_sales_lower          -- 下側予測区間
# MAGIC   FROM AI_FORECAST(
# MAGIC     TABLE (monthly_sales),
# MAGIC     horizon   => (SELECT ADD_MONTHS(max_month, 6) FROM limits),
# MAGIC     time_col  => 'month',
# MAGIC     value_col => 'total_sales',
# MAGIC     group_col => 'customer_segment'
# MAGIC   )
# MAGIC )
# MAGIC
# MAGIC SELECT *
# MAGIC FROM forecast
# MAGIC ORDER BY customer_segment, month;
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Genie Spaceに権限を付与する
# MAGIC
# MAGIC ここでは、`all workspace user`に付与する（デモなので）
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### デモの流れ
# MAGIC ```
# MAGIC <ダッシュボード>
# MAGIC ・表現ひと通り
# MAGIC ・グローバルフィルタ
# MAGIC ・SQL式
# MAGIC 　直近1週間の売り上げを日次推移で教えて
# MAGIC
# MAGIC ・ダッシュボードグラフ作成
# MAGIC 　地域別の売上を棒グラフで表現（カテゴリごとに色分け）
# MAGIC 　→ ドリルスルー表現
# MAGIC
# MAGIC 　月別売上推移
# MAGIC 　→ Explain this decrease
# MAGIC 　→ 予測
# MAGIC
# MAGIC ・編集＆公開モード
# MAGIC ・外部公開の権限
# MAGIC
# MAGIC <Genie>
# MAGIC ・チャット
# MAGIC 顧客区分 × 出荷モードごとに、平均注文額・平均利益・リードタイム（日数）を比較して、どの組み合わせが最も収益性が高いのか教えてください。
# MAGIC あわせて、改善が必要そうな組み合わせもあれば指摘してください。
# MAGIC
# MAGIC ・SQL関数
# MAGIC 2017〜2019年で、顧客セグメント×出荷モードごとに比較して。その上で優秀・要改善な組み合わせを見つけて、その特徴と改善ポイントを教えて。
# MAGIC
# MAGIC ・サンプルクエリ
# MAGIC 今後売上が伸びる顧客層はどこですか？月次売上のAI予測を出して教えてください。
# MAGIC
# MAGIC ・SQL式
# MAGIC リードタイムのカテゴリごとに売上・返品率・利益率を比較して、
# MAGIC 配送速度がパフォーマンスにどう影響しているか教えてください。
# MAGIC
# MAGIC 商品カテゴリ別に返品率を比較し、
# MAGIC 返品の多いカテゴリに共通する特徴があればまとめてください。
# MAGIC
# MAGIC ・リサーチエージェント
# MAGIC 顧客区分 × 配送方法の組み合わせについて、収益性の観点から最重要なポイントを3つに絞って教えてください。
# MAGIC 特に、短期で改善インパクトが大きい領域を優先的に挙げてください。
# MAGIC
# MAGIC
# MAGIC <メトリクスビュー>
# MAGIC 売上を購入人数で割った顧客単価という新規指標を作成して
# MAGIC
# MAGIC 受注日について、月次・週次変換した新しいディメンションを作成してください
# MAGIC
# MAGIC 売上合計、売上平均のメトリクスを作成してください
# MAGIC 収益がマイナスのデータは除出ください
# MAGIC
# MAGIC → Genie に聞く
# MAGIC 月別の顧客単価の推移は？
# MAGIC ```
