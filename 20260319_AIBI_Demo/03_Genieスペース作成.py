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
# MAGIC - `{catalog}.{schema}.sales_silver`
# MAGIC
# MAGIC #### 設定<br>
# MAGIC - Title: `スーパーストアGenie`
# MAGIC - Description: `スーパーストアの注文を用いた分析アシスタント`
# MAGIC - Default Warehouse: <任意のSQL Warehouse>
# MAGIC - サンプル質問:
# MAGIC   - `カテゴリとサブカテゴリ別の売上・利益・返品率を比較して`
# MAGIC   - `顧客区分と出荷モード別に、平均注文額・平均利益を集計して`
# MAGIC   - `月別の売上推移を教えて`
# MAGIC - 一般的な指示:
# MAGIC   ```
# MAGIC   * あなたはスーパーストアの分析アシスタントです。
# MAGIC   * 必ず日本語で回答してください。
# MAGIC   * 顧客・製品・地域・期間別の売上・利益・返品状況などに関する質問に対して、プロフェッショナルなアナリストとして分析・示唆出しをします。
# MAGIC   * 回答の最後に深掘りの質問案を提示して
# MAGIC   ```
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
# MAGIC ## 3. デモの流れ
# MAGIC
# MAGIC ### Genieで試す質問
# MAGIC
# MAGIC | モード | 対象者 | 質問 |
# MAGIC |--------|--------|------|
# MAGIC | **チャット** | 全員 | カテゴリ別の売上と利益を教えて |
# MAGIC | **チャット** | 全員 | 返品率が高い商品カテゴリは？ |
# MAGIC | **チャット** | 全員 | 月別の売上推移を教えて |
# MAGIC | **エージェント** | 経営層 | スーパーストアの現状を経営層にわかりやすく報告してください |
# MAGIC | **エージェント** | 営業・マーケ | スーパーストアのお客様について、営業チームにわかりやすく説明してください |
# MAGIC | **エージェント** | 現場 | スーパーストアで今起きている問題を、現場の人にわかりやすく伝えてください |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### ダッシュボードと組み合わせたデモ
# MAGIC 1. **ダッシュボードで全体像を確認**
# MAGIC    - クロスフィルタリング（地域を選択 → 全グラフ連動）
# MAGIC    - ドリルダウン（カテゴリ → サブカテゴリ）
# MAGIC    - 予測モード（月別売上の将来予測）
# MAGIC    - Explain changes → Genie表示
# MAGIC    - **Genieに質問を投げる**:
# MAGIC      - 「この売上減少の原因を詳しく分析して」
# MAGIC      - 「なぜ関東地域が好調なのか教えて」
# MAGIC      - 「返品率を下げるための具体的な施策を提案して」
# MAGIC 2. **Genieで詳細分析・深掘り**
# MAGIC 3. **エージェントモードで示唆と改善提案を取得**
