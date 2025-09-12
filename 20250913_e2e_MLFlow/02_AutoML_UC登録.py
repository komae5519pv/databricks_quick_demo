# Databricks notebook source
# MAGIC %md # 02 AutoMLによるモデル作成・UCへのモデル登録
# MAGIC
# MAGIC このノートブックでは、01で作った特徴量を使ってベストなモデルを作成し、Unity Catalogにベストモデルを登録します
# MAGIC
# MAGIC DBR 15.4 ML 以降をお使いください
# MAGIC <img src='https://github.com/komae5519pv/komae_dbdemos/blob/main/e2e_ML_20250629/_data/_imgs/2_automl.png?raw=true' width='1200' />

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. トレーニングデータのロード
# MAGIC

# COMMAND ----------

df = spark.table(f"{MY_CATALOG}.{MY_SCHEMA}.churn_features")
df = df.drop('customerID')

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. AutoMLトレーニング

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-1. AutoMLエクスペリメントの実行
# MAGIC AutoMLでモデルをトレーニングし、最良モデルを特定します。<br>
# MAGIC
# MAGIC 先ほど作成した特徴量テーブル(`churn_features`)を選択するだけでOKです。<br>
# MAGIC - ML問題タイプは、今回は分類です。<br>
# MAGIC - 予測ターゲットは`churn`カラムです。<br>
# MAGIC - 評価メトリクスは`F1スコア`を使います。<br>
# MAGIC - Demo時には時間短縮のため、5分にセットします。<br>
# MAGIC
# MAGIC この作業はUIでも行えますが、ここでは[python API](https://docs.databricks.com/aws/ja/machine-learning/automl#automl-python-api-1)で操作します。

# COMMAND ----------

from databricks import automl

# AutoMLエクスペリメントの実行
summary = automl.classify(
    dataset=df,
    target_col="churn",
    primary_metric="f1",  # f1
    timeout_minutes=5     # 5分
)

# 最良モデルのrun_idを取得
best_run_id = summary.best_trial.mlflow_run_id


# COMMAND ----------

print(best_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2-2. テスト推論
# MAGIC [Python MLflow モデルのロード]{https://learn.microsoft.com/ja-jp/azure/databricks/mlflow/models}
# MAGIC MLflowにログされたモデルをMlflow.pyfuncでロードし、正常に使えるかテストします

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2-2-1. Python汎用関数としてモデルロード & Python環境での推論
# MAGIC MLflowモデルの場合、Python汎用関数としてモデルをロードしてシングルノードで推論できます。
# MAGIC - メソッド: `mlflow.pyfunc.load_model()`
# MAGIC - 目的: 単一ノードのPython環境でモデルを直接実行し、小規模データやローカルテストに適しています。<br>
# MAGIC - 実行環境: Pythonプロセス内で動作（例: Pandas DataFrameやNumPy配列の入力）。<br>
# MAGIC - スケーラビリティ: 単一マシンのリソース制限あり。大規模データには向かない。<br>
# MAGIC - ユースケース: 開発中の検証、小規模バッチ推論、APIサーバーでのリアルタイム推論

# COMMAND ----------

import mlflow
import pandas as pd

# MLflowモデルのロード
logged_model = f'runs:/{best_run_id}/model'
loaded_model = mlflow.pyfunc.load_model(model_uri=logged_model)

# Spark DataFrameをpandas DataFrameに変換
pdf = df.toPandas()

# 推論
predictions = loaded_model.predict(pdf)

# pandas DataFrameをSpark DataFrameへ変換
predictions_df = spark.createDataFrame(pdf.assign(predictions=predictions))
display(predictions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 2-2-2. Spark UDFとしてモデルロード & 分散推論
# MAGIC Spark UDF としてモデルをロードして分散推論できます。
# MAGIC - メソッド: `mlflow.pyfunc.spark_udf()`<br>
# MAGIC - 目的: Sparkクラスター上でモデルを分散処理し、大規模データやストリーミングに適しています<br>
# MAGIC - 実行環境: Spark UDF（User Defined Function）として登録され、Sparkエンジンで分散実行<br>
# MAGIC - スケーラビリティ: クラスターリソースを活用し、TB級データでも効率的<br>
# MAGIC - ユースケース: バッチジョブ（例: 全顧客データのスコアリング）、Sparkストリーミング処理

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col

logged_model = f'runs:/{best_run_id}/model'

# Spark UDFとしてモデルをロード
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Sparkデータフレームでの予測
predictions_df = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns))))
display(predictions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. モデルのUnity Catalog(UC)への登録

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-1. UC登録
# MAGIC MLflowを使用して、最良モデルをUCに登録します。

# COMMAND ----------

# best_run_id = "beb9b8563f6844a181da4ff05a1f2d1a"

# COMMAND ----------

from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
import databricks.sdk.service.catalog as c

# Databricks Unity Catalogを使用してモデルを保存します
mlflow.set_registry_uri('databricks-uc')
client = MlflowClient()

# カタログにモデルを追加
latest_model = mlflow.register_model(f'runs:/{best_run_id}/model', MODEL_NAME)

# UCエイリアスを使用してプロダクション対応としてフラグを立てる
client.set_registered_model_alias(name=f"{MODEL_NAME}", alias="prod", version=latest_model.version)

# WorkspaceClientのインスタンスを作成
sdk_client = WorkspaceClient()

# 全ユーザー(グループ名: account users)にモデルの権限を設定
sdk_client.grants.update(c.SecurableType.FUNCTION, f"{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}", 
                         changes=[c.PermissionsChange(add=[c.Privilege["ALL_PRIVILEGES"]], principal="account users")])

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3-2. テスト推論

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3-2-1. Python汎用関数としてモデルロード & Python環境での推論
# MAGIC MLflowモデルの場合、Python汎用関数としてモデルをロードしてシングルノードで推論できます。
# MAGIC - メソッド: `mlflow.pyfunc.load_model()`
# MAGIC - 目的: 単一ノードのPython環境でモデルを直接実行し、小規模データやローカルテストに適しています。<br>
# MAGIC - 実行環境: Pythonプロセス内で動作（例: Pandas DataFrameやNumPy配列の入力）。<br>
# MAGIC - スケーラビリティ: 単一マシンのリソース制限あり。大規模データには向かない。<br>
# MAGIC - ユースケース: 開発中の検証、小規模バッチ推論、APIサーバーでのリアルタイム推論

# COMMAND ----------

import mlflow

# MLflowモデルのロード
model_uri = f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod"
loaded_model = mlflow.pyfunc.load_model(model_uri)

# Spark DataFrameをpandas DataFrameに変換
pdf = df.toPandas()

# 推論
predictions = loaded_model.predict(pdf)

# pandas DataFrameをSpark DataFrameへ変換
predictions_df = spark.createDataFrame(pdf.assign(predictions=predictions))
display(predictions_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3-2-2. Spark UDFとしてモデルロード & 分散推論
# MAGIC Spark UDF としてモデルをロードして分散推論できます。
# MAGIC - メソッド: `mlflow.pyfunc.spark_udf()`<br>
# MAGIC - 目的: Sparkクラスター上でモデルを分散処理し、大規模データやストリーミングに適しています<br>
# MAGIC - 実行環境: Spark UDF（User Defined Function）として登録され、Sparkエンジンで分散実行<br>
# MAGIC - スケーラビリティ: クラスターリソースを活用し、TB級データでも効率的<br>
# MAGIC - ユースケース: バッチジョブ（例: 全顧客データのスコアリング）、Sparkストリーミング処理

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col

# Spark UDFとしてモデルをロード
model_uri = f"models:/{MY_CATALOG}.{MY_SCHEMA}.{MODEL_NAME}@prod"
spark_udf = mlflow.pyfunc.spark_udf(spark, model_uri)

# Sparkデータフレームでの予測
predictions_df = df.withColumn('predictions', spark_udf(struct(*map(col, df.columns))))
display(predictions_df.limit(10))
