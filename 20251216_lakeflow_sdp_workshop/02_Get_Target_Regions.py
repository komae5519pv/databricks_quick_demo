# Databricks notebook source
# ノートブック名: 01_Get_Target_Regions
# 役割: 学習対象となる地域リストを動的に生成し、Task Valueとしてセットする

import json

print("データレイク内のアクティブな地域を確認しています...")

# 本来はデータベースなどから取得しますが、デモ用にリストを定義
# 少し多めにすると並列処理が映えます
target_regions = [
    "東京本社",
    "大阪支社",
    "ニューヨーク店舗",
    "ロンドン本部",
    "パリ路面店",
    "シンガポール拠点",
    "ベルリン中心拠点",
    "シドニー拠点"
]

print(f"処理対象となる拠点は {len(target_regions)} 件です: {target_regions}")

# 【重要】下流のFor Eachタスクにリストを渡す設定
# キー名 "regions" を覚えておいてください
dbutils.jobs.taskValues.set(key="regions", value=target_regions)

print("タスク値が正常に設定されました。")
