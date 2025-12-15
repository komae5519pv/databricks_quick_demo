# Databricks notebook source
# 何らかの処理やチェックを行う
row_count = 100 
condition_result = "process_data" if row_count > 0 else "skip"

# タスクバリューとして値をセット
# key: 下流のジョブで参照する変数名
# value: 渡したい値
dbutils.jobs.taskValues.set(key="run_status", value=condition_result)

print(f"Set task value 'run_status' to: {condition_result}")
