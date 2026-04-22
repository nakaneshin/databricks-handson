# Databricks notebook source
# MAGIC %md
# MAGIC # 環境設定
# MAGIC
# MAGIC **以下の2行を自分の環境に合わせて変更してください。**
# MAGIC
# MAGIC このノートブックは他のノートブックから `%run ./00_env` で呼び出されます。
# MAGIC ここを1回変更すれば、全てのノートブックに反映されます。

# COMMAND ----------

catalog = "workspace"  # ← 自分のカタログ名に変更してください
schema  = "default"    # ← 自分のスキーマ名に変更してください

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"カタログ: {catalog} / スキーマ: {schema}")
