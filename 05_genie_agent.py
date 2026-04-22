# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Code エージェントモード
# MAGIC
# MAGIC Genie Code の **エージェントモード** を使うと、プロンプトを与えるだけで
# MAGIC AI が自動的にセルを追加しながら分析を進めてくれます。
# MAGIC
# MAGIC **使い方:**
# MAGIC 1. 右上のランプアイコンから Genie Code を開く
# MAGIC 2. **Agent** モードに切り替える
# MAGIC 3. 下のプロンプト例をコピーして貼り付ける
# MAGIC 4. AI がセルを追加・実行していく様子を観察する
# MAGIC
# MAGIC **注意:** Agent モードは多くのクエリを自動実行するため、時間がかかる場合があります。

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.widgets.text("catalog", "workspace", "カタログ名")
# MAGIC dbutils.widgets.text("schema",  "default", "スキーマ名")
# MAGIC
# MAGIC catalog = dbutils.widgets.get("catalog")
# MAGIC schema  = dbutils.widgets.get("schema")
# MAGIC
# MAGIC spark.sql(f"USE CATALOG {catalog}")
# MAGIC spark.sql(f"USE SCHEMA {schema}")
# MAGIC
# MAGIC print(f"カタログ: {catalog} / スキーマ: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## プロンプト例 1: 売上分析
# MAGIC
# MAGIC 以下をコピーして Agent モードに貼り付けてください。
# MAGIC
# MAGIC ```
# MAGIC このスキーマにあるECサイトのデータを分析してください。
# MAGIC 2025年半ば以降、売上が減少しています。
# MAGIC 原因を多角的に調査し、グラフで可視化しながら、
# MAGIC 最後にMarkdownで調査レポートをまとめてください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## プロンプト例 2: 不審アクティビティ調査
# MAGIC
# MAGIC ```
# MAGIC sessionsテーブルに不審なBOTのアクティビティが
# MAGIC 含まれている可能性があります。
# MAGIC 通常ユーザーと異なる行動パターンを持つユーザーを特定し、
# MAGIC その特徴をusersテーブルと照合して報告してください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## プロンプト例 3: 成長戦略の提案
# MAGIC
# MAGIC ```
# MAGIC このECサイトの強みを分析してください。
# MAGIC 売れ筋商品、優良顧客の特徴、成長しているチャネルを
# MAGIC データから明らかにし、次の成長戦略を提案してください。
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC （↓ Agent がここから先にセルを追加していきます）
