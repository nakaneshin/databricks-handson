# Databricks notebook source
# MAGIC %md
# MAGIC # データ取り込み & クレンジング
# MAGIC
# MAGIC CSV → Raw（Bronze）→ Clean（Silver）のパイプラインを実行します。
# MAGIC 各セルを **上から順に実行** してください。
# MAGIC
# MAGIC | レイヤー | 内容 |
# MAGIC |---------|------|
# MAGIC | CSV（Volume） | 生ファイル |
# MAGIC | Raw（Bronze） | そのままDeltaテーブル化 |
# MAGIC | Clean（Silver） | 型変換・不要データ除去・カラム追加 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定

# COMMAND ----------

# MAGIC %run ./00_env

# COMMAND ----------

volume_path = f"/Volumes/{catalog}/{schema}/raw_data"
print(f"Volume: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: CSV → Raw テーブル（Bronze）
# MAGIC
# MAGIC Volume上のCSVをそのままDelta形式で取り込みます。
# MAGIC データは加工せず「生のまま」保存するのがBronzeレイヤーの考え方です。

# COMMAND ----------

for name in ["users", "products", "orders", "order_items", "sessions"]:
    df = spark.read.csv(f"{volume_path}/{name}.csv", header=True, inferSchema=True)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.raw_{name}")
    print(f"✅ raw_{name}: {df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 確認してみましょう
# MAGIC
# MAGIC 左サイドバーの **カタログ** から `raw_users`、`raw_orders` などのテーブルが作成されていることを確認してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM raw_orders LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Raw → Clean テーブル（Silver）
# MAGIC
# MAGIC Rawデータをクレンジングして、分析しやすい形に整えます。
# MAGIC
# MAGIC | 変換 | 内容 |
# MAGIC |------|------|
# MAGIC | 日付型変換 | 文字列 → DATE型 |
# MAGIC | 年齢層カラム追加 | age → age_group |
# MAGIC | 年月カラム追加 | order_date → ym（"2024-01"形式） |

# COMMAND ----------

# MAGIC %md
# MAGIC ### users テーブル

# COMMAND ----------

from pyspark.sql.functions import col, to_date, when, date_format

users = (
    spark.table(f"{catalog}.{schema}.raw_users")
    .withColumn("registration_date", to_date(col("registration_date")))
    .withColumn("age_group",
        when(col("age") < 20, "10代")
        .when(col("age") < 30, "20代")
        .when(col("age") < 40, "30代")
        .when(col("age") < 50, "40代")
        .otherwise("50代以上"))
)
(users.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.users"))
print(f"✅ users: {users.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### products テーブル

# COMMAND ----------

products = spark.table(f"{catalog}.{schema}.raw_products")
(products.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.products"))
print(f"✅ products: {products.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### orders テーブル

# COMMAND ----------

orders = (
    spark.table(f"{catalog}.{schema}.raw_orders")
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("ym", date_format(col("order_date"), "yyyy-MM"))
)
(orders.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.orders"))
print(f"✅ orders: {orders.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### order_items テーブル

# COMMAND ----------

order_items = spark.table(f"{catalog}.{schema}.raw_order_items")
(order_items.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.order_items"))
print(f"✅ order_items: {order_items.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### sessions テーブル

# COMMAND ----------

sessions = (
    spark.table(f"{catalog}.{schema}.raw_sessions")
    .withColumn("session_date", to_date(col("session_date")))
    .withColumn("ym", date_format(col("session_date"), "yyyy-MM"))
)
(sessions.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.sessions"))
print(f"✅ sessions: {sessions.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Unity Catalog でテーブルを探索する
# MAGIC
# MAGIC コードを書く前に、まずは **Unity Catalog** でデータの全体像を把握しましょう。
# MAGIC これはデータ分析の第一歩として非常に重要なステップです。
# MAGIC
# MAGIC ### やってみよう
# MAGIC
# MAGIC 1. 左サイドバーの **「カタログ」** をクリック
# MAGIC 2. 自分のカタログ → スキーマ → テーブル一覧を確認
# MAGIC 3. **`orders`** テーブルをクリックして、以下を確認してみてください:
# MAGIC
# MAGIC | タブ | 確認ポイント |
# MAGIC |------|-------------|
# MAGIC | **スキーマ** | どんなカラムがあるか？ — `status`, `channel`, `category` など、分析の切り口になりそうなカラムを探しましょう |
# MAGIC | **サンプルデータ** | 実データをプレビュー — `status` にはどんな値がある？ `channel` は？ |
# MAGIC | **詳細** | 行数やサイズを確認 — データの規模感をつかみましょう |
# MAGIC
# MAGIC 4. 同様に **`users`**, **`sessions`** テーブルも覗いてみてください
# MAGIC 5. テーブル同士の関係を考えてみましょう — **どのカラムでJOINできそうですか？**
# MAGIC
# MAGIC ### テーブルのリレーション
# MAGIC
# MAGIC ```
# MAGIC users (user_id)
# MAGIC   ├── 1:N → orders (user_id)
# MAGIC   │            └── 1:N → order_items (order_id)
# MAGIC   │                         └── N:1 → products (product_id)
# MAGIC   └── 1:N → sessions (user_id)
# MAGIC                └── N:1 → products (product_id)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: ノートブックからデータを確認する
# MAGIC
# MAGIC Unity Catalog で全体像をつかんだら、ノートブックでも簡単な集計をしてみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ### users — 年齢層分布

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT age_group, COUNT(*) AS cnt
# MAGIC FROM users
# MAGIC GROUP BY age_group
# MAGIC ORDER BY age_group

# COMMAND ----------

# MAGIC %md
# MAGIC ### orders — 月別注文数
# MAGIC
# MAGIC 💡 結果テーブルの右にある **＋ > 可視化** で折れ線グラフにしてみましょう。何か気づきはありますか？

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ym, COUNT(*) AS cnt
# MAGIC FROM orders
# MAGIC GROUP BY ym
# MAGIC ORDER BY ym

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 完了
# MAGIC
# MAGIC 以下のテーブルが作成されました:
# MAGIC
# MAGIC | テーブル | 主なカラム | 分析での役割 |
# MAGIC |---------|-----------|-------------|
# MAGIC | `users` | user_id, prefecture, age_group | 顧客属性 |
# MAGIC | `products` | product_id, category, price, cost | 商品情報 |
# MAGIC | `orders` | order_id, user_id, status, channel, ym | 注文（売上分析の中心） |
# MAGIC | `order_items` | order_id, product_id, quantity, subtotal | 注文明細 |
# MAGIC | `sessions` | user_id, event_type, channel, ym | 行動ログ（カゴ落ち分析） |
# MAGIC
# MAGIC **次のノートブック** では、これらのデータに隠された **2つの問題** を調査します！
# MAGIC
# MAGIC Unity Catalog で見た情報が、分析の手がかりになるはずです。
