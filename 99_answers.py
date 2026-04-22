# Databricks notebook source
# MAGIC %md
# MAGIC # 答え合わせ（講師用）
# MAGIC
# MAGIC このノートブックは **講師が答え合わせの時間に使用** します。
# MAGIC 参加者には配布しないでください。

# COMMAND ----------

catalog = "workspace"  # ← 自分のカタログ名に変更してください
schema  = "default"    # ← 自分のスキーマ名に変更してください

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"カタログ: {catalog} / スキーマ: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 問題1の答え: 売上不調の原因
# MAGIC
# MAGIC **2つの要因が重なっています。**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 答え1-A: mobileチャネルのキャンセル率急上昇（2025-07以降）
# MAGIC
# MAGIC mobileチャネルだけ、2025年7月以降にキャンセル率が約75%に跳ね上がっている。
# MAGIC web / app は変化なし → **mobileアプリ側の障害やUX問題** が疑われる。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ym, channel,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled,
# MAGIC     ROUND(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS cancel_rate_pct
# MAGIC FROM orders
# MAGIC GROUP BY ym, channel
# MAGIC ORDER BY ym, channel

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 折れ線グラフで `cancel_rate_pct` を Y軸、`ym` を X軸、`channel` を色にすると一目瞭然です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 答え1-B: 北海道ユーザーの売上急落（2025-04以降）
# MAGIC
# MAGIC 北海道のユーザーだけ、2025年4月以降に注文数が約70%減少している。
# MAGIC 他の都道府県は横ばい → **北海道への配送問題や地域固有の要因** が疑われる。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.ym, u.prefecture,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     SUM(o.total_amount) AS sales
# MAGIC FROM orders o
# MAGIC JOIN users u ON o.user_id = u.user_id
# MAGIC WHERE u.prefecture IN ('北海道', '東京都', '大阪府', '神奈川県')
# MAGIC GROUP BY o.ym, u.prefecture
# MAGIC ORDER BY o.ym, u.prefecture

# COMMAND ----------

# MAGIC %md
# MAGIC 💡 折れ線グラフで `sales` を Y軸、`ym` を X軸、`prefecture` を色にすると、北海道だけ急落していることがわかります。

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 問題2の答え: BOTによるカート占有攻撃
# MAGIC
# MAGIC **user_id 991〜998 の8名がBOTを使ってカートを大量占有している。**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: セッション数が異常なユーザーを発見

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ユーザー別のセッション数TOP20
# MAGIC SELECT user_id,
# MAGIC     COUNT(*) AS total_sessions,
# MAGIC     SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_adds,
# MAGIC     SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
# MAGIC     SUM(CASE WHEN event_type = 'abandon' THEN 1 ELSE 0 END) AS abandons,
# MAGIC     SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views
# MAGIC FROM sessions
# MAGIC GROUP BY user_id
# MAGIC ORDER BY total_sessions DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC → user_id 991〜998 が上位を独占。**カート追加とabandonだけで、viewもpurchaseもゼロ。**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: 注文データとの突き合わせ

# COMMAND ----------

# MAGIC %sql
# MAGIC -- BOTユーザーの注文を確認
# MAGIC SELECT user_id, COUNT(*) AS order_count
# MAGIC FROM orders
# MAGIC WHERE user_id BETWEEN 991 AND 998
# MAGIC GROUP BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC → **注文が一切ない。** カートに大量投入するだけで購入しないユーザー。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: プロフィールの確認

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, name, age, gender, prefecture, registration_date
# MAGIC FROM users
# MAGIC WHERE user_id BETWEEN 991 AND 998
# MAGIC ORDER BY user_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### BOT攻撃パターンのまとめ
# MAGIC
# MAGIC | 特徴 | 値 |
# MAGIC |------|-----|
# MAGIC | 対象ユーザー | user_id 991〜998（8名） |
# MAGIC | 都道府県 | **全員 埼玉県** |
# MAGIC | 登録日 | **全員 2024-11-15**（同日一斉登録） |
# MAGIC | 手口 | 高額電子機器のカートに大量投入 → 購入せずabandon |
# MAGIC | 目的 | **カート占有により在庫をロックし、一般ユーザーの購入を妨害**（転売目的の可能性） |
# MAGIC | 期間 | 2025-01 〜 2025-12（毎月40〜60セッション/人） |
# MAGIC | 特徴的な行動 | view = 0, purchase = 0（商品を見ず、買わない。BOTでカートを占有するだけ） |

# COMMAND ----------

# MAGIC %md
# MAGIC ### ディスカッション
# MAGIC
# MAGIC > 「BOTがカートを占有した結果、一般ユーザーが電子機器を購入できず、
# MAGIC > 売上が落ちていた可能性はありませんか？
# MAGIC > 問題1と問題2は、実はつながっているかもしれません。」

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## まとめ
# MAGIC
# MAGIC | 問題 | 発見 | 対策案 |
# MAGIC |------|------|--------|
# MAGIC | 売上不調① | mobileチャネルのキャンセル率急上昇（2025-07〜） | モバイルアプリの障害調査・UX改善 |
# MAGIC | 売上不調② | 北海道ユーザーの売上急落（2025-04〜） | 配送パートナーの調査・地域キャンペーン |
# MAGIC | 不審アクティビティ | 8名のBOTによるカート占有攻撃（全員埼玉県・同日登録） | アカウント凍結・BOT検知の導入・カートのタイムアウト短縮 |
