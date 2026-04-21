-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ECサイト データ調査
-- MAGIC
-- MAGIC このECサイトには **2つの問題** が隠れています。データを分析して見つけ出してください！
-- MAGIC
-- MAGIC **使えるもの:**
-- MAGIC - SQL（このノートブックのデフォルト言語）
-- MAGIC - Python（セル先頭に `%python` を付ける）
-- MAGIC - `display()` による可視化
-- MAGIC - Genie Code（ランプアイコン）
-- MAGIC - 自分でテーブルを作ってもOK
-- MAGIC
-- MAGIC **利用可能なテーブル:**
-- MAGIC
-- MAGIC | テーブル | 主なカラム |
-- MAGIC |---------|-----------|
-- MAGIC | `users` | user_id, name, age, gender, prefecture, age_group |
-- MAGIC | `products` | product_id, product_name, category, price, cost |
-- MAGIC | `orders` | order_id, user_id, order_date, status, total_amount, channel, category, ym |
-- MAGIC | `order_items` | item_id, order_id, product_id, quantity, unit_price, subtotal |
-- MAGIC | `sessions` | session_id, user_id, product_id, category, event_type, session_date, channel, ym |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 環境設定

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("catalog", "workspace", "カタログ名")
-- MAGIC dbutils.widgets.text("schema",  "default", "スキーマ名")
-- MAGIC
-- MAGIC catalog = dbutils.widgets.get("catalog")
-- MAGIC schema  = dbutils.widgets.get("schema")
-- MAGIC
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC spark.sql(f"USE SCHEMA {schema}")
-- MAGIC
-- MAGIC print(f"カタログ: {catalog} / スキーマ: {schema}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## まずはデータの全体像を把握しましょう
-- MAGIC
-- MAGIC 調査を始める前に、データの概要を確認します。

-- COMMAND ----------

-- 月別の売上推移（成立した注文のみ）
SELECT ym, COUNT(*) AS order_count, SUM(total_amount) AS total_sales
FROM orders
WHERE status = 'completed'
GROUP BY ym
ORDER BY ym

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 上のグラフを見て、気になるポイントはありますか？
-- MAGIC
-- MAGIC 💡 **ヒント**: 結果テーブルの右にある **＋ > 可視化** で折れ線グラフにすると傾向が見やすくなります。

-- COMMAND ----------

-- チャネル別の注文数
SELECT channel, COUNT(*) AS cnt
FROM orders
GROUP BY channel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 🔍 問題1: 売上減少の原因を特定せよ
-- MAGIC
-- MAGIC **2025年半ば以降、ECサイト全体の売上（completed注文）が減少傾向にあります。**
-- MAGIC
-- MAGIC 経営陣から「原因を突き止めてほしい」と依頼を受けました。
-- MAGIC 売上減少は**単一の原因ではない**可能性があります。多角的に調査してください。
-- MAGIC
-- MAGIC ### 調査の切り口
-- MAGIC - カテゴリ別 / チャネル別 / 地域別 / ステータス別
-- MAGIC - 上記の掛け合わせ
-- MAGIC
-- MAGIC 自由にセルを追加して調査してください。Genie Code を使っても構いません！

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 調査スペース（自由に使ってください）

-- COMMAND ----------

-- 例: カテゴリ別の売上推移
SELECT ym, category, SUM(total_amount) AS total_sales
FROM orders
WHERE status = 'completed'
GROUP BY ym, category
ORDER BY ym, category

-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 💡 ヒント（行き詰まったら読んでください）
-- MAGIC
-- MAGIC **観点A**: 売上が落ちているのは全チャネル一律ですか？チャネルごとに分けると見えるものがあるかもしれません。
-- MAGIC
-- MAGIC **観点B**: 全国一律に落ちていますか？地域差はありませんか？`users` テーブルと組み合わせると見えてくることがあります。
-- MAGIC
-- MAGIC **観点C**: 「成立した注文数」だけでなく、「注文の結末（ステータス）」にも注目してみてください。
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC Genie Code に聞いてみるのも有効です。例:
-- MAGIC - 「チャネルごとの注文傾向に違いはありますか？」
-- MAGIC - 「都道府県別に売上の変化を見せてください」

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 🔍 問題2: BOT攻撃の実態を解明せよ
-- MAGIC
-- MAGIC **SOC（セキュリティオペレーションセンター）から連絡がありました。**
-- MAGIC
-- MAGIC > 「BOTらしきIPアドレスからの大量アクセスを検知しました。
-- MAGIC > どんなユーザーが、どんな攻撃を仕掛けてきているか調査してください。」
-- MAGIC
-- MAGIC `orders`（注文データ）だけでなく、**`sessions`（行動ログ）** にも目を向けてください。
-- MAGIC BOTは注文するとは限りません。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 調査スペース（自由に使ってください）

-- COMMAND ----------

-- 例: セッションにユーザー情報を結合して年齢層別のイベント分布を見る
SELECT u.age_group, s.event_type, COUNT(*) AS cnt
FROM sessions s
JOIN users u ON s.user_id = u.user_id
GROUP BY u.age_group, s.event_type
ORDER BY u.age_group, s.event_type

-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- ここに自分のクエリを書いてみましょう


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 💡 ヒント（行き詰まったら読んでください）
-- MAGIC
-- MAGIC **観点D**: 普通のユーザーはどんな行動パターンをしますか？ 閲覧 → カート → 購入。では、この流れから大きく外れているユーザーはいませんか？
-- MAGIC
-- MAGIC **観点E**: 異常なユーザーを見つけたら、`orders` テーブルにそのユーザーの注文はあるか確認してみましょう。そして `users` テーブルでプロフィールも確認してみてください。
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC Genie Code に聞いてみるのも有効です。例:
-- MAGIC - 「セッション数が異常に多いユーザーはいますか？」
-- MAGIC - 「カートに入れる回数が多いのに購入しないユーザーは？」

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 調査レポート
-- MAGIC
-- MAGIC 調査結果をまとめてみましょう。
-- MAGIC
-- MAGIC **やり方**: 下のセルをダブルクリックして編集するか、Genie Code に
-- MAGIC 「これまでの分析結果をMarkdownでまとめて」と指示してみてください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 問題1: 売上減少の原因
-- MAGIC
-- MAGIC **発見した事実:**
-- MAGIC - （ここに記入）
-- MAGIC
-- MAGIC **いつから:** （ここに記入）
-- MAGIC
-- MAGIC **どこで:** （ここに記入）
-- MAGIC
-- MAGIC **なぜ（仮説）:** （ここに記入）
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 問題2: BOT攻撃の実態
-- MAGIC
-- MAGIC **攻撃者（ユーザーID）:** （ここに記入）
-- MAGIC
-- MAGIC **攻撃の手口:** （ここに記入）
-- MAGIC
-- MAGIC **攻撃者の共通点:** （ここに記入）
-- MAGIC
-- MAGIC **想定される目的:** （ここに記入）
-- MAGIC
-- MAGIC ---
-- MAGIC
-- MAGIC ### 対策提案
-- MAGIC
-- MAGIC - （ここに記入）

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## 🚀 チャレンジ: 攻めの分析
-- MAGIC
-- MAGIC 問題1・2で「守り」の分析をしました。ここからは **「攻め」** です。
-- MAGIC このECサイトの **強みを見つけ、次の成長戦略を考えましょう。**
-- MAGIC
-- MAGIC 時間の許す限り、好きなものに取り組んでください。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### チャレンジ1: 売れ筋商品TOP10（簡単）
-- MAGIC
-- MAGIC このECサイトで **最も売れている商品TOP10** を出してください。
-- MAGIC 商品名・カテゴリ・販売数量がわかるようにしましょう。
-- MAGIC
-- MAGIC 💡 **ヒント**: `order_items` と `products` を `product_id` でJOINし、`quantity` を集計します。

-- COMMAND ----------

-- チャレンジ1: ここにクエリを書いてみましょう


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### チャレンジ2: 優良顧客TOP10の共通点は？（中級）
-- MAGIC
-- MAGIC 購入回数・購入金額が多い **上位10名の優良顧客** を特定し、
-- MAGIC 彼らに **共通する属性**（年代、地域、よく使うチャネル等）を調べてください。
-- MAGIC
-- MAGIC 「どんな人がたくさん買ってくれるのか？」を明らかにしましょう。
-- MAGIC
-- MAGIC 💡 **ヒント**: `orders` で `user_id` ごとに集計 → `users` とJOIN

-- COMMAND ----------

-- チャレンジ2: ここにクエリを書いてみましょう


-- COMMAND ----------

-- チャレンジ2: 続き（属性の深掘り等）


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### チャレンジ3: どのチャネルに投資すべき？（中級）
-- MAGIC
-- MAGIC `sessions` テーブルを使って、チャネル別（web / mobile / app）の
-- MAGIC **コンバージョン率**（view → purchase の割合）を比較してください。
-- MAGIC
-- MAGIC 「どのチャネルが最も効率よく購入につながっているか？」をデータで示しましょう。
-- MAGIC
-- MAGIC 💡 **ヒント**: `event_type` の `view` と `purchase` の件数を `channel` 別に集計

-- COMMAND ----------

-- チャレンジ3: ここにクエリを書いてみましょう


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ---
-- MAGIC ## まとめ
-- MAGIC
-- MAGIC 今日やったことを振り返ります。
-- MAGIC
-- MAGIC | ステップ | 学んだこと |
-- MAGIC |---------|-----------|
-- MAGIC | ノートブック基礎 | セル実行、Python/SQL切替、display()、Genie Code |
-- MAGIC | データ取込 | CSV → Raw（Bronze）→ Clean（Silver）のパイプライン |
-- MAGIC | Unity Catalog | テーブルの構造・リレーションの確認 |
-- MAGIC | データ調査 | SQL + 可視化 + AI で問題を発見する手法 |
-- MAGIC
-- MAGIC ### 次回予告（第3回: 自動化 & AI活用）
-- MAGIC
-- MAGIC - 今回手動でやったパイプラインを **Lakeflow Jobs** で自動化
-- MAGIC - **AI/BI ダッシュボード** でリアルタイム可視化
-- MAGIC - **Genie エージェントモード** でAIが自律的に分析
-- MAGIC - **機械学習モデル** で売上予測
