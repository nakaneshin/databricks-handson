# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks ノートブックの基礎
# MAGIC
# MAGIC ノートブックは、Databricks上でコードを対話的に開発および実行するための主要な手段です。
# MAGIC このレッスンでは、Databricks ノートブックの基本的な使い方を紹介します。
# MAGIC
# MAGIC ## 学習目標
# MAGIC
# MAGIC このレッスンの終わりまでに、次のことができるようになります:
# MAGIC * ノートブックのセルを実行する
# MAGIC * ノートブックの言語を設定する
# MAGIC * マジックコマンドを使用する
# MAGIC * SQLセルとPythonセルを作成して実行する
# MAGIC * マークダウンセルを作成する
# MAGIC * データを可視化する
# MAGIC * Genie Code（AIアシスタント）を使う

# COMMAND ----------

# MAGIC %md
# MAGIC ## ノートブックのインタフェース
# MAGIC
# MAGIC ノートブックを開くと以下のような画面が表示されます。
# MAGIC
# MAGIC - **ノートブック名**: クリックで変更できます
# MAGIC - **デフォルト言語**: Python、SQLから選択（Free EditionではR、Scalaは利用不可）
# MAGIC - **コンピュートセレクター**: サーバレスコンピュートが自動で接続されます
# MAGIC - **Genie Code**: AIアシスタントを呼び出します（ランプアイコン）
# MAGIC - **セル**: コードやマークダウンを記述するための箱です
# MAGIC - **セルの実行**: 選択しているセル、あるいは前後のセルを実行します

# COMMAND ----------

# MAGIC %md
# MAGIC ## コンピュートへの接続
# MAGIC
# MAGIC Free Edition ではサーバレスコンピュートが利用されます。
# MAGIC ノートブックを開くと自動的に接続されますが、初回は数秒〜数十秒かかる場合があります。
# MAGIC
# MAGIC 画面右上のコンピュートセレクターに **「接続済み」** と表示されれば準備完了です。

# COMMAND ----------

# MAGIC %md
# MAGIC ## セルの実行
# MAGIC
# MAGIC 以下のいずれかの方法で、下のセルを実行してみましょう:
# MAGIC
# MAGIC * **Ctrl+Enter**（Mac: Cmd+Enter）: セルを実行
# MAGIC * **Shift+Enter**（Mac: Shift+Enter）: セルを実行して次のセルに移動
# MAGIC * セル左の **▶** ボタンをクリック

# COMMAND ----------

print("私はPythonを実行しています！🎉")

# COMMAND ----------

# MAGIC %md
# MAGIC **注意**: セルは上から順に実行してください。前のセルを飛ばすとエラーになることがあります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## デフォルト言語の設定
# MAGIC
# MAGIC - このノートブックのデフォルト言語は **Python** です
# MAGIC - ノートブックタイトルの右にある **Python** をクリックすると変更できます
# MAGIC - ただし、マジックコマンドを使えばセル単位で言語を切り替えられるので、通常は変更不要です

# COMMAND ----------

# MAGIC %md
# MAGIC ## マジックコマンド
# MAGIC
# MAGIC セルの先頭に `%` を付けると、**マジックコマンド**を使用できます。Databricks ノートブック特有の機能です。
# MAGIC
# MAGIC | コマンド | 説明 |
# MAGIC |---------|------|
# MAGIC | `%python` | Pythonコードを実行 |
# MAGIC | `%sql` | SQLクエリを実行 |
# MAGIC | `%md` | マークダウンを表示 |
# MAGIC | `%sh` | シェルコマンドを実行 |
# MAGIC
# MAGIC **ルール**:
# MAGIC - マジックコマンドはセルの **最初の行** に書く
# MAGIC - 1セルに1つのマジックコマンドのみ

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQLセルを試す
# MAGIC
# MAGIC 下のセルには `%sql` マジックコマンドが付いています。そのまま実行してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "こんにちはSQL！" AS greeting, current_date() AS today

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pythonセルを試す
# MAGIC
# MAGIC デフォルト言語がPythonの場合、`%python` は省略できます。

# COMMAND ----------

message = "こんにちはPython！"
today = "2026-04-23"
print(f"{message} 今日は {today} です。")

# COMMAND ----------

# MAGIC %md
# MAGIC ### やってみよう 💪
# MAGIC
# MAGIC 下に新しいセルを追加して（セルの境界にカーソルを合わせると「+ Code」が表示されます）、
# MAGIC 以下のSQLを実行してみてください。
# MAGIC
# MAGIC ```sql
# MAGIC %sql
# MAGIC SELECT 1 + 1 AS answer
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## マークダウン
# MAGIC
# MAGIC `%md` マジックコマンドを使うと、セル内でマークダウンをレンダリングできます。
# MAGIC このセル自体がマークダウンです！ダブルクリックすると編集できます。
# MAGIC
# MAGIC ### マークダウンの例
# MAGIC
# MAGIC **太字** と *斜体* のテキスト
# MAGIC
# MAGIC 順序付きリスト:
# MAGIC 1. Unity Catalog
# MAGIC 1. Delta Lake
# MAGIC 1. Lakeflow
# MAGIC
# MAGIC 順序なしリスト:
# MAGIC * ノートブック
# MAGIC * ダッシュボード
# MAGIC * Genie
# MAGIC
# MAGIC | 機能 | 説明 |
# MAGIC |------|------|
# MAGIC | display() | データを表形式やグラフで表示 |
# MAGIC | %sql | SQLクエリを実行 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## display() — データの表示と可視化
# MAGIC
# MAGIC `display()` はDatabricks独自の関数で、データを表形式で見やすく表示します。
# MAGIC さらに、ノーコードでグラフも作成できます。
# MAGIC
# MAGIC まずはサンプルデータを表示してみましょう。

# COMMAND ----------

df = spark.table("samples.nyctaxi.trips")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの可視化
# MAGIC
# MAGIC 上のセルの結果テーブルの右にある **＋ > 可視化** をクリックして、グラフを作ってみましょう。
# MAGIC
# MAGIC 例:
# MAGIC - **可視化タイプ**: 散布図
# MAGIC - **X軸**: `trip_distance`（走行距離）
# MAGIC - **Y軸**: `fare_amount`（運賃）
# MAGIC
# MAGIC 距離が長いほど運賃が高くなる傾向が見えるはずです。

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQLでも同じことができます
# MAGIC
# MAGIC `%sql` でクエリを書いても、同様に結果テーブルとグラフが表示されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT trip_distance, fare_amount
# MAGIC FROM samples.nyctaxi.trips
# MAGIC WHERE trip_distance > 0 AND fare_amount > 0
# MAGIC LIMIT 500

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Code（AIアシスタント）
# MAGIC
# MAGIC Databricks には **Genie Code** というAIアシスタントが組み込まれています。
# MAGIC コードの生成、エラーの修正、データの分析など、様々なシーンで支援してくれます。
# MAGIC
# MAGIC ### 使い方
# MAGIC 1. セル右上のランプアイコンをクリック
# MAGIC 2. プロンプトを入力（例: 「pickup_zip別のトリップ数を集計して」）
# MAGIC 3. 生成されたコードを確認して **Accept**

# COMMAND ----------

# MAGIC %md
# MAGIC ### やってみよう 💪
# MAGIC
# MAGIC 下に新しいセルを追加し、Genie Code を使って以下のコードを生成してみてください:
# MAGIC
# MAGIC **プロンプト例**: 「samples.nyctaxi.trips から、月別のトリップ数を集計して表示して」
# MAGIC
# MAGIC 生成されたコードを実行し、結果テーブルから **＋ > 可視化** で棒グラフにしてみましょう。

# COMMAND ----------

# MAGIC %md
# MAGIC ### エラーの自動修正
# MAGIC
# MAGIC 下のセルにはわざとエラーがあります。実行してエラーを確認した後、
# MAGIC **「診断エラー」** ボタンをクリックしてGenie Codeに修正してもらいましょう。

# COMMAND ----------

# わざとエラーを入れています。実行して「診断エラー」を試しましょう。
df = spark.table("samples.nyctaxi.trips").select(
    "tpep_pickup_date",    # ← このカラム名が間違っています
    "trip_distance",
    "fare_amount"
)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog でデータを確認する
# MAGIC
# MAGIC 左サイドバーの **カタログ** をクリックすると、Unity Catalog エクスプローラーが開きます。
# MAGIC
# MAGIC 試しに以下を辿ってみてください:
# MAGIC - `samples` > `nyctaxi` > `trips` をクリック
# MAGIC - **サンプルデータ** タブでデータをプレビュー
# MAGIC - **スキーマ** タブでカラム定義を確認
# MAGIC
# MAGIC Unity Catalog は、テーブル・ファイル・モデルを一元管理するDatabricksのガバナンス機能です。
# MAGIC この後のハンズオンで、自分のカタログ・テーブルを作成します。

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ
# MAGIC
# MAGIC | 学んだこと | ポイント |
# MAGIC |-----------|---------|
# MAGIC | セルの実行 | Ctrl+Enter / Shift+Enter |
# MAGIC | 言語の切替 | `%python`, `%sql`, `%md` |
# MAGIC | データ表示 | `display()` で表＋グラフ |
# MAGIC | AI活用 | Genie Code で生成・修正・分析 |
# MAGIC | データ管理 | Unity Catalog でテーブルを一元管理 |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **次のノートブック** では、ECサイトのデータを使って実際にデータパイプラインを構築します！
