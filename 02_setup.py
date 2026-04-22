# Databricks notebook source
# MAGIC %md
# MAGIC # セットアップ — ECサイト サンプルデータ生成
# MAGIC
# MAGIC このノートブックを **最初に1回だけ** 実行してください。
# MAGIC ECサイトの模擬データがあなたのカタログ・スキーマに作成されます。
# MAGIC
# MAGIC **所要時間:** 約2分

# COMMAND ----------

# MAGIC %md
# MAGIC ## 環境設定（ここだけ変更してください）
# MAGIC
# MAGIC 以下のWidgetに **自分のカタログ名とスキーマ名** を入力してください。

# COMMAND ----------

catalog = "workspace"  # ← 自分のカタログ名に変更してください
schema  = "default"    # ← 自分のスキーマ名に変更してください

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_data")

VOLUME_PATH = f"/Volumes/{catalog}/{schema}/raw_data"

print(f"カタログ: {catalog}")
print(f"スキーマ: {schema}")
print(f"Volume:  {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## データ生成（以下は変更不要、そのまま実行してください）

# COMMAND ----------

import random
from datetime import date, timedelta

random.seed(42)

# ── ユーザーマスタ（1,000人）─────────────────────────────────────

LAST_NAMES = ["佐藤","鈴木","高橋","田中","伊藤","渡辺","山本","中村","小林","加藤",
              "吉田","山田","佐々木","松本","井上","木村","林","斉藤","清水","山口"]
FIRST_NAMES_M = ["太郎","次郎","健太","翔太","大輝","拓海","陸","蓮","悠真","大和"]
FIRST_NAMES_F = ["花子","さくら","美咲","陽菜","結衣","凛","芽依","心春","杏","莉子"]
PREFECTURES = ["東京都","大阪府","神奈川県","愛知県","埼玉県","千葉県","福岡県","北海道","兵庫県","京都府"]
GENDERS = ["M", "F"]

users_data = []
for uid in range(1, 1001):
    gender = random.choice(GENDERS)
    name = random.choice(LAST_NAMES) + " " + random.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
    age = random.randint(18, 65)
    pref = random.choice(PREFECTURES)
    reg_date = date(2023, 1, 1) + timedelta(days=random.randint(0, 700))
    email = f"user{uid}@example.com"
    users_data.append((uid, name, age, gender, pref, str(reg_date), email))

FRAUD_UIDS = list(range(991, 999))
for i, uid in enumerate(FRAUD_UIDS):
    name = random.choice(LAST_NAMES) + " " + random.choice(FIRST_NAMES_M)
    users_data[uid - 1] = (uid, name, random.randint(25, 40), "M", "埼玉県", "2024-11-15", f"user{uid}@example.com")

# ── 商品マスタ（80商品）────────────────────────────────────────

CATEGORIES = {
    "電子機器":     [("スマートフォン",60000,35000),("ノートPC",90000,55000),("タブレット",45000,28000),
                     ("イヤホン",12000,6000),("スマートウォッチ",30000,18000),("モニター",35000,22000),
                     ("キーボード",8000,4000),("マウス",5000,2500),("USBハブ",3000,1500),("Webカメラ",7000,3500)],
    "ファッション": [("Tシャツ",3000,1200),("デニムパンツ",6000,2800),("スニーカー",8000,4000),
                     ("ジャケット",12000,6000),("ワンピース",7000,3200),("リュック",5000,2500),
                     ("キャップ",2500,1000),("サングラス",4000,1800),("ベルト",3500,1500),("ストール",4500,2000)],
    "食品":         [("コーヒー豆",1500,700),("チョコレート",800,350),("ナッツ詰合せ",1200,500),
                     ("オリーブオイル",2000,900),("パスタセット",1000,450),("はちみつ",1800,800),
                     ("お茶ギフト",2500,1100),("ドライフルーツ",1400,600),("グラノーラ",900,400),("ジャム",700,300)],
    "家具・インテリア": [("デスク",25000,15000),("チェア",18000,10000),("本棚",15000,9000),
                         ("ラグ",8000,4000),("クッション",3000,1200),("照明スタンド",6000,3000),
                         ("収納ボックス",4000,2000),("カーテン",5000,2500),("ミラー",7000,3500),("時計",4500,2200)],
    "スポーツ":     [("ランニングシューズ",10000,5000),("ヨガマット",3000,1500),("ダンベル",5000,2500),
                     ("プロテイン",4000,1800),("スポーツウェア",6000,3000),("水筒",2000,900),
                     ("サイクルグローブ",2500,1200),("テニスラケット",15000,8000),("バランスボール",3500,1700),("縄跳び",1500,600)],
    "本・メディア": [("ビジネス書",1500,700),("小説",800,350),("技術書",3000,1500),
                     ("漫画セット",4000,2000),("雑誌定期便",1200,600),("洋書",2000,900),
                     ("絵本",1000,450),("参考書",2500,1200),("写真集",3500,1800),("辞書",2800,1400)],
    "美容・健康":   [("化粧水",2500,1000),("日焼け止め",1500,600),("シャンプー",1800,800),
                     ("サプリメント",3000,1200),("フェイスマスク",800,350),("ハンドクリーム",1200,500),
                     ("美容液",4000,1800),("ボディソープ",1000,450),("歯ブラシセット",600,250),("アロマオイル",2000,900)],
    "ホビー・ゲーム": [("ボードゲーム",4000,2000),("プラモデル",3500,1800),("パズル",2000,900),
                       ("トレカパック",500,200),("フィギュア",6000,3000),("画材セット",5000,2500),
                       ("ラジコン",8000,4000),("ミニ四駆",1500,700),("手芸キット",3000,1400),("天体望遠鏡",15000,8000)],
}

products_data = []
pid = 1
for cat, items in CATEGORIES.items():
    for pname, price, cost in items:
        products_data.append((pid, pname, cat, price, cost))
        pid += 1

cat_products = {cat: [(p[0], p[3], p[4]) for p in products_data if p[2] == cat] for cat in CATEGORIES}

# ── 注文・セッションデータ（2024-01 〜 2025-12）─────────────

hokkaido_ids = {uid for uid, name, age, gender, pref, reg, email in users_data if pref == "北海道"}

CHANNELS = ["web", "mobile", "app"]
STATUSES = ["completed", "completed", "completed", "completed", "cancelled", "returned"]

orders_data = []
items_data = []
sessions_data = []
oid = 1
iid = 1
sid = 1

for month_idx in range(24):
    year  = 2024 + month_idx // 12
    month = month_idx % 12 + 1
    days_in_month = 28 if month == 2 else 30 if month in (4,6,9,11) else 31

    for cat in CATEGORIES:
        prods = cat_products[cat]
        n_orders = max(1, int(random.gauss(30, 5)))

        for _ in range(n_orders):
            uid = random.randint(1, 1000)
            channel = random.choice(CHANNELS)

            if uid in hokkaido_ids and month_idx >= 15:
                if random.random() < 0.70:
                    continue

            if channel == "mobile" and month_idx >= 18:
                status = random.choice(["cancelled", "cancelled", "cancelled", "completed"])
            else:
                status = random.choice(STATUSES)

            day = random.randint(1, days_in_month)
            odate = f"{year}-{month:02d}-{day:02d}"

            n_items = random.randint(1, 3)
            total = 0
            for _ in range(n_items):
                ppid, price, cost = random.choice(prods)
                qty = random.randint(1, 2)
                subtotal = price * qty
                total += subtotal
                items_data.append((iid, oid, ppid, qty, price, subtotal))
                iid += 1

            orders_data.append((oid, uid, odate, status, total, channel, cat))
            oid += 1

    for cat in CATEGORIES:
        n_sessions = max(1, int(random.gauss(50, 10)))
        for _ in range(n_sessions):
            uid = random.randint(1, 1000)
            ppid = random.choice(cat_products[cat])[0]
            day = random.randint(1, days_in_month)
            sdate = f"{year}-{month:02d}-{day:02d}"

            channel = random.choice(CHANNELS)
            if channel == "mobile" and month_idx >= 18:
                evt = random.choices(["view","add_to_cart","abandon","abandon"],
                                     weights=[30,20,40,10])[0]
            else:
                evt = random.choices(["view","add_to_cart","purchase","abandon"],
                                     weights=[40,25,20,15])[0]

            sessions_data.append((sid, uid, ppid, cat, evt, sdate, channel))
            sid += 1

high_value_pids = [p[0] for p in cat_products["電子機器"] if p[1] >= 30000]

for month_idx in range(12, 24):  # 2025-01 〜 2025-12
    year = 2025
    month = month_idx % 12 + 1
    days_in_month = 28 if month == 2 else 30 if month in (4,6,9,11) else 31

    for fraud_uid in FRAUD_UIDS:
        n_bot_sessions = random.randint(40, 60)
        for _ in range(n_bot_sessions):
            day = random.randint(1, days_in_month)
            sdate = f"{year}-{month:02d}-{day:02d}"
            ppid = random.choice(high_value_pids)
            evt = random.choice(["add_to_cart", "add_to_cart", "add_to_cart", "abandon"])
            sessions_data.append((sid, fraud_uid, ppid, "電子機器", evt, sdate, "web"))
            sid += 1

print(f"生成完了: users={len(users_data)}, products={len(products_data)}, orders={len(orders_data)}, items={len(items_data)}, sessions={len(sessions_data)}")

# COMMAND ----------

import csv, io

def write_csv_to_volume(volume_path, filename, header, rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(header)
    w.writerows(rows)
    content = buf.getvalue()
    path = f"{volume_path}/{filename}"
    dbutils.fs.put(path, content, overwrite=True)
    print(f"  {filename}: {len(rows):,} rows")

write_csv_to_volume(VOLUME_PATH, "users.csv",
    ["user_id","name","age","gender","prefecture","registration_date","email"], users_data)
write_csv_to_volume(VOLUME_PATH, "products.csv",
    ["product_id","product_name","category","price","cost"], products_data)
write_csv_to_volume(VOLUME_PATH, "orders.csv",
    ["order_id","user_id","order_date","status","total_amount","channel","category"], orders_data)
write_csv_to_volume(VOLUME_PATH, "order_items.csv",
    ["item_id","order_id","product_id","quantity","unit_price","subtotal"], items_data)
write_csv_to_volume(VOLUME_PATH, "sessions.csv",
    ["session_id","user_id","product_id","category","event_type","session_date","channel"], sessions_data)

print(f"\n✅ CSVファイルをVolumeに書き込み完了: {VOLUME_PATH}")
for f in dbutils.fs.ls(VOLUME_PATH):
    print(f"  {f.name} ({f.size:,} bytes)")
