# my-redis-app

Redis 実践編。自作した [my-redis](../my-redis) と本物の Redis を「使う側」から比較しながら、
キャッシュ・原子性・ランキング・分散ロック・運用の 5 テーマを扱う。

題材は架空のゲーム（ユーザー 10,000 / プレイ記録 100,000 / 限定アイテム 20）。

---

## 必要なもの

- JDK 21
- Leiningen
- Docker / Docker Compose
- `redis-cli`（確認用。無くても動く）

---

## 準備

### 1. my-redis をローカルリポジトリに入れる

```bash
cd ../my-redis
lein install
```

`my-redis-app` は `my-redis.resp` を Redis クライアントとして流用しているため、先にこれが必要。

### 2. PostgreSQL を起動

```bash
cd ../my-redis-app
docker compose up -d
docker compose ps        # healthy になるまで待つ
```

ホスト側は **5437** 番にマッピングしている。

### 3. データの生成と投入

```bash
lein run -m my-redis-app.gen-data   # data/*.csv を生成
lein run -m my-redis-app.db         # スキーマ作成 + 投入（十数秒かかる）
```

### 4. Redis を 2 つ起動

```bash
# 本物（参照用）
docker run --rm -d -p 6379:6379 --name redis-real redis

# 自作（別ターミナル）
cd ../my-redis && lein run
```

`lein run` は 6380 番で起動する。ポートを変えるなら `lein run 6390`。

### 5. 接続確認

```bash
redis-cli -p 6379 PING      # PONG
redis-cli -p 6380 PING      # PONG
```

```clojure
;; REPL
(require '[my-redis-app.db :as db] '[my-redis-app.redis :as r])

(db/q1 "SELECT count(*) AS n FROM users")   ;; => {:n 10000}

(def mine (r/pool 6380))
(def real (r/pool 6379))
(r/cmd mine "PING")                          ;; => "PONG"
```

---

## 後片付け

```bash
docker compose down          # コンテナを止める（データは残る）
docker compose down -v       # データごと消す
docker stop redis-real
```

自作 Redis は `Ctrl-C` で止める。

---

## 構成

```
data/                    生成した CSV
sql/schema.sql           テーブル定義
src/my_redis_app/
  gen_data.clj           CSV の生成
  db.clj                 PostgreSQL への接続・投入・クエリ
  redis.clj              最小限の Redis クライアント（接続プール付き）
  bench.clj              計測用のヘルパ
docker-compose.yml       PostgreSQL（ホスト側 5437）
```

### Redis クライアントについて

carmine などのライブラリを使わず、自作の `my-redis.resp` を流用している。

- 何が起きているかを隠さない
- **パイプライニングを自分で実装して効果を測る**（Day 5）ため。ライブラリは勝手にやってしまう

```clojure
(r/cmd mine "SET" "k" "v")     ; エラー応答は例外になる
(r/cmd* mine "NOSUCHCMD")      ; エラー応答をそのまま返す（文面を見たいとき）
```

---

## データ

### users（10,000 行）

| 列 | 内容 |
|---|---|
| user_id | `u00000` 〜 |
| name | 日本語の氏名 |
| level | 1〜60 |
| registered_at | 2025 年のどこか |

### plays（100,000 行）

| 列 | 内容 |
|---|---|
| play_id | `p0000000` 〜 |
| user_id | users への参照 |
| score | ユーザーごとの実力（レベル依存 + 個人差）に応じて対数正規に分布 |
| played_at | 2026 年 9 月の 1 ヶ月間 |

**3 割のプレイ記録を上位 1,000 人に偏らせている。** よく遊ぶ人が何十回も、たまにしか来ない人が 1〜2 回、という分布。
スコアに上限を設けていないので、最大値を取っても天井に張り付かず、上位ほど間隔が広がる。

### items（20 行）

| 列 | 内容 |
|---|---|
| item_id | `i000` 〜 |
| name | 「伝説の剣」など |
| stock | 5〜50（**少ない**。在庫の奪い合いを起こすため） |
| price | 100〜5000 |

---

## 各日の進め方

**Redis なしの実装を動かして問題を測る → どう改善するか考える → Redis で解く**

| Day | テーマ | Redis なしの比較対象 |
|---|---|---|
| 1 | キャッシュ | DB に毎回問い合わせる |
| 2 | 原子性が壊れる場所 | アプリ側で read-modify-write |
| 3 | ランキング | SQL の `ORDER BY` + 行番号 |
| 4 | 分散ロック | Day 2 の「原子的コマンドで解く」版 |
| 5 | 運用 | — |

詳細は [curriculum.md](./docs/curriculum.md) を参照。
