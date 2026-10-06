(ns my-redis-app.day3.ranking
  (:require [my-redis-app.bench :as bench]
            [my-redis-app.db :as db]
            [my-redis-app.redis :as r])
  (:import [java.time LocalDate]
           [java.time.format DateTimeFormatter]
           [java.time.temporal WeekFields IsoFields]))

(defn top-n-sql
  "上位 n 人。"
  [n]
  (db/q "SELECT p.user_id, u.name, max(p.score) AS best
         FROM plays p JOIN users u USING (user_id)
         GROUP BY p.user_id, u.name
         ORDER BY best DESC LIMIT ?" n))

(defn rank-of-sql
  "user-id の順位（1 始まり）。自分より上の人数 + 1。"
  [user-id]
  (:rank
   (db/q1 "WITH bests AS (
             SELECT user_id, max(score) AS best FROM plays GROUP BY user_id
           )
           SELECT count(*) + 1 AS rank
           FROM bests
           WHERE best > (SELECT best FROM bests WHERE user_id = ?)"
          user-id)))

(defn around-me-sql
  "自分の前後 n 人。"
  [user-id n]
  (db/q "WITH bests AS (
           SELECT user_id, max(score) AS best FROM plays GROUP BY user_id
         ),
         ranked AS (
           SELECT user_id, best, row_number() OVER (ORDER BY best DESC) AS rnk
           FROM bests
         ),
         me AS (SELECT rnk FROM ranked WHERE user_id = ?)
         SELECT r.user_id, u.name, r.best, r.rnk
         FROM ranked r JOIN users u USING (user_id), me
         WHERE r.rnk BETWEEN me.rnk - ? AND me.rnk + ?
         ORDER BY r.rnk" user-id n n))

;; ---------- ZSet 版 ----------

(def ^:const ranking-key "ranking:best")

(defn load-ranking!
  "DB から集約してランキングを ZSet に載せる。初期化・再構築用。"
  [pool]
  (r/cmd pool "DEL" ranking-key)
  (let [rows (db/q "SELECT user_id, max(score) AS best FROM plays GROUP BY user_id")]
    ;; ZADD は可変長なので、まとめて送る（1000 件ずつ）
    (doseq [chunk (partition-all 1000 rows)]
      (apply r/cmd pool "ZADD" ranking-key
             (mapcat (fn [{:keys [user_id best]}] [best user_id]) chunk)))
    (count rows)))

(defn top-n-zset
  "上位 n 人。ZSet は昇順なので ZREVRANGE を使う。"
  [pool n]
  (let [flat (r/cmd pool "ZREVRANGE" ranking-key 0 (dec n) "WITHSCORES")]
    (mapv (fn [[uid score]] {:user_id uid :best (Long/parseLong score)})
          (partition 2 flat))))

(defn rank-of-zset
  "user-id の順位（1 始まり）。"
  [pool user-id]
  (when-let [r (r/cmd pool "ZREVRANK" ranking-key user-id)]
    (inc r)))

(defn around-me-zset
  "自分の前後 n 人。"
  [pool user-id n]
  (when-let [rank (r/cmd pool "ZREVRANK" ranking-key user-id)]
    (let [from (max 0 (- rank n))
          to   (+ rank n)
          flat (r/cmd pool "ZREVRANGE" ranking-key from to "WITHSCORES")]
      (mapv (fn [i [uid score]]
              {:rank (inc (+ from i)) :user_id uid :best (Long/parseLong score)})
            (range)
            (partition 2 flat)))))

;; ---------- キー設計 ----------
;;
;; ranking:alltime              累計。TTL なし
;; ranking:weekly:2026-W40      週間。TTL 60 日
;; ranking:daily:2026-10-04     日別。TTL 7 日
;;
;; 包含関係 日別 ⊆ 週間 ⊆ 累計 を利用し、狭い方から判定して往復を減らす。

(def ^:const daily-ttl   (* 7 24 3600))
(def ^:const weekly-ttl  (* 60 24 3600))

(defn daily-key [^LocalDate d]
  (str "ranking:daily:" (.format d DateTimeFormatter/ISO_LOCAL_DATE)))

(defn weekly-key [^LocalDate d]
  (format "ranking:weekly:%d-W%02d"
          (.get d IsoFields/WEEK_BASED_YEAR)
          (.get d IsoFields/WEEK_OF_WEEK_BASED_YEAR)))

(def ^:const alltime-key "ranking:alltime")

;; ---------- スコアの記録 ----------

(def ^:private round-trips (atom 0))

(defn reset-trips! [] (reset! round-trips 0))
(defn trips [] @round-trips)

(defn- cmd! [pool & args]
  (swap! round-trips inc)
  (apply r/cmd pool args))

(defn- best-score
  "ZSet から user の現在のベストを取る。無ければ nil。"
  [pool key user-id]
  (when-let [s (cmd! pool "ZSCORE" key user-id)]
    (Long/parseLong s)))

(defn record-score!
  "プレイ結果を記録する。
   日別 → 週間 → 累計 の順に判定し、超えなければそこで打ち切る。
   更新した階層のベクタを返す。"
  [pool user-id score ^LocalDate date]
  (let [dk (daily-key date)
        wk (weekly-key date)]
    (if-let [db-best (best-score pool dk user-id)]
      (if (<= score db-best)
        []                                              ; 今日のベストを超えない → 終了
        (do
          (cmd! pool "ZADD" dk score user-id)
          (cmd! pool "EXPIRE" dk daily-ttl)
          (if-let [wb (best-score pool wk user-id)]
            (if (<= score wb)
              [:daily]                                  ; 週間は超えない
              (do
                (cmd! pool "ZADD" wk score user-id)
                (cmd! pool "EXPIRE" wk weekly-ttl)
                (if-let [ab (best-score pool alltime-key user-id)]
                  (if (<= score ab)
                    [:daily :weekly]
                    (do (cmd! pool "ZADD" alltime-key score user-id)
                        [:daily :weekly :alltime]))
                  (do (cmd! pool "ZADD" alltime-key score user-id)
                      [:daily :weekly :alltime]))))
            ;; 週間に記録が無い（今週初プレイ）
            (do
              (cmd! pool "ZADD" wk score user-id)
              (cmd! pool "EXPIRE" wk weekly-ttl)
              (if-let [ab (best-score pool alltime-key user-id)]
                (if (<= score ab)
                  [:daily :weekly]
                  (do (cmd! pool "ZADD" alltime-key score user-id)
                      [:daily :weekly :alltime]))
                (do (cmd! pool "ZADD" alltime-key score user-id)
                    [:daily :weekly :alltime]))))))
      ;; 日別に記録が無い（今日初プレイ）→ 全階層を判定する
      (do
        (cmd! pool "ZADD" dk score user-id)
        (cmd! pool "EXPIRE" dk daily-ttl)
        (let [wb (best-score pool wk user-id)]
          (if (and wb (<= score wb))
            [:daily]
            (do
              (cmd! pool "ZADD" wk score user-id)
              (cmd! pool "EXPIRE" wk weekly-ttl)
              (let [ab (best-score pool alltime-key user-id)]
                (if (and ab (<= score ab))
                  [:daily :weekly]
                  (do (cmd! pool "ZADD" alltime-key score user-id)
                      [:daily :weekly :alltime]))))))))))

;; ---------- ランキングの取得 ----------

(defn- fetch-top
  [pool key n]
  (mapv (fn [i [uid s]] {:rank (inc i) :user_id uid :score (Long/parseLong s)})
        (range)
        (partition 2 (r/cmd pool "ZREVRANGE" key 0 (dec n) "WITHSCORES"))))

(defn- fetch-rank
  [pool key user-id]
  (when-let [r (r/cmd pool "ZREVRANK" key user-id)]
    (inc r)))

(defn- fetch-around
  [pool key user-id n]
  (when-let [rank (r/cmd pool "ZREVRANK" key user-id)]
    (let [from (max 0 (- rank n))
          to   (+ rank n)]
      (mapv (fn [i [uid s]] {:rank (inc (+ from i)) :user_id uid :score (Long/parseLong s)})
            (range)
            (partition 2 (r/cmd pool "ZREVRANGE" key from to "WITHSCORES"))))))

(defn choose-ranking-key
  "期間の指定からキーを決める。"
  [period ^LocalDate date]
  (case period
    :daily   (daily-key date)
    :weekly  (weekly-key date)
    :alltime alltime-key))

;; ---------- 初期構築 ----------

(defn rebuild-all!
  "plays テーブルから全期間のランキングを構築する。"
  [pool]
  ;; 既存のランキングキーを消す
  (doseq [k (r/cmd pool "KEYS" "ranking:*")]
    (r/cmd pool "DEL" k))

  ;; 累計
  (let [rows (db/q "SELECT user_id, max(score) AS best FROM plays GROUP BY user_id")]
    (doseq [chunk (partition-all 1000 rows)]
      (apply r/cmd pool "ZADD" alltime-key
             (mapcat (fn [{:keys [user_id best]}] [best user_id]) chunk))))

  ;; 日別
  (let [rows (db/q "SELECT user_id, played_at::date AS d, max(score) AS best
                    FROM plays GROUP BY user_id, played_at::date")]
    (doseq [[d group] (group-by :d rows)]
      (let [k (daily-key (.toLocalDate ^java.sql.Date d))]
        (doseq [chunk (partition-all 1000 group)]
          (apply r/cmd pool "ZADD" k
                 (mapcat (fn [{:keys [user_id best]}] [best user_id]) chunk)))
        ;; 過去のデータなので TTL は付けない（検証用）
        )))

  ;; 週間
  (let [rows (db/q "SELECT user_id,
                           to_char(played_at, 'IYYY-\"W\"IW') AS wk,
                           max(score) AS best
                    FROM plays GROUP BY user_id, to_char(played_at, 'IYYY-\"W\"IW')")]
    (doseq [[wk group] (group-by :wk rows)]
      (let [k (str "ranking:weekly:" wk)]
        (doseq [chunk (partition-all 1000 group)]
          (apply r/cmd pool "ZADD" k
                 (mapcat (fn [{:keys [user_id best]}] [best user_id]) chunk))))))

  {:keys (count (r/cmd pool "KEYS" "ranking:*"))})

(defn top-n     [pool period date n]      (fetch-top pool (choose-ranking-key period date) n))
(defn my-rank   [pool period date uid]    (fetch-rank pool (choose-ranking-key period date) uid))
(defn around-me [pool period date uid n]  (fetch-around pool (choose-ranking-key period date) uid n))

(defn record-score-naive!
  "判定せず、3 つとも無条件に ZADD する。
   ベストでなくなるが、往復は常に 3 回（EXPIRE 込みで 5 回）。"
  [pool user-id score ^LocalDate date]
  (cmd! pool "ZADD" (daily-key date) score user-id)
  (cmd! pool "ZADD" (weekly-key date) score user-id)
  (cmd! pool "ZADD" alltime-key score user-id)
  [:daily :weekly :alltime])

(comment
  (require '[my-redis-app.day3.ranking :as d3] '[my-redis-app.bench :as bench]
           '[my-redis-app.db :as db] '[my-redis-app.redis :as r] :reload)
  (import '[java.time LocalDate])

  (def mine (r/pool 6380))

  ;; 適当なユーザーを選ぶ（上位・中位・下位）
  (def sample-users
    (mapv :user_id
          (db/q "WITH bests AS (SELECT user_id, max(score) AS best FROM plays GROUP BY user_id),
                      ranked AS (SELECT user_id, row_number() OVER (ORDER BY best DESC) AS rnk FROM bests)
                 SELECT user_id FROM ranked WHERE rnk IN (1, 10, 100, 1000, 5000, 9000)")))

  sample-users

  (bench/report "上位 20 人（SQL）"    (bench/timed #(d3/top-n-sql 20) 30))
  (bench/report "順位を引く（SQL）"    (bench/timed #(d3/rank-of-sql (rand-nth sample-users)) 30))
  (bench/report "前後 5 人（SQL）"     (bench/timed #(d3/around-me-sql (rand-nth sample-users) 5) 30))

  
  (time (d3/rebuild-all! mine))
  
  ;; どんなキーができたか
  (sort (r/cmd mine "KEYS" "ranking:*"))
  )