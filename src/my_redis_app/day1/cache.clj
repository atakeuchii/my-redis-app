(ns my-redis-app.day1.cache
  "Day 1: キャッシュ。まず Redis なしで動かし、問題を測る。"
  (:require [clojure.string :as str]
            [my-redis-app.bench :as bench]
            [my-redis-app.db :as db]
            [my-redis-app.redis :as r]))

;; ---------- DB 直撃 ----------

(def db-hits    (atom 0))
(def cache-hits (atom 0))
(def cache-miss (atom 0))

(defn reset-counters! []
  (reset! db-hits 0) (reset! cache-hits 0) (reset! cache-miss 0))

(defn stats []
  {:db @db-hits :hit @cache-hits :miss @cache-miss})

(defn fetch-profile-from-db
  "ユーザー 1 人のプロフィールを DB から引く。"
  [user-id]
  (swap! db-hits inc)
  (db/q1 "SELECT user_id, name, level FROM users WHERE user_id = ?" user-id))

(defn top-user-ids
  "スコア上位 n 人の user_id。ランキング画面の元データ。"
  [n]
  (mapv :user_id
        (db/q "SELECT user_id, max(score) AS best
               FROM plays GROUP BY user_id
               ORDER BY best DESC LIMIT ?" n)))

(defn render-ranking-direct
  "ランキング画面を作る。プロフィールは毎回 DB から引く。"
  [user-ids]
  (mapv (fn [uid]
          (let [p (fetch-profile-from-db uid)]
            {:user_id uid :name (:name p) :level (:level p)}))
        user-ids))

;; ---------- キャッシュ ----------

(def ^:const profile-ttl 60)

(defn- profile-key [user-id] (str "user:" user-id))

(defn- encode-profile
  "プロフィールを 1 つの文字列にする。素朴に区切り文字で。"
  [p]
  (when p (str (:name p) "\u0001" (:level p))))

(defn- decode-profile
  [user-id s]
  (when s
    (let [[nm lv] (str/split s #"\u0001")]
      {:user_id user-id :name nm :level (Long/parseLong lv)})))

(defn fetch-profile-cached
  "cache-aside。キャッシュを見て、無ければ DB から取って入れる。"
  [pool user-id]
  (let [k (profile-key user-id)]
    (if-let [cached (r/cmd pool "GET" k)]
      (do (swap! cache-hits inc)
          (decode-profile user-id cached))
      (do (swap! cache-miss inc)
          (let [p (fetch-profile-from-db user-id)]
            (when p
              (r/cmd pool "SET" k (encode-profile p) "EX" profile-ttl))
            p)))))

(defn render-ranking-cached
  [pool user-ids]
  (mapv (fn [uid]
          (let [p (fetch-profile-cached pool uid)]
            {:user_id uid :name (:name p) :level (:level p)}))
        user-ids))
