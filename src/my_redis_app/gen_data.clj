(ns my-redis-app.gen-data
  "題材データの生成。架空のゲームのユーザー・プレイ記録・限定アイテム。"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import [java.time Instant Duration]))

(def ^:private family-names
  ["佐藤" "鈴木" "高橋" "田中" "伊藤" "渡辺" "山本" "中村" "小林" "加藤"])

(def ^:private given-names
  ["太郎" "花子" "一郎" "美咲" "健太" "さくら" "大輔" "愛" "翔" "結衣"])

(def ^:private rng (java.util.Random.))

(defn- rand-gaussian ^double [] (.nextGaussian rng))

(defn- random-name [i]
  (str (rand-nth family-names) (rand-nth given-names) (inc (mod i 999))))

(defn- iso [^Instant t] (str t))

(defn gen-users
  [n]
  (let [base (Instant/parse "2025-01-01T00:00:00Z")]
    (for [i (range n)]
      (let [level (inc (rand-int 60))]
        {:user_id       (format "u%05d" i)
         :name          (random-name i)
         :level         level
         :registered_at (iso (.plus base (Duration/ofHours (rand-int 15000))))}))))

(defn gen-plays
  "プレイ記録。ユーザーごとに実力（基準スコア）を持たせ、その周りにばらつかせる。
   上限を設けないので、最大値を取っても天井に張り付かない。"
  [n users]
  (let [base   (Instant/parse "2026-09-01T00:00:00Z")
        ;; レベルに応じた実力 + 個人差。対数正規分布で上位ほど疎にする
        skill  (into {} (map (fn [{:keys [user_id level]}]
                               [user_id (* (+ 200 (* 30 level))
                                           (Math/exp (* 0.5 (rand-gaussian))))]))
                     users)
        ids    (mapv :user_id users)
        heavy  (vec (take (quot (count ids) 10) (shuffle ids)))]
    (for [i (range n)]
      (let [uid (if (< (rand) 0.3) (rand-nth heavy) (rand-nth ids))
            s   (get skill uid)
            ;; 実力の 0.3〜1.3 倍。たまに大当たりが出る
            v   (* s (+ 0.3 (rand)) (if (< (rand) 0.02) (+ 1.5 (rand)) 1.0))]
        {:play_id   (format "p%07d" i)
         :user_id   uid
         :score     (max 10 (int v))
         :played_at (iso (.plus base (Duration/ofMinutes (rand-int 43200))))}))))

(defn gen-items
  []
  (let [names ["伝説の剣" "英雄の盾" "賢者の杖" "疾風のブーツ" "炎の護符"
               "氷結のローブ" "雷鳴の弓" "聖なる鎧" "影の短剣" "星屑の指輪"
               "竜鱗の兜" "不滅の腕輪" "虹色の宝玉" "嵐のマント" "月光の鈴"
               "大地の原石" "天空の羽根" "深淵の鍵" "黄金の杯" "時渡りの砂時計"]]
    (map-indexed (fn [i nm]
                   {:item_id (format "i%03d" i)
                    :name    nm
                    :stock   (+ 5 (rand-int 46))      ; 5〜50
                    :price   (* 100 (+ 1 (rand-int 50)))})
                 names)))

(defn- write-csv!
  [path cols rows]
  (io/make-parents path)
  (with-open [w (io/writer path)]
    (csv/write-csv w (cons (map name cols)
                           (map (fn [r] (map #(str (get r %)) cols)) rows))))
  (println (format "%s: %d 行" path (count rows))))

(defn -main [& _]
  (let [users (vec (gen-users 10000))
        plays (vec (gen-plays 100000 users))
        items (vec (gen-items))]
    (write-csv! "data/users.csv" [:user_id :name :level :registered_at] users)
    (write-csv! "data/plays.csv" [:play_id :user_id :score :played_at] plays)
    (write-csv! "data/items.csv" [:item_id :name :stock :price] items)))
