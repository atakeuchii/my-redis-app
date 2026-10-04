(ns my-redis-app.db
  "PostgreSQL への接続とクエリ。"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as rs]))

(def db-spec
  {:dbtype "postgresql"
   :host   "localhost"
   :port   5437
   :dbname "game"
   :user   "postgres"
   :password "postgres"})

(def ds (jdbc/get-datasource db-spec))

(def opts {:builder-fn rs/as-unqualified-lower-maps})

(defn q
  "クエリを実行して行のベクタを返す。"
  [sql & params]
  (jdbc/execute! ds (into [sql] params) opts))

(defn q1
  "1 行だけ返す。"
  [sql & params]
  (first (apply q sql params)))

;; ---------- 投入 ----------

(defn- read-csv
  "CSV をマップのシーケンスとして読む。"
  [path]
  (with-open [r (io/reader path)]
    (let [[header & rows] (csv/read-csv r)
          ks (mapv keyword header)]
      (doall (map #(zipmap ks %) rows)))))

(defn- exec-script! [path]
  (jdbc/execute! ds [(slurp path)]))

(defn load-all!
  "スキーマを作り直して CSV を投入する。"
  []
  (exec-script! "sql/schema.sql")
  (println "schema created")

  (let [users (read-csv "data/users.csv")]
    (jdbc/execute-batch! ds
                         "INSERT INTO users (user_id, name, level, registered_at)
                          VALUES (?, ?, ?::int, ?::timestamptz)"
                         (mapv (juxt :user_id :name :level :registered_at) users)
                         {:batch-size 1000})
    (println "users:" (count users)))

  (let [plays (read-csv "data/plays.csv")]
    (jdbc/execute-batch! ds
                         "INSERT INTO plays (play_id, user_id, score, played_at)
                          VALUES (?, ?, ?::int, ?::timestamptz)"
                         (mapv (juxt :play_id :user_id :score :played_at) plays)
                         {:batch-size 1000})
    (println "plays:" (count plays)))

  (let [items (read-csv "data/items.csv")]
    (jdbc/execute-batch! ds
                         "INSERT INTO items (item_id, name, stock, price)
                          VALUES (?, ?, ?::int, ?::int)"
                         (mapv (juxt :item_id :name :stock :price) items)
                         {:batch-size 100})
    (println "items:" (count items)))

  (jdbc/execute! ds ["ANALYZE"])
  (println "done"))

(defn -main [& _] (load-all!))
