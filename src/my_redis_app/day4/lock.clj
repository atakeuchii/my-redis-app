(ns my-redis-app.day4.lock
  "Day 4: 分散ロック。Day 2 の在庫問題を、外部リソースを含む形で解き直す。"
  (:require [my-redis-app.bench :as bench]
            [my-redis-app.db :as db]
            [my-redis-app.redis :as r]
            [next.jdbc :as jdbc]))

;; ---------- 準備 ----------

(defn reset-all! [pool item-id stock]
  (r/cmd pool "SET" (str "stock:" item-id) stock)
  (db/q "DELETE FROM purchases WHERE item_id = ?" item-id)
  (r/cmd pool "DEL" (str "lock:" item-id)))

(defn stock-of [pool item-id]
  (Long/parseLong (r/cmd pool "GET" (str "stock:" item-id))))

(defn purchase-count [item-id]
  (:n (db/q1 "SELECT count(*) AS n FROM purchases WHERE item_id = ?" item-id)))

;; ---------- ロックなし ----------

(defn buy-no-lock!
  "在庫を確認し、減らし、購入履歴を DB に記録する。
   Redis と DB の 2 つのリソースをまたぐため、DECR だけでは守れない。"
  [pool user-id item-id]
  (let [k (str "stock:" item-id)
        current (Long/parseLong (r/cmd pool "GET" k))]
    (if (pos? current)
      (do
        (r/cmd pool "SET" k (dec current))
        ;; 外部リソースへの書き込み。ここが重く、時間がかかる
        (db/q "INSERT INTO purchases (purchase_id, user_id, item_id)
               VALUES (?, ?, ?)"
              (str (random-uuid)) user-id item-id)
        :ok)
      :sold-out)))

(defn buy-decr!
  "在庫は DECR で原子的に減らす。履歴は別途記録する。"
  [pool user-id item-id]
  (let [k (str "stock:" item-id)
        remaining (r/cmd pool "DECR" k)]
    (if (>= remaining 0)
      (do
        (db/q "INSERT INTO purchases (purchase_id, user_id, item_id)
               VALUES (?, ?, ?)"
              (str (random-uuid)) user-id item-id)
        :ok)
      (do (r/cmd pool "INCR" k) :sold-out))))

(defn buy-decr-crash!
  "DECR の後、INSERT の前にクラッシュする版。"
  [pool user-id item-id]
  (let [k (str "stock:" item-id)
        remaining (r/cmd pool "DECR" k)]
    (if (>= remaining 0)
      (do
        (when (< (rand) 0.3)
          (throw (ex-info "INSERT の前にクラッシュ" {})))   ; 在庫は減ったが履歴が無い
        (db/q "INSERT INTO purchases (purchase_id, user_id, item_id)
               VALUES (?, ?, ?)"
              (str (random-uuid)) user-id item-id)
        :ok)
      (do (r/cmd pool "INCR" k) :sold-out))))

(defn buy-db-only!
  "DB のトランザクションで在庫と履歴を1つの単位にする。
   Redis を使わない。"
  [user-id item-id]
  (jdbc/with-transaction [tx db/ds]
    (let [n (-> (jdbc/execute! tx ["UPDATE items SET stock = stock - 1
                                    WHERE item_id = ? AND stock > 0" item-id])
                first :next.jdbc/update-count)]
      (if (pos? n)
        (do (jdbc/execute! tx ["INSERT INTO purchases (purchase_id, user_id, item_id)
                                VALUES (?, ?, ?)"
                               (str (random-uuid)) user-id item-id])
            :ok)
        :sold-out))))

(defn buy-db-only-crash!
  "途中でクラッシュしても、トランザクションがロールバックする。"
  [user-id item-id]
  (jdbc/with-transaction [tx db/ds]
    (let [n (-> (jdbc/execute! tx ["UPDATE items SET stock = stock - 1
                                    WHERE item_id = ? AND stock > 0" item-id])
                first :next.jdbc/update-count)]
      (if (pos? n)
        (do
          (when (< (rand) 0.3)
            (throw (ex-info "INSERT の前にクラッシュ" {})))
          (jdbc/execute! tx ["INSERT INTO purchases (purchase_id, user_id, item_id)
                              VALUES (?, ?, ?)"
                             (str (random-uuid)) user-id item-id])
          :ok)
        :sold-out))))

;; ---------- バッチ処理の排他 ----------

(def batch-runs (atom []))

(defn reset-batch-log! [] (reset! batch-runs []))

(defn run-batch!
  "集計バッチ。実行を記録する。duration-ms で処理時間を模擬する。"
  [worker-id duration-ms]
  (let [start (System/currentTimeMillis)]
    (Thread/sleep duration-ms)
    (swap! batch-runs conj {:worker worker-id :start start
                            :end (System/currentTimeMillis)})
    :done))

;; ---------- 素朴なロック ----------

(def ^:const lock-key "lock:batch")

(defn with-lock-naive!
  "SET NX EX でロックを取り、処理して DEL で解放する。"
  [pool lock-ttl-sec f]
  (if (= "OK" (r/cmd pool "SET" lock-key "1" "NX" "EX" lock-ttl-sec))
    (try
      (f)
      (finally (r/cmd pool "DEL" lock-key)))
    :skipped))

(defn with-lock-blocking!
  "ロックが取れるまで待つ。実際のバッチ処理に近い。"
  [pool lock-ttl-sec f]
  (loop [attempt 0]
    (if (= "OK" (r/cmd pool "SET" lock-key "1" "NX" "EX" lock-ttl-sec))
      (try (f) (finally (r/cmd pool "DEL" lock-key)))
      (if (< attempt 200)
        (do (Thread/sleep 50) (recur (inc attempt)))
        :timeout))))

;; ---------- トークン付きロック ----------

(defn- release-lock!
  "自分のトークンと一致する場合だけ削除する。
   GET と DEL が別コマンドなので、この判定自体が原子的でない。"
  [pool token]
  (when (= token (r/cmd pool "GET" lock-key))
    (r/cmd pool "DEL" lock-key)))

(defn with-lock-token!
  "トークン付きロック。解放時に自分のものか確認する。"
  [pool lock-ttl-sec f]
  (let [token (str (random-uuid))]
    (loop [attempt 0]
      (if (= "OK" (r/cmd pool "SET" lock-key token "NX" "EX" lock-ttl-sec))
        (try (f) (finally (release-lock! pool token)))
        (if (< attempt 200)
          (do (Thread/sleep 50) (recur (inc attempt)))
          :timeout)))))

(comment
  (require '[my-redis-app.day4.lock :as d4] '[my-redis-app.bench :as bench]
           '[my-redis-app.redis :as r] :reload)
  
  (def mine (r/pool 6380))
  (def real (r/pool 6379))
  (def item "i001")
  
  (defn test-purchase [label buy-fn stock attempts threads]
    (d4/reset-all! mine item stock)
    (let [results (atom {})
          each    (quot attempts threads)]
      (->> (range threads)
           (map (fn [t] (future (dotimes [i each]
                                  (let [uid (str "u" t "-" i)]
                                    (swap! results update (buy-fn mine uid item) (fnil inc 0)))))))
           doall
           (run! deref))
      (let [final-stock (d4/stock-of mine item)
            purchased   (d4/purchase-count item)]
        (println (format "%-20s 在庫 %2d → %3d  成功 %3d  履歴 %3d  %s"
                         label stock final-stock (:ok @results 0) purchased
                         (cond
                           (not= (:ok @results 0) purchased) "★成功数と履歴が不一致★"
                           (> purchased stock)               "★売り過ぎ★"
                           (not= (- stock final-stock) purchased) "★在庫と履歴が不一致★"
                           :else "整合"))))))
  
  (dotimes [_ 3] (test-purchase "ロックなし" d4/buy-no-lock! 20 200 20))
  (dotimes [_ 3] (test-purchase "DECR" d4/buy-decr! 20 200 20))
  
  (defn test-with-crash [label buy-fn stock attempts threads]
    (d4/reset-all! mine item stock)
    (let [ok (atom 0)]
      (->> (range threads)
           (map (fn [t] (future (dotimes [i (quot attempts threads)]
                                  (try
                                    (when (= :ok (buy-fn mine (str "u" t "-" i) item))
                                      (swap! ok inc))
                                    (catch Exception _ nil))))))
           doall
           (run! deref))
      (println (format "%-24s 在庫 %2d → %2d（%2d 減）  履歴 %2d  差 %2d"
                       label stock (d4/stock-of mine item)
                       (- stock (d4/stock-of mine item))
                       (d4/purchase-count item)
                       (- (- stock (d4/stock-of mine item)) (d4/purchase-count item))))))
  
  (dotimes [_ 3] (test-with-crash "DECR + クラッシュ" d4/buy-decr-crash! 20 200 20))
  
  (defn reset-db-only! [item-id stock]
    (db/q "UPDATE items SET stock = ? WHERE item_id = ?" stock item-id)
    (db/q "DELETE FROM purchases WHERE item_id = ?" item-id))
  
  (defn stock-db-of [item-id]
    (:stock (db/q1 "SELECT stock FROM items WHERE item_id = ?" item-id)))
  
  (defn test-db-only [label buy-fn stock attempts threads]
    (reset-db-only! item stock)
    (let [ok (atom 0)]
      (->> (range threads)
           (map (fn [t] (future (dotimes [i (quot attempts threads)]
                                  (try
                                    (when (= :ok (buy-fn (str "u" t "-" i) item))
                                      (swap! ok inc))
                                    (catch Exception _ nil))))))
           doall
           (run! deref))
      (println (format "%-28s 在庫 %2d → %2d（%2d 減）  履歴 %2d  %s"
                       label stock (stock-db-of item)
                       (- stock (stock-db-of item))
                       (d4/purchase-count item)
                       (if (= (- stock (stock-db-of item)) (d4/purchase-count item))
                         "整合" "★不整合★")))))
  
  (dotimes [_ 3] (d4/test-db-only "DB トランザクション" d4/buy-db-only! 20 200 20))
  (dotimes [_ 3] (d4/test-db-only "DB + クラッシュ" d4/buy-db-only-crash! 20 200 20))
  
  (defn test-batch [label lock-fn n-workers duration-ms lock-ttl]
    (r/cmd mine "DEL" d4/lock-key)
    (d4/reset-batch-log!)
    (->> (range n-workers)
         (map (fn [w] (future (lock-fn mine lock-ttl #(d4/run-batch! w duration-ms)))))
         doall
         (run! deref))
    (let [runs @d4/batch-runs]
      (println (format "%-28s 実行 %d 回  %s"
                       label (count runs)
                       (if (= 1 (count runs)) "" "★重複実行★")))))
  
  (defn test-batch-retry [label lock-fn n-workers duration-ms lock-ttl attempts]
    (r/cmd mine "DEL" d4/lock-key)
    (d4/reset-batch-log!)
    (->> (range n-workers)
         (map (fn [w] (future
                        (dotimes [_ attempts]
                          (lock-fn mine lock-ttl #(d4/run-batch! w duration-ms))
                          (Thread/sleep 200)))))
         doall
         (run! deref))
    (let [runs (sort-by :start @d4/batch-runs)
          ;; 実行時間が重なっているペアを数える
          overlaps (count (for [[a b] (partition 2 1 runs)
                                :when (< (:start b) (:end a))]
                            [a b]))]
      (println (format "%-28s 実行 %2d 回  重複 %2d 組  %s"
                       label (count runs) overlaps
                       (if (zero? overlaps) "" "★同時実行★")))))
  
  (test-batch "素朴なロック（TTL 10s, 処理 100ms）" d4/with-lock-naive! 20 100 10)
  (test-batch-retry "TTL 5s, 処理 100ms" d4/with-lock-naive! 10 100 5 5)
  (test-batch-retry "TTL 1s, 処理 3000ms" d4/with-lock-naive! 10 3000 1 2))