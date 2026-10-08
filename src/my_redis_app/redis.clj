(ns my-redis-app.redis
  "最小限の Redis クライアント。コネクションプール付き。
   RESP のエンコード・デコードは my-redis の resp 層を流用する。"
  (:require [my-redis.resp :as resp])
  (:import [java.net Socket]
           [java.io BufferedInputStream BufferedOutputStream]
           [java.util.concurrent LinkedBlockingQueue TimeUnit]))

;; ---------- 1 本の接続 ----------

(defrecord Conn [^Socket socket in out])

(defn- connect
  [host port]
  (let [sock (doto (Socket. ^String host (int port))
               (.setTcpNoDelay true)
               (.setSoTimeout 5000))]
    (->Conn sock
            (BufferedInputStream. (.getInputStream sock))
            (BufferedOutputStream. (.getOutputStream sock)))))

(defn- close-conn! [^Conn c]
  (try (.close ^Socket (:socket c)) (catch Exception _ nil)))

(defn- conn-alive? [^Conn c]
  (and c (not (.isClosed ^Socket (:socket c)))))

(defn- send-on!
  "1 本の接続でコマンドを実行する。"
  [^Conn c cmd]
  (let [^BufferedOutputStream out (:out c)]
    (resp/write-reply! out (mapv str cmd))
    (.flush out)
    (resp/read-reply (:in c))))

;; ---------- プール ----------

(defrecord Pool [host port queue])

(defn pool
  "接続プールを作る。size 本をあらかじめ張る。"
  ([port] (pool "localhost" port 8))
  ([host port size]
   (let [q (LinkedBlockingQueue. (int size))]
     (dotimes [_ size] (.put q (connect host port)))
     (->Pool host port q))))

(defn close-pool! [^Pool p]
  (loop []
    (when-let [c (.poll ^LinkedBlockingQueue (:queue p))]
      (close-conn! c)
      (recur))))

(defn- borrow!
  ^Conn [^Pool p]
  (let [c (.poll ^LinkedBlockingQueue (:queue p) 5 TimeUnit/SECONDS)]
    (when-not c (throw (ex-info "redis pool exhausted" {})))
    (if (conn-alive? c) c (connect (:host p) (:port p)))))

(defn- return! [^Pool p c]
  (.offer ^LinkedBlockingQueue (:queue p) c))

(defn cmd
  "コマンドを 1 つ実行する。エラー応答は ex-info にして投げる。"
  [^Pool p & args]
  (let [c (borrow! p)]
    (try
      (let [r (send-on! c (vec args))]
        (return! p c)
        (if (resp/error? r)
          (throw (ex-info (:message r) {:cmd (vec args)}))
          r))
      (catch Exception e
        ;; 接続が壊れた可能性があるので捨てて張り直す
        (close-conn! c)
        (return! p (connect (:host p) (:port p)))
        (throw e)))))

(defn cmd*
  "エラー応答を例外にせず、そのまま返す。エラー文面を見たいとき用。"
  [^Pool p & args]
  (let [c (borrow! p)]
    (try
      (let [r (send-on! c (vec args))]
        (return! p c)
        r)
      (catch Exception e
        (close-conn! c)
        (return! p (connect (:host p) (:port p)))
        (throw e)))))

(defn- send-all!
  "応答を読まずにコマンド列を全部書き、最後に 1 回だけ flush する。"
  [^Conn c commands]
  (let [^BufferedOutputStream out (:out c)]
    (doseq [cmd commands]
      (resp/write-reply! out (mapv str cmd)))
    (.flush out)))

(defn- read-n
  "n 件の応答を順に読む。"
  [^Conn c n]
  (let [in (:in c)]
    (mapv (fn [_] (resp/read-reply in)) (range n))))

(defn pipeline
  "コマンド列をまとめて送り、応答をまとめて受け取る。
   commands は [[\"SET\" \"k\" \"v\"] [\"GET\" \"k\"] ...]。
   戻り値は commands と同じ順序・同じ長さのベクタ。
   エラー応答は投げずにそのまま要素として返す（後述）。"
  [^Pool p commands]
  (let [commands (vec commands)
        c (borrow! p)]
    (try
      (send-all! c commands)
      (let [replies (read-n c (count commands))]
        (return! p c)
        replies)
      (catch Exception e
        (close-conn! c)
        (return! p (connect (:host p) (:port p)))
        (throw e)))))

(defn pipeline-batched
  ([^Pool p commands] (pipeline-batched p commands 1000))
  ([^Pool p commands batch-size]
   (into [] (mapcat #(pipeline p %) (partition-all batch-size commands)))))

(defn pipeline-batched-on-one-conn
  "1 本の接続を保持したままバッチを回す。"
  ([^Pool p commands] (pipeline-batched-on-one-conn p commands 1000))
  ([^Pool p commands batch-size]
   (let [c (borrow! p)]
     (try
       (let [result (into []
                          (mapcat (fn [batch]
                                    (let [batch (vec batch)]
                                      (send-all! c batch)
                                      (read-n c (count batch)))))
                          (partition-all batch-size commands))]
         (return! p c)
         result)
       (catch Exception e
         (close-conn! c)
         (return! p (connect (:host p) (:port p)))
         (throw e))))))

(defn raw-connect [port]
  (let [s (java.net.Socket. "localhost" port)]
    {:sock s
     :in (java.io.BufferedInputStream. (.getInputStream s))
     :out (java.io.BufferedOutputStream. (.getOutputStream s))}))

;; 20 本つなごうとする
(def conns
  (doall
   (for [i (range 20)]
     (try
       (let [c (raw-connect 6379)]
         ;; 実際に通信して接続が生きているか確認
         (resp/write-reply! (:out c) ["PING"])
         (.flush (:out c))
         {:i i :ok (resp/read-reply (:in c)) :conn c})
       (catch Exception e
         {:i i :error (.getMessage e)})))))
