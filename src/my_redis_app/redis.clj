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
