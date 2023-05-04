(ns broadcast.core
  (:require [cheshire.core :as json])
  (:gen-class))

(def msg-id (atom 1))

(defn gen-msg-id []
  (swap! msg-id inc))

(def broadcast-node
  (atom {:id nil,
         :node "",
         :messages [],
         :known {},
         :neighborhood []}))

(defn gen-reply-init [msg]
  {:src (:dest msg)
   :dest (:src msg)
   :body {:msg_id (gen-msg-id)
          :in_reply_to (get-in msg [:body :msg_id])
          :type "init_ok"}})

(defn save-new-msg! [msg]
  (swap! broadcast-node update-in [:messages] conj msg))

(defn gen-broadcast-reply [msg msg-id]
  (save-new-msg! (get-in msg [:body :message]))
  {:src (:dest msg)
   :dest (:src msg)
   :body {:type "broadcast_ok",
          :in_reply_to msg-id}})

(defn gen-read-reply [msg msg-id]
  {:src (:dest msg)
   :dest (:src msg)
   :body {:type "read_ok",
          :messages (get-in @broadcast-node [:messages])
          :in_reply_to msg-id}})

(defn gen-topology-reply [msg msg-id]
  {:src (:dest msg)
   :dest (:src msg)
   :body {:type "topology_ok"
          :in_reply_to msg-id}})

(defn input-loop
  []
  (doseq [line (line-seq (java.io.BufferedReader. *in*))]
    (if (empty? line)
      (throw (Exception. "Empty messages are not allowed."))
      (let [nline (json/decode line true)
            body-type (get-in nline [:body :type])
            msg-id (get-in nline [:body :msg_id])
            node-id (get-in nline [:body :node_id])]
        (cond
          (= body-type "init")
          (do
            (binding [*out* *err*]
              (println (str "Initialized node " node-id)))
            (println (json/encode (gen-reply-init nline))))

          (= body-type "broadcast")
          (println (json/encode (gen-broadcast-reply nline msg-id)))
          (= body-type "read")
          (println (json/encode (gen-read-reply nline msg-id)))
          (= body-type "topology")
          (println (json/encode (gen-topology-reply nline msg-id))))))))

(comment
  (input-loop)
  (println @broadcast-node)
  (gen-broadcast-reply 1891 1)
  (gen-read-reply {:src "n1", :dest "n2", :body {:type "read"}} 1))

(defn -main
  []
  (input-loop))
