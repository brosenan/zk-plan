(ns zk-plan.core
  (:use [zookeeper :as zk]))

(defn create-plan [zk parent]
  (zk/create zk (str parent "/plan-") :persistent? true :sequential? true))

(defn to-bytes [str]
  (.getBytes str "UTF-8"))

(defn set-clj-data [zk node data]
  (let [str (pr-str data)
        bytes (to-bytes str)
        ver (:version (zk/exists zk node))]
    (zk/set-data zk node bytes ver)))

(defn add-dependency [zk from to]
  (let [prov (zk/create zk (str from "/prov-") :persistent? true :sequential? true)
        dep (zk/create zk (str to "/dep-") :persistent? true :sequential? true)]
    (set-clj-data zk prov dep)))

(defn add-task [zk plan fn arg-tasks]
  (let [task (zk/create zk (str plan "/task-") :persistent? true :sequential? true)]
    (set-clj-data zk task fn)
    (doseq [arg arg-tasks]
      (add-dependency zk arg task))
    task))

