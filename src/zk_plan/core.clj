(ns zk-plan.core
  (:use [zookeeper :as zk]))

(defn create-plan [zk parent]
  (zk/create zk (str parent "/plan-") :sequential? true))

(defn to-bytes [str]
  (.getBytes str "UTF-8"))

(defn set-clj-data [zk node data]
  (let [str (pr-str data)
        bytes (to-bytes str)
        ver (:version (zk/exists zk node))]
    (zk/set-data zk node bytes ver)))

(defn add-task [zk plan fn arg-tasks]
  (let [task (zk/create zk (str plan "/task-") :sequential? true)]
    (set-clj-data zk task fn)
    task))

