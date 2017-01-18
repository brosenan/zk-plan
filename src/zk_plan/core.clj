(ns zk-plan.core
  (:use [zookeeper :as zk])
  (:require [clojure.string :as str]))

(defn create-plan [zk parent]
  (zk/create zk (str parent "/plan-") :persistent? true :sequential? true))

(defn to-bytes [str]
  (.getBytes str "UTF-8"))

(defn set-initial-clj-data [zk node data]
  (let [str (pr-str data)
        bytes (to-bytes str)]
    (zk/set-data zk node bytes 0)))

(defn add-dependency [zk from to]
  (let [prov (zk/create zk (str from "/prov-") :persistent? true :sequential? true)
        dep (zk/create zk (str to "/dep-") :persistent? true :sequential? true)]
    (set-initial-clj-data zk prov dep)))

(defn add-task [zk plan fn arg-tasks]
  (let [task (zk/create zk (str plan "/task-") :persistent? true :sequential? true)]
    (set-initial-clj-data zk task fn)
    (doseq [arg arg-tasks]
      (add-dependency zk arg task))
    task))

(defn get-task [zk plan]
  (let [task-names (zk/children zk plan)
        tasks (map #(str plan "/" %) task-names)
        valid-tasks (filter (fn [task]
                              (let [task-props (zk/children zk task)]
                                (not (some (partial re-matches #"dep-[0-9]+") task-props)))) tasks)]
    (first valid-tasks)))
