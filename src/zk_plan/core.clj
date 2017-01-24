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

(defn mark-as-ready [zk task]
  (zk/create zk (str task "/ready") :persistent? true))

(defn add-task [zk plan fn arg-tasks]
  (let [task (zk/create zk (str plan "/task-") :persistent? true :sequential? true)]
    (set-initial-clj-data zk task fn)
    (doseq [arg arg-tasks]
      (add-dependency zk arg task))
    (mark-as-ready zk task)
    task))

(defn take-ownership [zk task]
  (zk/create zk (str task "/owner") :persistent? false))

(defn get-task [zk plan]
  (let [task-names (zk/children zk plan)
        tasks (map #(str plan "/" %) task-names)
        valid-tasks (filter (fn [task]
                              (let [task-props (zk/children zk task)]
                                (and (not (some #(or (re-matches #"dep-[0-9]+" %)
                                                     (= "owner" %)) task-props))
                                     (contains? (set task-props) "ready")
                                     (take-ownership zk task)))) tasks)]
    (first valid-tasks)))


(defn get-clj-data [zk node]
  (read-string (String. (zk/data zk node) "UTF-8")))

(defn execute-function [zk node]
  (let [func (get-clj-data zk node)
        children (zk/children zk node)
        argnodes (filter #(re-matches #"arg-\d+" %) children)
        argnodes (sort argnodes)
        argnodes (map #(str node "/" %) argnodes)
        vals (map #(get-clj-data zk %) argnodes)]
    (apply (eval func) vals)))

(defn propagate-result [zk prov result])

(defn prov? [key]
  (re-matches #"prov-\d+" key))

(defn perform-task [zk task]
  (let [children   (zk/children zk task)
        result-node (str task "/result")
        res (if (contains? (set children) "result")
              (get-clj-data zk result-node)
              ; else
              (execute-function zk task))]
    (when-not (contains? (set children) "result")
      (zk/create zk result-node :persistent? true)
      (set-initial-clj-data zk result-node res))
    (doseq [prov (filter prov? children)]
      (propagate-result zk (str task "/" prov) res)))
  (zk/delete-all zk task))

(comment   (let [func (get-clj-data zk task)
        result (execute-function zk func task)
        res-node (str task "/result")]
    (zk/create zk res-node :persistent? true)
    (set-initial-clj-data zk res-node result)))

