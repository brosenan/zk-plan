(ns zk-plan.core-test
  (:use midje.sweet)
  (:use [zk-plan.core])
  (:use [zookeeper :as zk]))

(facts "about (create-plan zk parent)"
       (fact "it calls zk/create"
             (create-plan ..zk.. ..parent..) => ..node..
             (provided
              (zk/create ..zk.. ..prefix.. :sequential? true) => ..node..
              (str ..parent.. "/plan-") => ..prefix..)))
(facts "about (add-task zk plan fn arg-tasks)"
       (fact "it creates a sequential node under the plan"
             (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
             (provided
              (zk/create ..zk.. ..prefix.. :sequential? true) => ..task..
              (str ..plan.. "/task-") => ..prefix..
              (set-clj-data irrelevant irrelevant irrelevant) => irrelevant))
       (fact "it sets the task node's data to contain a serialization of fn"
             (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
             (provided
              (zk/create irrelevant irrelevant :sequential? true) => ..task..
              (set-clj-data ..zk.. ..task.. ..fn..) => irrelevant))
       (fact "it calls add-dependency for each arg-task"
             (add-task ..zk.. ..plan.. ..fn.. [..arg1.. ..arg2.. ..arg3..]) => irrelevant
             (provided
              (zk/create irrelevant irrelevant :sequential? true) => ..task..
              (set-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (add-dependency ..zk.. ..arg1.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg2.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg3.. ..task..) => irrelevant)))

(facts "about (set-clj-data zk node data)"
       (fact "it calls zk/set-data to update the data"
             (set-clj-data ..zk.. ..node.. ..data..) => irrelevant
             (provided
              (pr-str ..data..) => ..str..
              (to-bytes ..str..) => ..bytes..
              (zk/set-data ..zk.. ..node.. ..bytes.. irrelevant) => irrelevant
              (zk/exists irrelevant irrelevant) => irrelevant))
       (fact "it derives the version number from the existing version"
             (set-clj-data ..zk.. ..node.. ..data..) => irrelevant
             (provided
              (zk/exists ..zk.. ..node..) => {:version ..oldver.. :foo "bar"}
              (zk/set-data irrelevant irrelevant irrelevant ..oldver..) => irrelevant)))
