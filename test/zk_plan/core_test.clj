(ns zk-plan.core-test
  (:use midje.sweet)
  (:use [zk-plan.core])
  (:use [zookeeper :as zk]))

(facts "about (create-plan zk parent)"
       (fact "it calls zk/create"
             (create-plan ..zk.. ..parent..) => ..node..
             (provided
              (zk/create ..zk.. ..prefix.. :persistent? true :sequential? true) => ..node..
              (str ..parent.. "/plan-") => ..prefix..)))
(facts "about (add-task zk plan fn arg-tasks)"
       (fact "it creates a sequential node under the plan"
             (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
             (provided
              (zk/create ..zk.. ..prefix.. :persistent? true :sequential? true) => ..task..
              (str ..plan.. "/task-") => ..prefix..
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant)))

       (fact "it sets the task node's data to contain a serialization of fn"
             (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data ..zk.. ..task.. ..fn..) => irrelevant))
       (fact "it calls add-dependency for each arg-task"
             (add-task ..zk.. ..plan.. ..fn.. [..arg1.. ..arg2.. ..arg3..]) => irrelevant
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (add-dependency ..zk.. ..arg1.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg2.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg3.. ..task..) => irrelevant))
       (fact "it adds a 'ready' node once definition is complete"
             (add-task ..zk.. ..plan.. ..fn.. []) => irrelevant
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (mark-as-ready ..zk.. ..task..) => irrelevant))

(facts "about (set-initial-clj-data zk node data)"
       (fact "it calls zk/set-data to update the data"
             (set-initial-clj-data ..zk.. ..node.. ..data..) => irrelevant
             (provided
              (pr-str ..data..) => ..str..
              (to-bytes ..str..) => ..bytes..
              (zk/set-data ..zk.. ..node.. ..bytes.. irrelevant) => irrelevant))
       (fact "it derives the version number from the existing version"
             (set-initial-clj-data ..zk.. ..node.. ..data..) => irrelevant
             (provided
              (zk/set-data irrelevant irrelevant irrelevant 0) => irrelevant)))

(facts "about (add-dependency zk from to)"
       (fact "it adds sequential children to both the 'from' and the 'to' tasks"
             (add-dependency ..zk.. "/path/from" "/path/to") => irrelevant
             (provided
              (zk/create ..zk.. "/path/from/prov-" :persistent? true :sequential? true) => irrelevant
              (zk/create ..zk.. "/path/to/dep-" :persistent? true :sequential? true) => irrelevant
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant))
       (fact "it sets the data of the prov child to be the path to the corresponding dep child"
             (add-dependency ..zk.. "/path/from" "/path/to") => irrelevant
             (provided
              (zk/create ..zk.. "/path/from/prov-" :persistent? true :sequential? true) => ..from-link..
              (zk/create ..zk.. "/path/to/dep-" :persistent? true :sequential? true) => ..to-link..
              (set-initial-clj-data ..zk.. ..from-link.. ..to-link..) => irrelevant)))

(facts "about (get-task zk plan)"
       (fact "it returns nil if the plan is empty"
             (get-task ..zk.. ..plan..) => nil
             (provided
              (zk/children ..zk.. ..plan..) => nil))
       (fact "it returns a task if it does not have dep-* or owner as children"
             (get-task ..zk.. "/foo") => "/foo/bar"
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "quux")))
       (fact "it does not return tasks that have dep-* children"
             (get-task ..zk.. "/foo") => nil
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "quux" "dep-0001")))
       (fact "it does not return tasks that have owner nodes"
             (get-task ..zk.. "/foo") => nil
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "quux" "owner"))))
