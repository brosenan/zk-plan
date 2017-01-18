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
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (mark-as-ready irrelevant irrelevant) => irrelevant))
       (fact "it sets the task node's data to contain a serialization of fn"
             (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data ..zk.. ..task.. ..fn..) => irrelevant
              (mark-as-ready irrelevant irrelevant) => irrelevant))
       (fact "it calls add-dependency for each arg-task"
             (add-task ..zk.. ..plan.. ..fn.. [..arg1.. ..arg2.. ..arg3..]) => irrelevant
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (add-dependency ..zk.. ..arg1.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg2.. ..task..) => irrelevant
              (add-dependency ..zk.. ..arg3.. ..task..) => irrelevant
              (mark-as-ready irrelevant irrelevant) => irrelevant))
       (fact "it adds a 'ready' node once definition is complete"
             (add-task ..zk.. ..plan.. ..fn.. []) => irrelevant
             (provided
              (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
              (mark-as-ready ..zk.. ..task..) => irrelevant)))

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
              (zk/children ..zk.. "/foo/bar") => '("baz" "ready" "quux")
              (take-ownership ..zk.. "/foo/bar") => true))
       (fact "it does not return tasks that have dep-* children"
             (get-task ..zk.. "/foo") => nil
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "ready" "quux" "dep-0001")))
       (fact "it does not return tasks that have owner nodes"
             (get-task ..zk.. "/foo") => nil
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "quux" "ready" "owner")))
       (fact "it does not take tasks that are not marked ready"
             (get-task ..zk.. "/foo") => nil
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("baz" "quux")))
       (fact "it takes ownership over the task by adding an 'owner' node"
             (get-task ..zk.. "/foo") => "/foo/bar"
             (provided
              (zk/children ..zk.. "/foo") => '("bar")
              (zk/children ..zk.. "/foo/bar") => '("ready")
              (take-ownership ..zk.. "/foo/bar") => true))
       (fact "it moves to the next task if it is unable to take ownership"
             (get-task ..zk.. "/foo") => "/foo/baz"
             (provided
              (zk/children ..zk.. "/foo") => '("bar" "baz")
              (zk/children ..zk.. "/foo/bar") => '("ready")
              (take-ownership ..zk.. "/foo/bar") => false
              (zk/children ..zk.. "/foo/baz") => '("ready")
              (take-ownership ..zk.. "/foo/baz") => true)))

(facts "about (mark-as-read zk task)"
       (fact "it creates a child node named 'ready'"
             (mark-as-ready ..zk.. "/foo/bar") => irrelevant
             (provided
              (zk/create ..zk.. "/foo/bar/ready" :persistent? true) => true)))


(facts "about (take-ownership zk task)"
       (fact "it tries to add an ephemeral 'owner' node to the task, and return whether it was successful"
             (take-ownership ..zk.. "/foo/bar") => ..result..
             (provided
              (zk/create ..zk.. "/foo/bar/owner" :persistent? false) => ..result..)))

(facts "about (perform-task zk task)"
       (fact "it evaluates the task data"
             (perform-task ..zk.. "/foo/bar") => irrelevant
             (provided
              (get-clj-data ..zk.. "/foo/bar") => ..func..
              (execute-function irrelevant irrelevant irrelevant) => irrelevant
              (zk/create irrelevant irrelevant :persistent? true) => true
              (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant))
       (fact "it writes the return value as a 'result' node"
             (perform-task ..zk.. "/foo/bar") => irrelevant
             (provided
              (get-clj-data ..zk.. "/foo/bar") => ..fn..
              (execute-function ..zk.. ..fn.. "/foo/bar") => ..result..
              (zk/create ..zk.. "/foo/bar/result" :persistent? true) => true
              (set-initial-clj-data ..zk.. "/foo/bar/result" ..result..) => irrelevant)))

(facts "about (execute-function zk func task)"
       (fact "it executes the function without parameters if no parameters exist in the task"
             (execute-function ..zk.. '(fn [] 3) ..task..) => 3
             (provided
              (zk/children ..zk.. ..task..) => '("foo" "bar")))
       (fact "it passes the task arguments to the function"
             (execute-function ..zk.. '(fn [& args] args) "/foo/bar") => [1 2 3]
             (provided
              (zk/children ..zk.. "/foo/bar") => '("arg-00001" "arg-00002" "arg-00000")
              (get-clj-data ..zk.. "/foo/bar/arg-00000") => 1
              (get-clj-data ..zk.. "/foo/bar/arg-00001") => 2
              (get-clj-data ..zk.. "/foo/bar/arg-00002") => 3)))
