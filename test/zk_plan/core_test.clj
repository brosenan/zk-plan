(ns zk-plan.core-test
  (:use midje.sweet)
  (:use [zk-plan.core])
  (:use [zookeeper :as zk]))

[[:chapter {:title "External API"}]]
[[:section {:title "create-plan"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **parent:** the parent node for the new plan

**Returns:** the path to the plan
It calls zk/createn to create a new zookeeper node"
(fact
 (create-plan ..zk.. ..parent..) => ..node..
 (provided
  (zk/create ..zk.. ..prefix.. :persistent? true :sequential? true) => ..node..
  (str ..parent.. "/plan-") => ..prefix..))


[[:section {:title "add-task"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **plan:** the path to the plan
- **fn:** the function to be executed
- **arg-tasks:** a sequence of task paths, which return values are to become arguments for fn

**Returns:** path to the new task"

"It creates a sequential node under the plan"
(fact 
      (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
      (provided
       (zk/create ..zk.. ..prefix.. :persistent? true :sequential? true) => ..task..
       (str ..plan.. "/task-") => ..prefix..
       (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
       (mark-as-ready irrelevant irrelevant) => irrelevant))
"It sets the task node's data to contain a serialization of fn"
(fact
      (add-task ..zk.. ..plan.. ..fn.. []) => ..task..
      (provided
       (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
       (set-initial-clj-data ..zk.. ..task.. ..fn..) => irrelevant
       (mark-as-ready irrelevant irrelevant) => irrelevant))
"It calls add-dependency for each arg-task"
(fact
      (add-task ..zk.. ..plan.. ..fn.. [..arg1.. ..arg2.. ..arg3..]) => irrelevant
      (provided
       (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
       (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
       (add-dependency ..zk.. ..arg1.. ..task..) => irrelevant
       (add-dependency ..zk.. ..arg2.. ..task..) => irrelevant
       (add-dependency ..zk.. ..arg3.. ..task..) => irrelevant
       (mark-as-ready irrelevant irrelevant) => irrelevant))
"It adds a 'ready' node once definition is complete"
(fact
      (add-task ..zk.. ..plan.. ..fn.. []) => irrelevant
      (provided
       (zk/create irrelevant irrelevant :persistent? true :sequential? true) => ..task..
       (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant
       (mark-as-ready ..zk.. ..task..) => irrelevant))

[[:section {:title "get-task"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **plan:** the path to the plan

**Returns:** path to the task"
"It returns nil if the plan is empty"
(fact
      (get-task ..zk.. ..plan..) => nil
      (provided
       (zk/children ..zk.. ..plan..) => nil))
"It returns a task if it does not have `dep-*` or owner as children"
(fact
      (get-task ..zk.. "/foo") => "/foo/bar"
      (provided
       (zk/children ..zk.. "/foo") => '("bar")
       (zk/children ..zk.. "/foo/bar") => '("baz" "ready" "quux")
       (take-ownership ..zk.. "/foo/bar") => true))
"It does not return tasks that have `dep-*` children"
(fact
      (get-task ..zk.. "/foo") => nil
      (provided
       (zk/children ..zk.. "/foo") => '("bar")
       (zk/children ..zk.. "/foo/bar") => '("baz" "ready" "quux" "dep-0001")))
"It does not return tasks that have owner nodes"
(fact
      (get-task ..zk.. "/foo") => nil
      (provided
       (zk/children ..zk.. "/foo") => '("bar")
       (zk/children ..zk.. "/foo/bar") => '("baz" "quux" "ready" "owner")))
"It does not take tasks that are not marked ready"
(fact
      (get-task ..zk.. "/foo") => nil
      (provided
       (zk/children ..zk.. "/foo") => '("bar")
       (zk/children ..zk.. "/foo/bar") => '("baz" "quux")))
"It takes ownership over the task by adding an 'owner' node"
(fact
      (get-task ..zk.. "/foo") => "/foo/bar"
      (provided
       (zk/children ..zk.. "/foo") => '("bar")
       (zk/children ..zk.. "/foo/bar") => '("ready")
       (take-ownership ..zk.. "/foo/bar") => true))
"It moves to the next task if it is unable to take ownership"
(fact
      (get-task ..zk.. "/foo") => "/foo/baz"
      (provided
       (zk/children ..zk.. "/foo") => '("bar" "baz")
       (zk/children ..zk.. "/foo/bar") => '("ready")
       (take-ownership ..zk.. "/foo/bar") => false
       (zk/children ..zk.. "/foo/baz") => '("ready")
       (take-ownership ..zk.. "/foo/baz") => true))
"It looks up children lazily"
(fact
      (get-task ..zk.. "/foo") => "/foo/bar"
      (provided
       (zk/children ..zk.. "/foo") => '("bar" "baz" "bat")
       (zk/children ..zk.. irrelevant) => '("ready") :times 1
       (take-ownership ..zk.. "/foo/bar") => true))


[[:section {:title "perform-task"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **task:** path to the task to perform

**Returns:** Nothing in particular"
"It evaluates the task data"
(fact
      (perform-task ..zk.. "/foo/bar") => irrelevant
      (provided
       (get-clj-data ..zk.. "/foo/bar") => ..func..
       (execute-function irrelevant irrelevant irrelevant) => irrelevant
       (zk/create irrelevant irrelevant :persistent? true) => true
       (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant))
"It writes the return value as a 'result' node"
(fact
      (perform-task ..zk.. "/foo/bar") => irrelevant
      (provided
       (get-clj-data ..zk.. "/foo/bar") => ..fn..
       (execute-function ..zk.. ..fn.. "/foo/bar") => ..result..
       (zk/create ..zk.. "/foo/bar/result" :persistent? true) => true
       (set-initial-clj-data ..zk.. "/foo/bar/result" ..result..) => irrelevant))


[[:chapter {:title "Internal Implementation"}]]
[[:section {:title "set-initial-clj-data"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **node:** the node 

**Returns:** Nothing in particular"

"It calls zk/set-data to update the data"
(fact
      (set-initial-clj-data ..zk.. ..node.. ..data..) => irrelevant
      (provided
       (pr-str ..data..) => ..str..
       (to-bytes ..str..) => ..bytes..
       (zk/set-data ..zk.. ..node.. ..bytes.. irrelevant) => irrelevant))
"It derives the version number from the existing version"
(fact
      (set-initial-clj-data ..zk.. ..node.. ..data..) => irrelevant
      (provided
       (zk/set-data irrelevant irrelevant irrelevant 0) => irrelevant))


[[:section {:title "add-dependency"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **from:** path of the task that provides the dependency
- **to:** path of the task that depends on 'from' 

**Returns:** nothing in particular"

"It adds sequential children to both the 'from' and the 'to' tasks"
(fact
      (add-dependency ..zk.. "/path/from" "/path/to") => irrelevant
      (provided
       (zk/create ..zk.. "/path/from/prov-" :persistent? true :sequential? true) => irrelevant
       (zk/create ..zk.. "/path/to/dep-" :persistent? true :sequential? true) => irrelevant
       (set-initial-clj-data irrelevant irrelevant irrelevant) => irrelevant))
"It sets the data of the prov child to be the path to the corresponding dep child"
(fact 
      (add-dependency ..zk.. "/path/from" "/path/to") => irrelevant
      (provided
       (zk/create ..zk.. "/path/from/prov-" :persistent? true :sequential? true) => ..from-link..
       (zk/create ..zk.. "/path/to/dep-" :persistent? true :sequential? true) => ..to-link..
       (set-initial-clj-data ..zk.. ..from-link.. ..to-link..) => irrelevant))





[[:section {:title "mark-as-ready"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **task:** the task to be marked as ready

**Returns:** nothing in particular"
"It creates a child node named 'ready'"
(fact
      (mark-as-ready ..zk.. "/foo/bar") => irrelevant
      (provided
       (zk/create ..zk.. "/foo/bar/ready" :persistent? true) => true))

[[:section {:title "take-ownership"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **task:** the task to take ownership over

**Returns:** whether or not we managed to take ownership"

"It tries to add an ephemeral 'owner' node to the task, and return whether it was successful"
(fact
      (take-ownership ..zk.. "/foo/bar") => ..result..
      (provided
       (zk/create ..zk.. "/foo/bar/owner" :persistent? false) => ..result..))



[[:section {:title "execute-function"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **func:** an s-expression that evaluates to the function to be executed
- **task:** path to the task node containing arguments for the function  

**Returns:** the return value from the function"
"It executes the function without parameters if no parameters exist in the task"
(fact
      (execute-function ..zk.. '(fn [] 3) ..task..) => 3
      (provided
       (zk/children ..zk.. ..task..) => '("foo" "bar")))
"It passes the task arguments to the function"
(fact
      (execute-function ..zk.. '(fn [& args] args) "/foo/bar") => [1 2 3]
      (provided
       (zk/children ..zk.. "/foo/bar") => '("arg-00001" "arg-00002" "arg-00000")
       (get-clj-data ..zk.. "/foo/bar/arg-00000") => 1
       (get-clj-data ..zk.. "/foo/bar/arg-00001") => 2
       (get-clj-data ..zk.. "/foo/bar/arg-00002") => 3))
