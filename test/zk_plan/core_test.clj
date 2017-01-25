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

[[:section {:title "worker"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **parent:** the parent node of all plans
- **attributes:** a map with attributes for the behavior of the worker

**Returns:** nothing in particular"
"It does the following:
- calls `get-task-from-any-plan` to get a task to work on
- if a task is returned (we have something to do), it calls `perform-task` to run it"
(fact
 (worker ..zk.. ..parent.. ..attrs..) => irrelevant
 (provided
  (get-task-from-any-plan ..zk.. ..parent..) => "/foo/bar"
  (perform-task ..zk.. "/foo/bar") => irrelevant))

"If `get-task-from-any-plan` returns `nil`, we call `calc-sleep-time` to calculate for how long
we need to sleep before the next retry.
We retry until we get a task."
(fact
 (worker ..zk.. ..parent.. ..attrs..) => irrelevant
 (provided
  (get-task-from-any-plan ..zk.. ..parent..) =streams=> [nil nil "/foo/bar"]
  (calc-sleep-time ..attrs.. 0) => 1
  (calc-sleep-time ..attrs.. 1) => 2
  (perform-task irrelevant irrelevant) => irrelevant))

[[:chapter {:title "Internal Implementation"}]]
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

"If the task has a 'result' child and no 'prov-*' children, this means the task completed
successfully, and the result has been distributed to all dependent tasks (if any).
In such a case we remove the task."
(fact
 (perform-task ..zk.. "/foo/bar") => irrelevant
 (provided
  (zk/children ..zk.. "/foo/bar") => '("result")
  (get-clj-data irrelevant irrelevant) => 123
  (zk/delete-all ..zk.. "/foo/bar") => irrelevant))

"If prov-* children exist, it reads the result and distributes it across the tasks
depending on this task (the corresponding dep-* nodes)"
(fact
 (perform-task ..zk.. "/foo/bar") => irrelevant
 (provided
  (zk/children ..zk.. "/foo/bar") => '("result" "prov-00000" "prov-0001")
  (get-clj-data ..zk.. "/foo/bar/result") => 3.1415
  (propagate-result ..zk.. "/foo/bar/prov-00000" 3.1415) => irrelevant
  (propagate-result ..zk.. "/foo/bar/prov-0001" 3.1415) => irrelevant
  (zk/delete-all irrelevant irrelevant) => irrelevant))

"If the task does not have a result, we need to calculate the result ourselves.
We call execute-function to get the result, and store it as the 'result' child."
(fact
 (perform-task ..zk.. "/foo/bar") => irrelevant
 (provided
  (zk/children ..zk.. "/foo/bar") => '()
  (execute-function ..zk.. "/foo/bar") => 1234.5
                                        ; It should create a result child node and store the result to it
  (zk/create ..zk.. "/foo/bar/result" :persistent? true) => true
  (set-initial-clj-data ..zk.. "/foo/bar/result" 1234.5) => irrelevant
  (zk/delete-all irrelevant irrelevant) => irrelevant))

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
- **task:** path to the task node containing arguments for the function  

**Returns:** the return value from the task's function"
"It reads the function definition from the content of the task node.
If no parameters exist in the task it executes the function without parameters."
(fact
      (execute-function ..zk.. ..task..) => 3
      (provided
       (get-clj-data ..zk.. ..task..) => '(fn [] 3)
       (zk/children ..zk.. ..task..) => '("foo" "bar")))
"It passes the task arguments to the function"
(fact
      (execute-function ..zk.. "/foo/bar") => [1 2 3]
      (provided
       (get-clj-data ..zk.. "/foo/bar") => '(fn [& args] args)
       (zk/children ..zk.. "/foo/bar") => '("arg-00001" "arg-00002" "arg-00000")
       (get-clj-data ..zk.. "/foo/bar/arg-00000") => 1
       (get-clj-data ..zk.. "/foo/bar/arg-00001") => 2
       (get-clj-data ..zk.. "/foo/bar/arg-00002") => 3))

[[:section {:title "propagate-result"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **prov:** path to the `prov-*` node to propagate
- **value:** the value to be propagated

**Returns:** nothing in particular"
"It does the following:
- reads the path of the `dep-*` node from the `prov-*` node
- create an `arg-*` node at the same task and with the same serial number as the `dep-*` node
- set the value of the `arg-*` node to be `value`
- remove the `dep-*` node"
(fact
 (propagate-result ..zk.. ..prov.. ..value..) => irrelevant
 (provided
  (get-clj-data ..zk.. ..prov..) => "/foo/bar/dep-01472"
  (zk/create ..zk.. "/foo/bar/arg-01472" :persistent? true) => true
  (set-initial-clj-data ..zk.. "/foo/bar/arg-01472" ..value..) => irrelevant
  (zk/delete ..zk.. "/foo/bar/dep-01472") => irrelevant))

[[:section {:title "get-task-from-any-plan"}]]
"
**Parameters:**
- **zk:** the Zookeeper connection object
- **parent:** path to the parent of all plans

**Returns:** path to a task, if one is found, or nil if not"
"It starts by getting the list of children (plans).  If this list is empty, it returns nil"
(fact
 (get-task-from-any-plan ..zk.. ..parent..) => nil
 (provided
  (zk/children ..zk.. ..parent..) => nil))

"If a plan exists, we check that it is ready and then call `get-task` on it"
(fact
 (get-task-from-any-plan ..zk.. "/foo") => ..task..
 (provided
  (zk/children ..zk.. "/foo") => '("bar")
  (zk/exists ..zk.. "/foo/bar/ready") => {:some-key "value"}
  (get-task ..zk.. "/foo/bar") => ..task..))

"If a plan is not ready, it should be skipped"
(fact
 (get-task-from-any-plan ..zk.. "/foo") => ..task..
 (provided
  (zk/children ..zk.. "/foo") => '("bar" "baz")
  (zk/exists ..zk.. "/foo/bar/ready") => nil
  (zk/exists ..zk.. "/foo/baz/ready") => {:some-key "value"}
  (get-task ..zk.. "/foo/baz") => ..task..))

"If a `get-task` does not return a task (e.g., no ready tasks), we move on to the next plan.
This should be done lazily, so that additional plans must not be queried."
(fact
 (get-task-from-any-plan ..zk.. "/foo") => ..task..
 (provided
  (zk/children ..zk.. "/foo") => '("bar" "baz" "quux")
  (zk/exists ..zk.. "/foo/bar/ready") => {:some-key "value"}
  (get-task ..zk.. "/foo/bar") => nil
  (zk/exists ..zk.. "/foo/baz/ready") => {:some-key "value"}
  (get-task ..zk.. "/foo/baz") => ..task..))

[[:chapter {:title "Integration Testing"}]]
[[:section {:title "Stress Test"}]]
"The idea of this test is to stress zk_plan by launching N parallel worker threads (TBD)"

