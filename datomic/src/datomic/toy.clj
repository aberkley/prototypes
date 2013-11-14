(ns datomic.toy
  (:use [datomic.api :only [q db] :as d]
        [clojure.pprint]
        [datomic.schema :as s]))

(def uri "datomic:mem://toy")

(d/delete-database uri)

(d/create-database uri)

(def conn (d/connect uri))

(def schema
  [{:db/id #db/id[:db.part/db]
    :db/ident :a/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db.install/_attribute :db.part/db}])

(def mock-data
  [{:db/id #db/id[:db.part/user -1], :a/name "a"}])

(defn some-local-function
  [name]
  (str name "!"))

(def construct-a-map
  "Returns map that could be added to transaction data to create
a new a, or nil if a exists"
  #db/fn {:lang :clojure
          :requires [[test-datomic.toy :as toy]]
          :params [database id name]
          :code (let [new-name (toy/some-local-function name)
                      exists?  (seq (d/q '[:find ?a
                                           :in $ ?name
                                           :where [?a :a/name ?name]]
                                         database new-name))]
                  (if exists?
                    (throw (Throwable. "a already exists"))
                    [{:db/id id
                      :a/name new-name}]))})

(comment
  (some-local-function "A")

  ;; setup db
  (d/transact conn test-datomic.toy/schema)
  (d/transact conn test-datomic.toy/mock-data)
  ;; send transaction explictly
  (def a (test-datomic.toy/construct-a-map (db conn) #db/id[:db.part/user] "A"))
  (d/transact conn a)
  ;; register transaction function
  (d/transact conn [{:db/id #db/id[:db.part/user]
                     :db/ident :construct-a
                     :db/fn construct-a-map}])

  ;; send with transaction function
  @(d/transact conn [[:construct-a #db/id[:db.part/user] "A?"]])


  (save-entity! a)
  (find-entity (db conn) a)
  (db-fn-save-entity! (db conn) #db/id[:db.part/user] a)


  (def a {:db/id #db/id[:db.part/user] :a/name "a"})
  (find-entities (db conn) a)
  (find-entity (db conn) a)
  (entities-exist? (db conn) {:db/id #db/id[:db.part/user] :a/name "Aa"}))

(comment
  (def test-function-1 #db/fn {:lang :clojure
                               :params [n]
                               :code (+ 1 n)})
  (register-db-function! (db conn) :test-function-1 test-function-1)
  (d/invoke (db conn) :test-function-1 3)

  (def test-function-2 #db/fn {:lang :clojure
                               :params [n]
                               :code (eval `(test-function-1 ~n))})
  (register-db-function! (db conn) :test-function-2 test-function-2)
  (d/invoke (db conn) :test-function-2 3)
  (test-function-2 3)
  ((resolve 'test-function-1) 3)
  )

(comment
  (register-db-function! (db conn) :construct-a construct-a-map)

  (d/q '[:find ?function
         :where [?function :db/ident :construct-a]]
       (db conn))
  (= (:lang construct-a-map) (:lang construct-a-map-2))
  )
