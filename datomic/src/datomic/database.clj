(ns datomic.database
  (:use [datomic.api :only [q db] :as d]
        [clojure.pprint]
        [datomic.schema :as s]))

(def uri "datomic:mem://database")

(d/delete-database uri)

(d/create-database uri)

(def conn (d/connect uri))

@(d/transact conn s/datomic-schema)

(defn db-fn= [db-fn1 db-fn2]
  (->> [db-fn1 db-fn2]
       (map (juxt :lang :params :code))
       (apply (partial map =))
       (apply (f and))))

(defn register-db-function!
  [database name db-function]
  "register a database function if it has changed"
  {:pre (keyword? name)}
  (let [query    '[:find ?body
                   :in $ ?name
                   :where [?entity :db/ident ?name]
                          [?entity :db/fn ?body]]
        fn-in-db (ffirst (d/q query database name))
        update?  (not (db-fn= fn-in-db db-function))]
    (if update? (d/transact conn [{:db/id #db/id[:db.part/user]
                                   :db/ident name
                                   :db/fn db-function}]))))

(defn entity?
  [entity]
  (try (contains? entity :db/id)
       (catch Throwable t
         false)))

(defn decorate-entity
  ;; re-write this so that the conditional is first then map whole structure to recursive call
  [database entity]
  {:pre (entity? entity)}
  (let [explicit-map (zipmap (keys entity) (vals entity))]
    (->> explicit-map
         (into [])
         (map (fn [[k v]] [k (if (entity? v)
                              (->> (d/entity database (:db/id v))
                                   (decorate-entity database))
                              v)]))
         (into {}))))

(defn find-entity-ids
  [database entity]
  (let [query (->> (dissoc entity :db/id)
                   (into [])
                   (map (fn [[k v]] ['?entity k v]))
                   (into [:find '?entity :where]))]
    (d/q query database)))

(defn entities-exist?
  [database entity]
  (if (seq (find-entity-ids database entity)) true false))

(defn find-entities
  [database entity]
  "searches db based on complete or partial entity"
  (->> [database entity]
       (apply find-entity-ids)
       (map first)
       (map (partial d/entity database))
       (map (partial decorate-entity database))))

(defn find-entity
  [database entity]
  (let [e (find-entities database entity)
        n (count e)]
    (cond
     (= n 1) (first e)
     (= n 0) nil
     :else (throw (Exception. "found multiple entities")))))

(defn unique-constrained?
  [database entity]
  (let [uniques (->> entity s/unique-attributes (into #{}))]
    (->> entity
         (into [])
         (filter (fn [[k v]] (contains? uniques k)))
         (map (fn [[k v]] {k v}))
         (map (partial find-entity database))
         (map (partial = nil))
         (apply (s/f and))
         not)))

(def entity-transaction
  #db/fn {:lang   :clojure
          :requires [[test-datomic.database :as test]]
          :params [database entity]
          :code   (cond
                   (test/find-entity database entity)
                     (throw (Exception. "entity already exists"))
                   (test/unique-constrained? database entity)
                     (throw (Exception. "violates unique constraint"))
                   :else
                     [entity])})

(comment
  (def spx-equity {:product/name "SPX" :product/type :product.type/equity})
  (def spx-index {:product/name "SPX" :product/type :product.type/index})
  (s/consistent? spx-equity)
  (find-entity (db conn) spx-equity)
  (unique-constrained? (db conn) spx-equity)
  (entity-transaction (db conn) spx-equity)
  (save-entity! spx-equity)
  )

(defn save-entity!
  ;; want this to be transactional so must be within a transaction function!
  [entity]
  (let [entity-with-id (assoc entity :db/id #db/id[:db.part/user])]
    (d/transact conn [[:save-entity! entity-with-id]])))

(register-db-function! (db conn) :save-entity! entity-transaction)
