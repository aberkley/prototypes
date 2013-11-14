(ns datomic.schema
  (:use [datomic.api :only [q db] :as d]
        [clojure.pprint]
        [clojure.string :only [split]]))

(def datomic-schema
  [
   ;; prices
   {:db/id #db/id[:db.part/db]
    :db/ident :price/value
    :db/valueType :db.type/bigdec
    :db/cardinality :db.cardinality/one
    :db/doc "numerical value of a price"
    :db.install/_attribute :db.part/db}
   ;;
   {:db/id #db/id[:db.part/db]
    :db/ident :price/time
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one
    :db/doc "instant of a price"
    :db.install/_attribute :db.part/db}
   ;;
   {:db/id #db/id[:db.part/db]
    :db/ident :price/bat
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "bid-ask-trade of a price"
    :db.install/_attribute :db.part/db}
   ;;
   {:db/id #db/id[:db.part/db]
    :db/ident :price/hlocv
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "high-low-open-close-volume of a price"
    :db.install/_attribute :db.part/db}
   ;;
   {:db/id #db/id[:db.part/db]
    :db/ident :price/product
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "product associated with the price"
    :db.install/_attribute :db.part/db}
   {:db/id #db/id[:db.part/db]
    :db/ident :price/source
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc "product name e.g. 'SPX'"
    :db.install/_attribute :db.part/db};;
   [:db/add #db/id[:db.part/user] :db/ident :price.bat/bid]
   [:db/add #db/id[:db.part/user] :db/ident :price.bat/ask]
   [:db/add #db/id[:db.part/user] :db/ident :price.bat/trade]
   ;;
   ;; products
   ;;
   {:db/id #db/id[:db.part/db]
    :db/ident :product/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/doc "product type e.g. future, index"
    :db.install/_attribute :db.part/db}
   {:db/id #db/id[:db.part/db]
    :db/ident :product/name
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/many
    :db/doc "product name e.g. 'SPX'"
    :db.install/_attribute :db.part/db}
   {:db/id #db/id[:db.part/db]
    :db/ident :product/underlying
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/doc "underlying of a derivative"
    :db.install/_attribute :db.part/db}
   ;;
   [:db/add #db/id[:db.part/user] :db/ident :product.type/future]
   [:db/add #db/id[:db.part/user] :db/ident :product.type/index]
   [:db/add #db/id[:db.part/user] :db/ident :product.type/equity]
   [:db/add #db/id[:db.part/user] :db/ident :product.type/option]
   [:db/add #db/id[:db.part/user] :db/ident :product.type/crossrate]
   [:db/add #db/id[:db.part/user] :db/ident :product.type/cash]
   ])

(def extended-schema {:key    {:price   [:price/product :price/time :price/value :price/source]
                               :product [:product/type :product/name]}
                      :unique {:product [:product/name]}})

(defmacro f [macro]
  `(fn [& args#] (eval (cons '~macro args#))))

(defn attribute-root
  [attribute]
  {:pre (keyword? attribute)}
  (-> attribute
      str
      (split #"/")
      first
      (split #":")
      second
      keyword))

(defn consistent?
  [entity]
  (->> (dissoc entity :db/id)
       (map attribute-root)
       (apply =)))

(defn root-attributes
  [entity root-type]
  {:pre [(consistent? entity) (contains? #{:key :unique} root-type)]
   :post (consistent? %)}
  (let [root (-> entity first attribute-root)]
    (-> extended-schema root-type root)))

(defn key-attributes
  [entity]
  (root-attributes entity :key))

(defn unique-attributes
  [entity]
  (root-attributes entity :unique))

(defn sufficient?
  [entity]
  {:pre (consistent? entity)}
  (->> (key-attributes entity)
       (map (partial contains? entity))
       (apply (f and))))

(defn valid?
  [entity]
  (and
   (consistent? entity)
   (sufficient? entity)))

;; how do you deal with the /many cardinality? save-entity doesn't work with product/name as you should be able to set more

;; experimental - want to create functions product?, price? etc. that check consistency and sufficiency
(comment
  (defn define-?s
    ;; ask Edmund - can't get it to work!
    [schema]
    (let [key- (:key schema)]
      (for [[k v] key-]
        (let [fname (-> k name (str "?") symbol)
              fbody  '(+ 1 2)
              ]
          fname))))
  (fname 1)
  (define-?s extended-schema)
  (define (symbol "pricey?") [entity] (+ 3 4))
  (p? 3)
  (price? "s")
  (define-f [price? product?] [entity] (+ 1 2))
  (macroexpand-1 '(define-f [price? product?] [entity] (+ 1 2)))
  `(do
     (defn ~(symbol (str fname "*")) ~args ~@body)))
