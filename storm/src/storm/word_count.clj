(ns storm.word-count
  (:import [backtype.storm StormSubmitter LocalCluster])
  (:use [backtype.storm clojure config]
        [clj-time.core :only [date-time plus secs millis] :as clj-time]
        [clojure.pprint]
        [clojure.math.numeric-tower])
  (:gen-class))

;; client-spout - reads a schema for each client and emits reporting requirements
;; reporting-spec-bolt - joins client-spout and other spouts/bolts that trigger dynamic reporting
;; BB-web-price-bolt - emits prices
;; IB-spout - emits prices, transactions, etc.
;; price-bolt - joins various data-feeds and handles persistence, regulates and emits prices

;; transaction-bolt - joins various clint/PB feeds, handles persistence, performs reconciliation, emits transactions
;; position-bolt
;; pnl-bolt - joins transaction bolt

(defn random-step
  "Produce a new value based on the previous value and a step size."
  [dp dt price]
  {:price/value (+ (:price/value price) (rand-nth [dp (- dp)]))
   :price/time  (plus (:price/time price) dt)})

(defn random-walk
  "Produce a random walk sequence beginning with the initial value and stepping up or down by the step value."
  [price dp dt]
  (iterate (partial random-step dp dt) price))

(defn random-transaction
  [dp dt transaction]
  {:transaction/units (* dp (rand-nth [-1, 0, 1]))
   :transaction/time  (plus (:transaction/time transaction) dt)})

(defn random-transactions
  [transaction dp dt]
  (iterate (partial random-transaction dp dt) transaction))

(defn transaction?
  [entity]
  (and
   (contains? entity :transaction/units)
   (contains? entity :transaction/time)))

(defn price?
  [entity]
  (and
   (contains? entity :price/value)
   (contains? entity :price/time)))

(defspout price-feed-spout ["price"]
  [conf context collector]
  (let [interval-size 100
        price-feed (atom (random-walk
                          {:price/time (date-time 2013 1 1) :price/value (bigdec 1)}
                          0.00001
                          (millis interval-size)))]
    ;; spout is a macro that reifies the ISpout interface.
    ;; It has the operations open, close, nextTuple, ack, and fail.
    (spout
     ;; nextTuple should emit the next tuple in the data stream
     (nextTuple []
                (Thread/sleep interval-size)
                (let [price (first @price-feed)]
                  (swap! price-feed rest)
                  (emit-spout! collector [price]))))))

(defspout transaction-feed-spout ["transaction"]
  [conf context collector]
  (let [interval-size 1000
        transaction-feed (atom (random-transactions
                                {:transaction/time (date-time 2013 1 1)
                                 :transaction/units 0}
                                1
                                (millis interval-size)))]
    ;; spout is a macro that reifies the ISpout interface.
    ;; It has the operations open, close, nextTuple, ack, and fail.
    (spout
     ;; nextTuple should emit the next tuple in the data stream
     (nextTuple []
                (Thread/sleep interval-size)
                (let [transaction (first @transaction-feed)]
                  (swap! transaction-feed rest)
                  (emit-spout! collector [transaction]))))))

(defbolt mark-position-bolt ["mark"] {:prepare true}
  [conf context collector]
  (let [units (atom 0)
        latest-price (atom nil)]
   (bolt
     (execute
      [tuple]
      (let [entity (.getValue tuple 0)
            mult 50]
        ;; update state
        (cond
         (price? entity)       (reset! latest-price entity)
         (transaction? entity) (swap! units (fn [u t] (+ u (:transaction/units t))) entity))
        ;; emit mark based on state
        (if @latest-price
          (emit-bolt! collector [{:mark/value (* (:price/value @latest-price) @units mult)
                                  :mark/time (:price/time @latest-price)}] :anchor tuple
                      ))
        (ack! collector tuple))))))

(def marks (atom []))

(defn square
  [x]
  (* x x))

(defn mean
  [series]
  (/ (reduce + series)
     (count series)))

(defn st-dev
  [series]
  (let [sum-squares (->> series
                         (map square)
                         mean)
        square-sum  (->> series
                         mean
                         square)]
    (sqrt (- sum-squares square-sum))))

(defbolt risk-metric-bolt ["st-dev" "count"] {:prepare true}
  [conf context collector]
  ;; can actually put state here since :prepare is true
  (bolt
   (execute
    [tuple]
    (let [mark (.getValue tuple 0)]
      (swap! marks merge mark)
      (emit-bolt! collector [;;(->> @marks  (map :mark/value) st-dev) (count @marks)
                             "2" "3"] :anchor tuple
                  )
      (ack! collector tuple)))))

(defn mk-topology []
  (topology
   {"1" (spout-spec price-feed-spout)
    "2" (spout-spec transaction-feed-spout)}
   {"3" (bolt-spec {"1":shuffle "2" :shuffle}
                   mark-position-bolt
                   :p 5)
    "4" (bolt-spec {"3" ["mark"]}
                   risk-metric-bolt
                   :p 6)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "word-count" {TOPOLOGY-DEBUG true} (mk-topology))
    (Thread/sleep 5000)
    (.shutdown cluster)))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
   name
   ;; optionally submit custom Kyro serializers
   {TOPOLOGY-DEBUG true
    TOPOLOGY-WORKERS 3}
   (mk-topology)))

(defn -main
  ([]
   (run-local!))
  ([name]
   (submit-topology! name)))

(comment
  ;; mark
  @marks
  (run-local!)
  )
