(ns storm.core
  (:import [backtype.storm StormSubmitter LocalCluster])
  (:use [backtype.storm clojure config]
        [clj-time.core :only [date-time plus secs] :as t]
        [clojure.math.numeric-tower])
  (:gen-class))

(def spx-price (atom {:price/value (bigdec 1000) :price/time (t/date-time 2013 1 1)}))

(defn next-price
  [{value :price/value time :price/time :as price}]
  {:time  (t/plus time (t/secs 1))
   :value (+ value (rand-nth [1 -1]))})

(defn update-price!
  [price]
  (swap! price next-price))

(defspout price-spout ["price"]
  [conf context collector]
  (spout
   (nextTuple []
              (Thread/sleep 100)
              (emit-spout! collector [(str (update-price! spx-price))])
              )
   (ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        )))

(defbolt mark ["mark"] [tuple collector]
  (let [units 2
        mult 50
        tuple-value (.getValueByField tuple "price")
        price (:price/value tuple)
        time  (:price/time tuple)
        value (* units mult price)]
    (emit-bolt! collector [{:mark/time time :mark/value value}] :anchor tuple)
    (ack! collector tuple)))

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

(def risks (atom []))

(defn update-risks!
  [risk]
  (swap! merge risk))

(defbolt risk ["risk"] {:prepare true}
  [conf context collector]
  (let [marks (atom [])]
    (bolt
     (execute [tuple]
              (let [mark (.getValueByField tuple "mark")]
         (swap! marks merge mark)
         (emit-bolt! collector [st-dev (->> @marks (map :mark/value))] :anchor tuple)
         (ack! collector tuple)
         )))))

(defn make-topology
  []
  (topology
   {"1" (spout-spec price-spout)}
   {"2" (bolt-spec {"1" :shuffle}
                   mark
                   :p 5)
    "3" (bolt-spec {"2" :shuffle}
                   risk
                   :p 6)}))

(defn run-local! []
  (let [cluster (LocalCluster.)]
    (.submitTopology cluster "mark positions" {TOPOLOGY-DEBUG true} (make-topology))
    (Thread/sleep 10000)
    (.shutdown cluster)))

(comment
  (run-local!)
  @risks)
