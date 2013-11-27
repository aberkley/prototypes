(ns prototypes.format
  (:require [clojure.string :only [trim replace] :as string])
  (:use [clj-time.format :only [parse unparse formatter]]
        [clj-time.core :only [minus now days day month year hour minute date-time]]))

(defn parse-partial-date
  [s]
  (-> s
      string/trim
      (string/split #" - Close")
      first
      ((partial parse (formatter "MMM dd")))
      ((juxt month day))
      ((partial apply merge [(year (now))]))
      ((partial apply date-time))))

(defn parse-google-time
  [s]
  (let [dt (now)]
    (-> s
        string/trim
        (string/split #" GMT")
        first
        ((partial parse (formatter "h:mma")))
        ((juxt hour minute))
        ((partial apply merge [(year dt) (month dt) (day dt)]))
        ((partial apply date-time)))))

(defn parse-bloomberg-time
  [s]
  (-> s
      string/trim
      (string/split #"As of")
      last
      string/trim
      (string/split #"ET on")
      ((partial map string/trim))
      reverse
      ((partial apply str))
      ((partial parse (formatter "MM/dd/yyy.hh:mm:ss")))))

(comment
  (parse-google-time "\n9:45am GMT\n")
  (parse-bloomberg-time "As of\n        \n            06:00:38 ET on\n        \n        11/27/2013.")
  )

(defn parse-datum
  [str]
  (let [try-nil (fn [f str] (try (f str)
                                 (catch Exception e nil)))
        date-format (clj-time.format/formatter "MMM dd, yyyy")]
   (or (try-nil (comp bigdec #(string/replace % "," "") string/trim) str)
       (try-nil (comp (partial parse date-format) string/trim) str)
       (try-nil parse-partial-date str)
       (try-nil parse-google-time str)
       (try-nil parse-bloomberg-time str))))

(defn parse-row
  "returns row as a map or nil if cannot parse"
  [row]
  (->> row
       (map :content)
       (map first)
       (map parse-datum)))

(defn unparse-date
  [dt]
  (let [fmt-1 (formatter "MMM+dd")
        fmt-2 (formatter "+yyyy")
        s     "%2C"]
    (-> ""
        (str (unparse fmt-1 dt))
        (str s)
        (str (unparse fmt-2 dt)))))
