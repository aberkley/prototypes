(ns yahoo.parse
  (:require [clojure.string :only [trim replace] :as string])
  (:use [clj-time.format :only [parse unparse formatter]]
        [clj-time.core :only [local-date minus now days day month year hour minute date-time]]))

(defn parse-partial-date
  [s]
  (-> s
      string/trim
      (string/split #" - Close")
      first
      ((partial parse (formatter "MMM dd")))
      ((juxt month day))
      ((partial apply merge [(year (now))]))
      ((partial apply local-date))))

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

(comment
  (parse-google-time "\n9:45am GMT\n")
  )

(defn parse-datum
  [str]
  (let [parse-as (fn [f str] (try (f str)
                                 (catch Exception e nil)))
        date-format (clj-time.format/formatter "MMM dd, yyyy")]
   (or (parse-as (comp bigdec #(string/replace % "," "") string/trim) str)
       (parse-as (comp (partial parse date-format) string/trim) str)
       (parse-as parse-partial-date str)
       (parse-as parse-google-time))))

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
