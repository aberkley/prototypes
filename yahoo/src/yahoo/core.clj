(ns yahoo.core
  (:require [clojure.pprint :only [pprint]]
            [clojure.string :only [trim replace] :as string]
            [clj-time.format :only [parse unparse]])
  (:use
        [dk.ative.docjure.spreadsheet :as docjure]
        [gloss.io :as g]
        [net.cgrand.enlive-html]
        [clj-time.core :only [local-date minus now days]]
        ))

;; enlive
(defn table
  [url]
  (select
   (html-resource
    (java.net.URL. url))
   [:table]))

(defn select-rows
  [table tag]
  (for [tr (select table [:tr])
        :let  [row (select tr [tag])]
        :when (seq row)]
    row))

(defn parse-datum
  [str]
  (let [parse-as (fn [f str] (try (f str)
                                 (catch Exception e nil)))
        date-format (clj-time.format/formatter "MMM dd, yyyy")]
   (or (parse-as (comp bigdec #(string/replace % "," "") string/trim) str)
       (parse-as (comp (partial parse date-format) string/trim) str))))

(defn parse-row
  "returns row as a map or nil if cannot parse"
  [row]
  (->> row
       (map :content)
       (map first)
       (map parse-datum)))

(defn map-row-to-header
  [header row]
  (if (= (count header) (count row))
    (zipmap header row)))

(defn data
  [table]
  (->> (select-rows table :td)
       (map parse-row)
       (map (partial map-row-to-header (header table)))))

(defn unparse-date
  [dt]
  (let [fmt-1 (formatter "MMM+dd")
        fmt-2 (formatter "+yyyy")
        s     "%2C"]
    (-> ""
        (str (unparse fmt-1 dt))
        (str s)
        (str (unparse fmt-2 dt)))))

(defn historical-url
  [sym start end num]
  (let [stem "https://www.google.co.uk/finance/historical"
        date-format (formatter "MMM+dd%2C+yyyy")]
    (-> stem
        (str "?q=" sym)
        (str "&startdate=" (unparse-date start))
        (str "&enddate=" (unparse-date end))
        (str "&num=" num))))

(defn historical-prices
  [sym start end num]
  (-> sym
      (historical-url start end num)
      table
      data))

(defn recent-daily-prices
  [sym]
  (let [num   10
        end   (minus (now) (days 1))
        start (minus end (days num))]
    (historical-prices sym start end num)))

(defn last-year-daily-prices
  ;; TODO: merge with recent-daily-prices
  [sym]
  (let [num   365
        end   (minus (now) (days 1))
        start (minus end (days num))]
    (historical-prices sym start end num)))

(comment
  (last-year-daily-prices "INDEXFTSE:UKX")
  (recent-daily-prices "INDEXFTSE:UKX")
  (historical-url "INDEXFTSE:UKX" (date-time 2012 11 21) (date-time 2013 11 21) 80)
  "https://www.google.co.uk/finance/historical?q=INDEXFTSE:UKX&startdate=Nov+21%2C+2012&enddate=Nov+22%2C+2013&num=365"
  (ukx-data))

(defn header
  [table]
  (->> (select-rows table :th)
       first
       (map :content)
       (map first)
       (map string/trim)))
