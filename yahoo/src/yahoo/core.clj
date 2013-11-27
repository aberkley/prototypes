(ns yahoo.core
  (:use
   [yahoo.parse]
   [clojure.pprint :only [pprint]]
   [dk.ative.docjure.spreadsheet :as docjure]
   [gloss.io :as g]
   [net.cgrand.enlive-html]))

;; live prices - grabs time/value pairs

(defn live-price
  [time-tags value-tags build-url sym]
  (->> sym
       build-url
       java.net.URL.
       html-resource
       ((juxt
         #(select % time-tags)
         #(select % value-tags)))
       (map (comp first :content first))
       (map parse-datum)
       (zipmap [:price/time :price/value])))

(def live-price-google
  (partial live-price
           [:div#market-data-div :div#price-panel :span.nwp [:span (attr? :id)]]
           [:div#market-data-div :div#price-panel :span.pr [:span (attr? :id)]]
           #(str "https://www.google.co.uk/finance?q=" %)))

(def live-price-bloomberg
  (partial live-price
           [:div#primary_content :div.ticker_header :p.fine_print]
           [:div#primary_content :div.ticker_header :span.price]
           #(str "http://www.bloomberg.com/quote/" %)))

(comment
  (live-price-google "LON:UKX")
  (live-price-bloomberg "UKX:IND"))

;; bloomberg

(defn bb-live-price
  [sym]
  ;; write parser for bb time string
  (->> sym
       bb-snapshot-url
       java.net.URL.
       html-resource
       ((juxt bb-live-time bb-live-value))
       (map (comp first :content first))
       (map parse-datum)
       (zipmap [:price/time :price/value])))

;; enlive

(defn select-rows
  [table tag]
  (for [tr (select table [:tr])
        :let  [row (select tr [tag])]
        :when (seq row)]
    row))

(defn header
  [html-]
  (-> (select html- [:table])
      (select-rows :th)
      first
      (map :content)
      (map first)
      (map string/trim)))

(defn map-row-to-header
  [header row]
  (if (= (count header) (count row))
    (zipmap header row)))

(defn data
  [html-]
  (->> (select html- [:table])
       (select-rows table :td)
       (map parse-row)
       (map (partial map-row-to-header (header table)))))

(defn historical-url
  [sym start end num]
  (let [stem "https://www.google.co.uk/finance/historical"]
    (-> stem
        (str "?q=" sym)
        (str "&startdate=" (unparse-date start))
        (str "&enddate=" (unparse-date end))
        (str "&num=" num))))

(defn historical-prices
  [sym start end num]
  (-> sym
      (historical-url start end num)
      live-price-url
      java.net.URL.
      html-resource
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


;; bb members

(defn bb-members-url
  [sym]
  (-> sym
      (bb-snapshot-url)
      (str "/members")))

(defn members-row-values
  [tr]
  (let [names  (-> tr
                   (select [[:td (attr= :class "first name")] :a])
                   ((partial (juxt (comp first :content first)
                                   (comp :href :attrs first))))
                   ((partial into #{})))
        others (-> tr
                   (select [[:td (but (attr= :class "first name"))]])
                   ((partial map (comp first :content))))]
    (-> [names]
        ((partial apply merge) others))))

(defn members-values
  [html-]
  (-> html-
      (select [:table.index_members_table :tbody :tr])
      ((partial map members-row-values))))

(defn members-header
  [html-]
  (let [thead  (select html- [:table.index_members_table :thead])
        others (select thead [[:th (but (attr= :class "datetime"))] :span])
        time   (select thead [[:th (attr= :class "datetime")]])]
    (->> others
         (merge time)
         flatten
         ((partial map (comp first :content))))))

(defn members-data
  [sym]
  (let [html- (-> sym
                  bb-members-url
                  java.net.URL.
                  html-resource)
        values (members-values html-)
        header (members-header html-)]
    (-> header
        ((partial repeat (count values)))
         (zipmap values))))

(defn members-data
  [sym]
  (let [html- (-> sym
                  bb-members-url
                  java.net.URL.
                  html-resource)
        values (members-values html-)
        header (members-header html-)]
    (->> header
         (repeat (count values))
         (map #(zipmap %2 %1) values))))

(comment
  (members-data "UKX:IND")
  )
