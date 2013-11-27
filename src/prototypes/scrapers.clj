(ns prototypes.scrapers
  (:use
   [prototypes.format]
   [clojure.pprint :only [pprint]]
   [net.cgrand.enlive-html]
   [clj-time.core :only [minus days now]]))

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
  (live-price-google "INDEXFTSE:UKX")
  (live-price-bloomberg "UKX:IND"))

(defn select-rows
  [table tag]
  (for [tr (select table [:tr])
        :let  [row (select tr [tag])]
        :when (seq row)]
    row))

(defn google-hist-header
  [html-]
  (-> (select html- [:table])
      (select-rows :th)
      first
      ((partial map :content))
      ((partial map first))))

(defn google-hist-data
  [html-]
  (-> html-
      (select [:table])
       (select-rows :td)
       ((partial map parse-row))))

(defn google-hist-url
  [end num sym]
  (let [stem "https://www.google.co.uk/finance/historical"]
    (-> stem
        (str "?q=" sym)
        (str "&startdate=" (unparse-date (minus end (days num))))
        (str "&enddate=" (unparse-date end))
        (str "&num=" num))))

(defn map-table-data
  [table-header table-values build-url sym]
  (let [html- (-> sym
                  build-url
                  java.net.URL.
                  html-resource)
        values (table-values html-)
        header (table-header html-)]
    (->> header
         (repeat (count values))
         (map #(zipmap %2 %1) values))))

(def google-recent-price-data
  (partial map-table-data
           google-hist-header
           google-hist-data
           (partial google-hist-url (minus (now) (days 1)) 10)))

(comment
  (google-recent-price-data "INDEXFTSE:UKX")
  )

;; bb members

(defn bb-members-url
  [sym]
  (str "http://www.bloomberg.com/quote/" sym "/members"))

(defn members-row-values
  [tr]
  (let [names  (-> tr
                   (select [[:td (attr= :class "first name")] :a])
                   ((partial (juxt (comp first :content first)
                                   (comp :href :attrs first)))))
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

(def map-members-data
  (partial map-table-data
           members-header
           members-values
           bb-members-url))

(comment
  (map-members-data "UKX:IND")
  )
