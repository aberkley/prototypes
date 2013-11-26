(ns yahoo.core
  (:require
            [clojure.string :only [trim replace] :as string]
            [clj-time.format :only [parse unparse]])
  (:use
   [clojure.pprint :only [pprint]]
        [dk.ative.docjure.spreadsheet :as docjure]
        [gloss.io :as g]
        [net.cgrand.enlive-html]
        [clj-time.core :only [local-date minus now days day month year]]
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

(defn parse-partial-date
  [s]
  (-> s
      string/trim
      (#(string/split % #" - Close"))
      first
      ((partial parse (formatter "MMM dd")))
      ((juxt month day))
      ((partial apply merge [(year (today))]))
      ((partial apply local-date))))

(defn parse-datum
  [str]
  (let [parse-as (fn [f str] (try (f str)
                                 (catch Exception e nil)))
        date-format (clj-time.format/formatter "MMM dd, yyyy")]
   (or (parse-as (comp bigdec #(string/replace % "," "") string/trim) str)
       (parse-as (comp (partial parse date-format) string/trim) str)
       (parse-as parse-partial-date str))))

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


;; live google prices

(defn live-price-url
  [sym]
  (str "https://www.google.co.uk/finance?q=" sym))

(defn live-price-value
  [html-]
  (-> html-
      (select [:div#market-data-div
               :div#price-panel
               :span.pr
               [:span (attr? :id)]])
      first
      :content
      first))

(defn live-price-time
  [html-]
  (-> html-
      (select [:div#market-data-div
               :div#price-panel
               :span.nwp])
      first
      :content
      first))

(defn live-price
  [sym]
  (->> sym
       live-price-url
       java.net.URL.
       html-resource
       ((juxt live-price-time live-price-value))
       (map parse-datum)
       (zipmap [:price/time :price/value])))

(comment
  (live-price "LON:SPX"))

;; bloomberg


(defn bb-snapshot-url
  [sym]
  (str "http://www.bloomberg.com/quote/" sym))

(defn bb-live-price-html
  [html-]

  )

(defn bb-live-value
  [html-]
  (-> html-
      (select [:div#primary_content :div.ticker_header :span.price])))

(defn bb-live-time
  [html-]
  (-> html-
      (select [:div#primary_content :div.ticker_header :p.fine_print])))

(defn bb-live-price
  [sym]
  ;; write parser for bb time string
  (-> sym
      bb-snapshot-url
      java.net.URL.
      html-resource
      ((juxt bb-live-time bb-live-value))
      ((partial map (comp first :content first)))
      ((partial map parse-datum))
      (zipmap [:price/time :price/value])))

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

(def herd-snippet "<form action=\"/herd_changes\" method=\"post\" id=\"animal_addition_form\">
    <table>
        <tr class=\"per_animal_row\">
            <td>
                <input type=\"text\" class=\"true_name\" name=\"true_name\"/>
            </td>
            <td>
                <select class=\"species\" name=\"species\">
                    <option selected=\"selected\">Bovine</option>
                    <option>Equine</option>
                </select>
            </td>
            <td>
                <input type=\"text\" class=\"extra_display_info\" name=\"extra_display_info\"/>
            </td>
        </tr>
        <tr>
          <td colspan=\"3\" style=\"text-align: center\">
            <input type=\"submit\" value=\"Make the Change\"/>
          </td>
        </tr>
    </table>
</form>")

(def herd (html-snippet herd-snippet))

(comment
  (pprint herd)
  (select herd [[:tr (attr= :class "per_animal_row")]])




  )


(comment
  (select [{:tag :a
            :attrs {:href "/quote/BA+:LN"
                    :class "some_class"}
            :content ["BAE Systems PLC"]}
           {:tag :a
            :attrs {
                    :class "some__otherclass"}
            :content ["Some other company"]}]
          [(attr? :class)]))



(comment

  (select
   [{:tag :td,
     :attrs {:class "first_name"},
     :content
     ["\n          "
      {:tag :a, :attrs {:href "/quote/AMEC:LN"}, :content ["AMEC PLC"]}
      "\n        "]}
    {:tag :td,
     :attrs {:class "value"},
     :content ["1.234"]}]
   [[:td (but (attr= :class "value"))]])
  )
