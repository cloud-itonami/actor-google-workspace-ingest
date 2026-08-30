(ns google-workspace-ingest.gmail

  "Gmail の history を page にする。純関数、credential 無し。

   ## history は id しか返さない —— これが Graph との一番大きな違い

   `users.history.list` が返すのは message の **stub**（id / threadId / labelIds）で
   あって、header も本文も無い。`com-google-gmail` の normalizer が同じ事実を
   『ids only; call gmail_get_message for headers』と書いているのと同じ形である。

   だから page は 2 つの入力を取る: history の応答と、**既に取得済みの message**。
   取得できていない id が 1 つでも残っていれば outcome は `:unmeasured` になり、
   cursor は動かない。ここを『取れた分だけ :synced』にすると、取れなかった message
   は次の history にも現れず（historyId が進むため）、**恒久的に消える**。

   ## 削除は history の中に混じっている

   `messagesDeleted` は record ではなく tombstone。無視すれば retention に答えられず、
   record として扱えば削除されたメールが corpus に残る。"
  (:require [clojure.string :as str]
            [importer.normalize :as n]
            [importer.plan :as plan]))

(def base-url "https://gmail.googleapis.com/gmail/v1")

(defn- g [m & ks] (some #(or (get m %) (get m (name %)) (get m (keyword %))) ks))

(defn history-request
  [c {:keys [mailbox page-size]}]
  {:connector.http/method :get
   :connector.http/url (str base-url "/users/" (or mailbox (:importer.cursor/key c)) "/history")
   :connector.http/query (cond-> {}
                           (:importer.cursor/token c) (assoc "startHistoryId" (str (:importer.cursor/token c)))
                           page-size (assoc "maxResults" (str page-size)))})

(defn message-request
  [{:keys [mailbox id]}]
  {:connector.http/method :get
   :connector.http/url (str base-url "/users/" mailbox "/messages/" id)
   ;; metadata が既定なのは `com-google-gmail` と同じ理由 —— 検索の副作用で
   ;; 本文を丸ごと引くのは高くつく驚きである。本文が要る run は明示的に頼む。
   :connector.http/query {"format" "metadata"}})

(defn- header [msg name*]
  (let [hs (or (g (g msg :payload) :headers) [])]
    (some (fn [h] (when (= (str/lower-case (str (g h :name))) (str/lower-case name*))
                    (g h :value)))
          hs)))

(defn- parse-ms [s]
  (when-not (str/blank? (str s))
    #?(:clj (try (Long/parseLong (str s)) (catch Exception _ nil))
       :cljs (let [n (js/parseInt (str s) 10)] (when-not (js/isNaN n) n)))))

(defn message
  "取得済みの Gmail message を canonical mail にする。"
  [msg]
  (n/mail {:message-id (header msg "Message-ID")
           :cid (g msg :id)
           :thread (g msg :threadId)
           :subject (header msg "Subject")
           :at (some-> (g msg :internalDate) str parse-ms)
           :from (header msg "From")
           :to (some-> (header msg "To") (str/split #",\s*"))
           :cc (some-> (header msg "Cc") (str/split #",\s*"))
           :labels (g msg :labelIds)
           :snippet (g msg :snippet)}))

(defn added-ids
  "この history ページが「取ってこい」と言っている message id。"
  [body]
  (->> (or (g body :history) [])
       (mapcat #(or (g % :messagesAdded) []))
       (keep #(g (g % :message) :id))
       distinct vec))

(defn deleted-ids
  [body]
  (->> (or (g body :history) [])
       (mapcat #(or (g % :messagesDeleted) []))
       (keep #(g (g % :message) :id))
       distinct vec))

(defn page
  "response + 取得済み message -> {:plan :tombstones :fetch :incremental-token}

   `fetched` は `{id -> message}`。`:fetch` に載っている id がそこに全部無ければ
   `:unmeasured` を返す —— 取れなかった message は historyId が進んだ後には
   二度と現れない。"
  ([c response] (page c response {}))
  ([c response fetched]
   (let [status (:connector.http/status response)
         body (:connector.http/body response)
         stream (:importer.cursor/stream c)
         k (:importer.cursor/key c)
         mk (fn [m] (plan/plan (merge {:stream stream :key k :items nil :next-token nil} m)))]
     (cond
       ;; startHistoryId が古すぎる。source が :history-too-old として名前を挙げてある。
       (= 404 status)
       {:plan (mk {:outcome :failed :resync-required? true
                   :note "startHistoryId is older than Gmail keeps history; this mailbox needs a full read"})
        :tombstones [] :fetch []}

       (or (nil? status) (= 429 status) (and (number? status) (>= status 500)))
       {:plan (mk {:outcome :unmeasured
                   :note (str "Gmail answered " (pr-str status) "; nothing was read")})
        :tombstones [] :fetch []}

       (not= 200 status)
       {:plan (mk {:outcome :failed :note (str "Gmail answered " status)})
        :tombstones [] :fetch []}

       :else
       (let [want (added-ids body)
             have (vec (keep #(get fetched %) want))
             short (- (count want) (count have))
             records (mapv message have)
             next-token (g body :nextPageToken)
             history-id (g body :historyId)]
         (cond-> {:fetch want
                  :tombstones (mapv (fn [id] {:tombstone/provider-id id :tombstone/reason "deleted"})
                                    (deleted-ids body))
                  :plan (if (pos? short)
                          (mk {:outcome :unmeasured
                               :note (str short " of " (count want)
                                          " messages named by this history page were not fetched;"
                                          " advancing would lose them permanently")})
                          (plan/plan {:stream stream :key k :items records
                                      :next-token next-token :outcome :synced
                                      :watermark (n/watermark-of records)}))}
           ;; nextPageToken が無い = この run は終わり。次回の起点は historyId。
           (and (nil? next-token) history-id (zero? short))
           (assoc :incremental-token (str history-id))))))))
