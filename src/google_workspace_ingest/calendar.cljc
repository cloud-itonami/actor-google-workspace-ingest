(ns google-workspace-ingest.calendar
  "Calendar の events を page にする。純関数、credential 無し。

   syncToken は 410 で失効する。それは事故ではなく provider の正常な返答で、
   source が `:resync-on #{:gone :sync-token-invalid}` として名前を挙げてある ——
   例外にすると、計画された経路が障害として記録される。

   `status: \"cancelled\"` の event は tombstone。sync 応答の中に混じって来る。"
  (:require [kotoba.lang.text :as str]
            [importer.normalize :as n]
            [importer.plan :as plan]))

(def base-url "https://www.googleapis.com/calendar/v3")

(defn- g [m & ks] (some #(or (get m %) (get m (name %)) (get m (keyword %))) ks))

(defn events-request
  [c {:keys [calendar-id page-size]}]
  (let [cal (or calendar-id (:importer.cursor/key c))]
    {:connector.http/method :get
     :connector.http/url (str base-url "/calendars/" cal "/events")
     :connector.http/query (cond-> {}
                             (:importer.cursor/token c) (assoc "syncToken" (str (:importer.cursor/token c)))
                             page-size (assoc "maxResults" (str page-size)))}))

(defn cancelled? [e] (= "cancelled" (str (g e :status))))

(defn- when-ms [x]
  (let [s (or (g x :dateTime) (g x :date))]
    (when-not (str/blank? (str s))
      #?(:clj (try (.toEpochMilli (java.time.Instant/parse (str s))) (catch Exception _ nil))
         :cljs (let [n (js/Date.parse (str s))] (when-not (js/isNaN n) n))))))

(defn- person
  "Google の recipient は {\"email\" ..., \"displayName\" ...}。Graph は
   {\"emailAddress\" {\"address\" ..., \"name\" ...}}。**別の形なので、
   address と表示名を取り出してから正規化に渡す。** 片方だけ渡すと表示名が
   静かに落ちる（実測: 最初はそう書いていて test が落ちた）。"
  [x]
  (when (map? x)
    (when-let [addr (or (get x "email") (get x :email))]
      (let [nm (or (get x "displayName") (get x :displayName))]
        (n/address (if (str/blank? (str nm)) (str addr) (str nm " <" addr ">")))))))

(defn event
  [cal-id e]
  (cond-> {:event/id (str "gcal:" (g e :id))
           :event/calendar cal-id}
    (g e :summary) (assoc :event/title (g e :summary))
    (when-ms (g e :start)) (assoc :event/start (when-ms (g e :start)))
    (when-ms (g e :end)) (assoc :event/end (when-ms (g e :end)))
    ;; Google の recipient は {"email" ..., "displayName" ...}。Graph の
    ;; {"emailAddress" {...}} とは別の形なので、address を先に取り出して渡す。
    (person (g e :organizer)) (assoc :event/organizer (person (g e :organizer)))
    (seq (keep person (g e :attendees)))
    (assoc :event/attendees (vec (distinct (keep person (g e :attendees)))))))

(defn page
  [c response]
  (let [status (:connector.http/status response)
        body (:connector.http/body response)
        stream (:importer.cursor/stream c)
        k (:importer.cursor/key c)
        mk (fn [m] (plan/plan (merge {:stream stream :key k :items nil :next-token nil} m)))]
    (cond
      (= 410 status)
      {:plan (mk {:outcome :failed :resync-required? true
                  :note "the calendar syncToken is no longer valid"})
       :tombstones []}

      (or (nil? status) (= 429 status) (and (number? status) (>= status 500)))
      {:plan (mk {:outcome :unmeasured
                  :note (str "Calendar answered " (pr-str status) "; nothing was read")})
       :tombstones []}

      (not= 200 status)
      {:plan (mk {:outcome :failed :note (str "Calendar answered " status)}) :tombstones []}

      :else
      (let [items (or (g body :items) [])
            {dead true live false} (group-by cancelled? items)
            next-token (g body :nextPageToken)
            sync-token (g body :nextSyncToken)]
        (cond-> {:plan (plan/plan {:stream stream :key k
                                   :items (mapv #(event k %) live)
                                   :next-token next-token :outcome :synced})
                 :tombstones (mapv (fn [e] {:tombstone/provider-id (str "gcal:" (g e :id))
                                            :tombstone/reason "cancelled"})
                                   dead)}
          (and (nil? next-token) sync-token) (assoc :incremental-token (str sync-token)))))))
