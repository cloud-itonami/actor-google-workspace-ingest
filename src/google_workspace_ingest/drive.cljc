(ns google-workspace-ingest.drive
  "Drive の changes を page にする。純関数、credential 無し。

   Drive は 2 種類のトークンを返す。**同じ場所には来ない。**

     nextPageToken       この run にまだ続きがある
     newStartPageToken   この run は終わり。次回はここから

   Graph の nextLink / deltaLink と同じ形で、間違え方も同じ —— 片方を『次の
   トークン』としてまとめると、途中のページで backfill が終わったことになり、
   残りは二度と読まれない。

   metadata だけを扱う。**本文（バイト）はここを通らない**: content は block 面に
   直行し、datom 面には `:file/blob` の参照だけが載る（kotobase の L0/L1 規則）。"
  (:require [clojure.string :as str]
            [importer.plan :as plan]))

(def base-url "https://www.googleapis.com/drive/v3")

(defn- g [m & ks] (some #(or (get m %) (get m (name %)) (get m (keyword %))) ks))

(defn changes-request
  [c {:keys [page-size]}]
  {:connector.http/method :get
   :connector.http/url (str base-url "/changes")
   :connector.http/query (cond-> {"fields" "nextPageToken,newStartPageToken,changes(fileId,removed,file(id,name,mimeType,modifiedTime,owners(emailAddress,displayName)))"}
                           (:importer.cursor/token c) (assoc "pageToken" (str (:importer.cursor/token c)))
                           page-size (assoc "pageSize" (str page-size)))})

(defn start-token-request []
  {:connector.http/method :get
   :connector.http/url (str base-url "/changes/startPageToken")})

(defn removed? [ch] (boolean (or (g ch :removed) (nil? (g ch :file)))))

(defn file
  [ch]
  (let [f (g ch :file)
        owner (first (or (g f :owners) []))]
    (cond-> {:file/id (str "gdrive:" (or (g f :id) (g ch :fileId)))}
      (g f :name) (assoc :file/name (g f :name))
      (g f :mimeType) (assoc :file/media-type (g f :mimeType))
      (g f :modifiedTime) (assoc :file/modified-at (g f :modifiedTime))
      owner (assoc :file/owner
                   (cond-> {:person/address (some-> (g owner :emailAddress) str/lower-case)}
                     (g owner :displayName) (assoc :person/name (g owner :displayName)))))))

(defn page
  [c response]
  (let [status (:connector.http/status response)
        body (:connector.http/body response)
        stream (:importer.cursor/stream c)
        k (:importer.cursor/key c)
        mk (fn [m] (plan/plan (merge {:stream stream :key k :items nil :next-token nil} m)))]
    (cond
      (or (nil? status) (= 429 status) (and (number? status) (>= status 500)))
      {:plan (mk {:outcome :unmeasured
                  :note (str "Drive answered " (pr-str status) "; nothing was read")})
       :tombstones []}

      (= 410 status)
      {:plan (mk {:outcome :failed :resync-required? true
                  :note "the Drive page token is no longer valid"})
       :tombstones []}

      (not= 200 status)
      {:plan (mk {:outcome :failed :note (str "Drive answered " status)}) :tombstones []}

      :else
      (let [changes (or (g body :changes) [])
            {dead true live false} (group-by removed? changes)
            next-token (g body :nextPageToken)
            start-token (g body :newStartPageToken)]
        (cond-> {:plan (plan/plan {:stream stream :key k :items (mapv file live)
                                   :next-token next-token :outcome :synced})
                 :tombstones (mapv (fn [ch] {:tombstone/provider-id (str "gdrive:" (g ch :fileId))
                                             :tombstone/reason "removed"})
                                   dead)}
          (and (nil? next-token) start-token) (assoc :incremental-token (str start-token)))))))
