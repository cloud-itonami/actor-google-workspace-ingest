(ns google-workspace-ingest.run
  "取り込み 1 ページ。**gate が先、cursor は最後。**

   兄弟 `cloud-itonami/m365-ingest` と同じ 2 段構え（`step` が effect を返し、
   `land` が sink の報告を受けて初めて cursor を動かす）。1 本にすると『書く前に
   cursor を進める』が書けてしまい、次の delta が二度と触れない穴になる。

   違うのは 3 系統あることだけで、順序は同じである。"
  (:require [google-workspace-ingest.calendar :as calendar]
            [google-workspace-ingest.drive :as drive]
            [google-workspace-ingest.gate :as gate]
            [google-workspace-ingest.gmail :as gmail]
            [google-workspace-ingest.source :as source]
            [importer.cursor :as cursor]
            [importer.plan :as plan]))

(def collection "cloud.itonami.workspace.imported")

(def own-keys
  "封筒の top level に在ってよい key。**これ以外が 1 つでも在れば漏れている。**

   『予約 key を持っていないこと』ではなく『自分の key しか無いこと』を見る ——
   前者は禁止一覧を書き忘れた key を通し、後者は書き忘れようが無い。"
  #{:rkey :source :streamKey :importedAt :imported})

(defn envelope
  "canonical record を 1 段下に落とす。

   取り込むのは **外から届いたデータ**である。top level に merge すると、
   送信者が選べる文字列が actor や gate の主張と同じ場所に並ぶ。兄弟 repo が
   `:record/caller-can-forge-actor-did` として測った穴の、遠隔から踏める版。"
  [c record now]
  {:rkey (str (:importer.cursor/key c) "-"
              (or (:mail/id record) (:file/id record) (:event/id record) "unknown"))
   :source source/source-id
   :streamKey (:importer.cursor/key c)
   :importedAt now
   :imported record})

(defn envelope-problems
  "封筒の top level に居るはずのない key。取り込んだ内容が 1 段上へ漏れていれば
   ここに出る。"
  [e]
  (vec (sort (remove own-keys (keys e)))))

(defn- page-for
  [c response opts]
  (case (:importer.cursor/stream c)
    :mail (gmail/page c response (:fetched opts {}))
    :files (drive/page c response)
    :calendar (calendar/page c response)
    (throw (ex-info "unknown stream" {:stream (:importer.cursor/stream c)}))))

(defn step
  "1 ページ分の計画。**cursor は動かさない。**

   opts: {:cursor :attestations :response :now :fetched}

   gate が通らなければ effect 0 本・`:unmeasured`。`:synced` の 0 件ではない ——
   測れていないのと、測って空だったのは別の答えである。"
  [{:keys [cursor attestations response now] :as opts}]
  (gate/authorize
   attestations
   (fn []
     (let [{:keys [plan tombstones incremental-token fetch]} (page-for cursor response opts)
           records (:importer.plan/items plan)]
       (cond-> {:outcome (:importer.plan/outcome plan)
                :plan plan
                :tombstones (or tombstones [])
                :effects (mapv (fn [r]
                                 (let [e (envelope cursor r now)]
                                   {:op :mst/put-record
                                    :actor "cloud-itonami/actor-google-workspace-ingest"
                                    :collection collection
                                    :rkey (:rkey e)
                                    :record e}))
                               records)}
         incremental-token (assoc :incremental-token incremental-token)
         (seq fetch) (assoc :fetch fetch))))
   {:outcome :unmeasured
    :effects []
    :tombstones []
    :plan (plan/plan {:stream (:importer.cursor/stream cursor)
                      :key (:importer.cursor/key cursor)
                      :items nil :next-token nil :outcome :unmeasured
                      :note "gate blocked"})}))

(defn land
  "sink 自身の報告を受けて cursor を動かす（動かさない判断も含む）。"
  [step-result c sink-report now]
  (plan/commit (:plan step-result) c sink-report now))

(defn resync-needed? [step-result]
  (boolean (:importer.plan/resync-required? (:plan step-result))))

(defn after-resync [c reason now] (cursor/reset c reason now))
