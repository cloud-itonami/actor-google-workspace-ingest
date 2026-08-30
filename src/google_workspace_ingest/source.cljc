(ns google-workspace-ingest.source
  "Google Workspace を 1 つの source として宣言する。**3 つの cursor 系統を持つ。**

   ここが兄弟 `m365-ingest` と構造的に違う一点である。connector 面では Gmail /
   Drive / Calendar が 3 repo に分かれている（3 API・1 OAuth client）が、importer の
   単位は **API ではなく tenant grant** なので 1 actor になる。3 つに割ると、
   1 つの admin 同意に対して resident が 3 体・gate が 3 枚・governance 記録が
   3 つできる。

   その代わり cursor が 3 種類あり、失効の仕方も 3 通り違う。"
  (:require [importer.cursor :as cursor]
            [importer.model :as im]))

(def source-id "com.google.workspace")

(def governance
  (im/governance {:classification :restricted
                  :pii-tier 3
                  :retention-days 2555
                  :consent-required? true
                  :purpose "compliance:audit + internal:knowledge-extraction"}))

;; Google が公表しているのは『通常は少なくとも 1 週間』という **下界** である。
;; 下界を期限として使うと必要より早く resync する —— 安全な向きに外す方を選ぶ。
;; 上界として使うと、まだ生きている cursor を捨てる代わりに、既に死んだ cursor を
;; 生きているものとして扱う window ができる。
(def gmail-history-floor-ms (* 7 24 60 60 1000))

(def source
  (im/validated
   (-> (im/source source-id "Google Workspace"
                  {:summary "1 tenant の Gmail / Drive / Calendar を、3 系統の cursor で追う。"
                   :origin-domain "google.com"
                   :connector-id "com.google.gmail"
                   :docs-url "https://developers.google.com/workspace/admin"
                   :governance governance})

       (im/add-stream
        :mail
        {:cursor-style :delta-token
         :description "users.history.list の startHistoryId。**ページは id しか返さない。**"
         :history-retention gmail-history-floor-ms
         :resync-on #{:history-too-old :gone}})

       (im/add-stream
        :files
        {:cursor-style :page-token
         :description "changes.list の pageToken。最終ページの newStartPageToken が次回の起点。"
         :history-retention :unknown})

       (im/add-stream
        :calendar
        {:cursor-style :sync-token
         :description "events.list の syncToken。410 で失効する。"
         :history-retention :unknown
         :resync-on #{:gone :sync-token-invalid}}))))

(defn mail-cursor     [mailbox]  (cursor/cursor source :mail mailbox))
(defn files-cursor    [drive-id] (cursor/cursor source :files drive-id))
(defn calendar-cursor [cal-id]   (cursor/cursor source :calendar cal-id))
