(ns google-workspace-ingest.ingest-test
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [google-workspace-ingest.calendar :as calendar]
            [google-workspace-ingest.drive :as drive]
            [google-workspace-ingest.gate :as gate]
            [google-workspace-ingest.gmail :as gmail]
            [google-workspace-ingest.run :as run]
            [google-workspace-ingest.source :as source]
            [importer.cursor :as cursor]
            [importer.model :as im]
            ["node:fs" :as fs]))

(defn- slurp* [p] (fs/readFileSync p "utf8"))
(def manifest (js->clj (js/JSON.parse (slurp* "actor-manifest.jsonld"))))
(def claims (edn/read-string (slurp* "docs/identity-claims.edn")))
(def census (:census claims))

(def open (set gate/required))
(defn- ok [body] {:connector.http/status 200 :connector.http/body body})

;; ── gate ──────────────────────────────────────────────────────────────────

(deftest nothing-is-emitted-without-attestation
  (doseq [a [{} #{} nil]]
    (testing (pr-str a)
      (is (= :blocked (:gate/status (gate/verdict a))))
      (is (= (count gate/required) (count (:gate/missing (gate/verdict a))))))))

(deftest one-missing-attestation-still-blocks
  (is (= :blocked (:gate/status (gate/verdict (disj open :retention-policy-bound))))
      "the seven are an AND; six of seven is not six sevenths of ready"))

(deftest an-explicit-false-is-not-an-attestation
  (let [a (into {} (map (fn [g] [g (if (= g :no-platform-held-key-baseline) false true)]))
                gate/required)]
    (is (= [:no-platform-held-key-baseline] (gate/missing a))
        "reading a key's presence rather than its value is the quietest way to loosen a gate")))

(deftest all-four-attestation-shapes-are-accepted
  (testing "one shape falling through blocks only the callers using it -- safely, so it does not look like an incident"
    (doseq [[label a] [["keyword set" open]
                       ["string set" (set (map name gate/required))]
                       ["keyword map" (zipmap gate/required (repeat true))]
                       ["string map" (zipmap (map name gate/required) (repeat true))]]]
      (testing label
        (is (gate/ready? (gate/verdict a)))))))

(deftest authorize-runs-the-body-only-when-ready
  (is (= :ran (:v (gate/authorize open (fn [] {:v :ran}) {:v :blocked}))))
  (is (= :blocked (:v (gate/authorize {} (fn [] {:v :ran}) {:v :blocked})))))

;; ── source ────────────────────────────────────────────────────────────────

(deftest the-source-declares-three-streams-with-three-cursor-styles
  (is (im/valid? source/source) (pr-str (im/errors source/source)))
  (is (= [:calendar :files :mail] (im/stream-kinds source/source)))
  (is (= 3 (count (set (map :importer.stream/cursor-style (im/streams source/source)))))
      "three APIs, three ways for a cursor to lapse -- this is why Google is one importer and not one parameterised one"))

(deftest governance-agrees-with-the-manifest
  (let [g (get manifest "governance")]
    (is (= (get g "piiTier") (:importer.governance/pii-tier source/governance)))
    (is (= (get g "retentionDays") (:importer.governance/retention-days source/governance)))
    (is (= (get g "consentRequired") (:importer.governance/consent-required? source/governance)))
    (is (= (get g "purpose") (:importer.governance/purpose source/governance)))))

(deftest the-gmail-retention-is-a-documented-lower-bound
  (is (number? (im/history-retention source/source :mail))
      "a number, because Gmail publishes one; :unknown would understate what we know")
  (is (= (* 7 24 60 60 1000) (im/history-retention source/source :mail))
      "used as an expiry, a lower bound resyncs earlier than needed -- the safe direction to be wrong in"))

;; ── identity: do not name a DID that does not resolve ─────────────────────

(defn unmeasured-dids
  "`texts` に現れる did:web で、claims が測っていないもの。"
  [texts claims]
  (let [measured (set (concat (map :did (:claims claims)) (map :did (:observed claims))))]
    (->> texts
         (mapcat #(re-seq #"did:web:[a-zA-Z0-9:._%-]+" %))
         distinct
         (remove measured)
         sort vec)))

(deftest the-checker-for-unmeasured-dids-actually-discriminates
  "空を返す検査は、走らなかった検査と同じ顔をする。先に噛むことを見せる。"
  (is (= ["did:web:made.up.example"]
         (unmeasured-dids ["we are did:web:made.up.example now"] claims)))
  (is (= [] (unmeasured-dids ["did:web:itonami.cloud"] claims))
      "measured ones pass")
  (is (seq (:observed claims)) "the claims file has real measurements in it, not an empty shell"))

(deftest no-unmeasured-did-is-named-in-this-repository
  (let [srcs (->> (fs/readdirSync "src/google_workspace_ingest")
                  (map #(slurp* (str "src/google_workspace_ingest/" %))))
        texts (conj (vec srcs) (slurp* "actor-manifest.jsonld"))]
    (is (= [] (unmeasured-dids texts claims))
        "naming a did:web nobody measured is how m365-ingest ended up claiming one that does not exist")
    (is (= 0 (:declared-did-count census)))
    (is (nil? (get manifest "did")) "the manifest says null rather than inventing one")))

(deftest the-census-matches-the-substrate
  (is (= (count gate/required) (:required-gate-count census)))
  (is (= (count (im/streams source/source)) (:stream-count census))))

;; ── gmail: a history page yields ids, not records ─────────────────────────

(def c-mail (source/mail-cursor "jun@example.com"))

(def a-history
  {"history" [{"id" "10"
               "messagesAdded" [{"message" {"id" "m1" "threadId" "t1"}}
                                {"message" {"id" "m2" "threadId" "t1"}}]
               "messagesDeleted" [{"message" {"id" "gone1"}}]}]
   "historyId" "42"})

(defn- a-message [id]
  {"id" id "threadId" "t1" "internalDate" "1788051723000"
   "labelIds" ["INBOX"] "snippet" "hi"
   "payload" {"headers" [{"name" "Message-ID" "value" (str "<" id "@x.com>")}
                         {"name" "Subject" "value" "Re: quote"}
                         {"name" "From" "value" "A Buyer <Buyer@example.com>"}
                         {"name" "To" "value" "jun@example.com"}]}})

(deftest a-history-page-names-ids-and-carries-no-headers
  (let [r (gmail/page c-mail (ok a-history))]
    (is (= ["m1" "m2"] (:fetch r)))
    (is (= [{:tombstone/provider-id "gone1" :tombstone/reason "deleted"}] (:tombstones r)))))

(deftest an-unfetched-message-holds-the-cursor
  (testing "fetching one of two and syncing would lose the other forever: the historyId moves past it"
    (let [r (gmail/page c-mail (ok a-history) {"m1" (a-message "m1")})]
      (is (= :unmeasured (:importer.plan/outcome (:plan r))))
      (is (str/includes? (:importer.plan/note (:plan r)) "1 of 2"))
      (is (= :held (:importer.cursor/state
                    (cursor/advance c-mail {:outcome (:importer.plan/outcome (:plan r))
                                            :planned 0 :persisted 0 :at 1})))))))

(deftest a-fully-fetched-page-syncs-and-names-the-next-run
  (let [r (gmail/page c-mail (ok a-history) {"m1" (a-message "m1") "m2" (a-message "m2")})
        rec (first (:importer.plan/items (:plan r)))]
    (is (= :synced (:importer.plan/outcome (:plan r))))
    (is (= 2 (count (:importer.plan/items (:plan r)))))
    (is (= "rfc5322:m1@x.com" (:mail/id rec))
        "the RFC 5322 id, so a mailbox migrated from Exchange is not stored twice")
    (is (= {:person/address "buyer@example.com" :person/name "A Buyer"} (:mail/from rec)))
    (is (= "42" (:incremental-token r)) "the historyId is where the next run starts")))

(deftest a-mid-run-page-does-not-name-the-next-run
  (let [r (gmail/page c-mail (ok (assoc a-history "nextPageToken" "np"))
                      {"m1" (a-message "m1") "m2" (a-message "m2")})]
    (is (= "np" (:importer.plan/next-token (:plan r))))
    (is (nil? (:incremental-token r))
        "promoting on a mid-run page would end the backfill early and never read the rest")))

(deftest a-404-history-is-a-planned-resync
  (let [r (gmail/page c-mail {:connector.http/status 404 :connector.http/body {}})]
    (is (:importer.plan/resync-required? (:plan r)))
    (is (= :failed (:importer.plan/outcome (:plan r))))))

(deftest a-throttled-gmail-is-unmeasured
  (doseq [s [429 500 nil]]
    (is (= :unmeasured (:importer.plan/outcome (:plan (gmail/page c-mail {:connector.http/status s :connector.http/body {}}))))
        (str "status " s))))

;; ── drive and calendar ────────────────────────────────────────────────────

(def c-files (source/files-cursor "my-drive"))
(def c-cal (source/calendar-cursor "primary"))

(deftest drive-keeps-its-two-tokens-apart
  (let [mid (drive/page c-files (ok {"changes" [] "nextPageToken" "np"}))
        fin (drive/page c-files (ok {"changes" [] "newStartPageToken" "start-9"}))]
    (is (= "np" (:importer.plan/next-token (:plan mid))))
    (is (nil? (:incremental-token mid)))
    (is (nil? (:importer.plan/next-token (:plan fin))))
    (is (= "start-9" (:incremental-token fin)))))

(deftest drive-removals-are-tombstones
  (let [r (drive/page c-files (ok {"changes" [{"fileId" "f1" "file" {"id" "f1" "name" "quote.pdf"
                                                                    "mimeType" "application/pdf"}}
                                              {"fileId" "f2" "removed" true}]
                                   "newStartPageToken" "s"}))]
    (is (= 1 (count (:importer.plan/items (:plan r)))))
    (is (= "gdrive:f1" (:file/id (first (:importer.plan/items (:plan r))))))
    (is (= [{:tombstone/provider-id "gdrive:f2" :tombstone/reason "removed"}] (:tombstones r)))))

(deftest a-lapsed-calendar-sync-token-is-a-planned-resync
  (let [r (calendar/page c-cal {:connector.http/status 410 :connector.http/body {}})]
    (is (:importer.plan/resync-required? (:plan r)))))

(deftest cancelled-events-are-tombstones-and-organizers-are-people
  (let [r (calendar/page c-cal (ok {"items" [{"id" "e1" "summary" "Kickoff"
                                              "start" {"dateTime" "2026-08-30T01:00:00Z"}
                                              "end" {"dateTime" "2026-08-30T02:00:00Z"}
                                              "organizer" {"email" "Jun@Example.com" "displayName" "Jun"}
                                              "attendees" [{"email" "buyer@example.com"}]}
                                             {"id" "e2" "status" "cancelled"}]
                                    "nextSyncToken" "st-2"}))
        ev (first (:importer.plan/items (:plan r)))]
    (is (= 1 (count (:importer.plan/items (:plan r)))))
    (is (= "gcal:e1" (:event/id ev)))
    (is (= {:person/address "jun@example.com" :person/name "Jun"} (:event/organizer ev))
        "Google nests the address under \"email\", Graph under \"emailAddress\"; both must land on one person")
    (is (= [{:person/address "buyer@example.com"}] (:event/attendees ev)))
    (is (= "st-2" (:incremental-token r)))))

;; ── run: gate first, persist, then move ───────────────────────────────────

(defn- fetched-history []
  {:cursor c-mail :attestations open :now 1000 :response (ok a-history)
   :fetched {"m1" (a-message "m1") "m2" (a-message "m2")}})

(deftest a-blocked-gate-emits-nothing-and-does-not-move-the-cursor
  (let [r (run/step (assoc (fetched-history) :attestations {}))]
    (is (= :unmeasured (:outcome r)))
    (is (= [] (:effects r)))
    (is (= :blocked (:gate/status (:gate r))))
    (let [{:keys [importer/cursor importer/outcome]} (run/land r c-mail {:importer.sink/persisted 0} 1000)]
      (is (= c-mail cursor))
      (is (= :unmeasured outcome)))))

(deftest an-open-gate-emits-one-effect-per-record
  (let [r (run/step (fetched-history))]
    (is (= :synced (:outcome r)))
    (is (= 2 (count (:effects r))))
    (is (= run/collection (:collection (first (:effects r)))))))

(deftest imported-data-cannot-reach-the-top-of-the-envelope
  (let [hostile (assoc-in (a-message "m1") ["payload" "headers"]
                          [{"name" "Message-ID" "value" "<m1@x.com>"}])
        hostile (assoc hostile "actor" "cloud-itonami/somebody-else" "source" "forged")
        r (run/step (assoc (fetched-history) :fetched {"m1" hostile "m2" (a-message "m2")}))
        record (:record (first (:effects r)))]
    (is (= "cloud-itonami/actor-google-workspace-ingest" (:actor (first (:effects r)))))
    (is (= source/source-id (:source record)))
    (is (= [] (run/envelope-problems record)))
    (is (contains? record :imported))))

(deftest a-page-the-sink-half-took-does-not-move-the-cursor
  (let [r (run/step (fetched-history))
        {:keys [importer/cursor importer/outcome importer/receipt]}
        (run/land r c-mail {:importer.sink/persisted 1} 1000)]
    (is (= :failed outcome))
    (is (= c-mail cursor))
    (is (= :partial-write (:importer.receipt/held-reason receipt)))))

(deftest the-three-streams-keep-separate-cursors
  (is (= 3 (count (set (map cursor/stream-key [c-mail c-files c-cal]))))
      "one cursor shared across three APIs is a cursor that is wrong for two of them"))
