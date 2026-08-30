#!/usr/bin/env nbb
;; 不変条件を 1 つずつ壊した写しで suite を回し、**赤くなること**と
;; **それを pin している test が名前で出ること**の両方を要求する。
;;
;;   nbb --classpath "src:../../kotoba-lang/importer/src" mutate.cljs \
;;       --kernel ../../kotoba-lang/importer/src
;;
;; 緑の suite が言うのは「コードが test を通る」ことだけで、「コードが正しくなく
;; なったら test が気づく」ことではない。それを言うのはこちら。
(ns mutate
  (:require ["fs" :as fs] ["path" :as path] ["os" :as os] ["child_process" :as cp]
            [clojure.string :as str]))

(def argv (vec (drop 2 (js->clj js/process.argv))))
(defn- flag [n d]
  (if-let [i (some (fn [[i a]] (when (= a n) i)) (map-indexed vector argv))]
    (nth argv (inc i) d) d))
(def kernel (path/resolve (flag "--kernel" "../../kotoba-lang/importer/src")))

(def mutations
  [{:id :presence-is-attestation
    :file "src/google_workspace_ingest/gate.cljc"
    :from "     (map? attestations) (or (get attestations gate)\n                             (get attestations (name gate)))"
    :to   "     (map? attestations) (or (contains? attestations gate)\n                             (contains? attestations (name gate)))"
    :expect "an-explicit-false-is-not-an-attestation"
    :why "`{:gate false}` counts as attested"}

   {:id :one-shape-falls-through
    :file "src/google_workspace_ingest/gate.cljc"
    :from "     (set? attestations) (or (contains? attestations gate)\n                             (contains? attestations (name gate)))"
    :to   "     (set? attestations) (contains? attestations gate)"
    :expect "all-four-attestation-shapes-are-accepted"
    :why "string-set callers block silently -- safe-side, so it does not look like a fault"}

   {:id :authorize-always-runs
    :file "src/google_workspace_ingest/gate.cljc"
    :from "    (if (ready? v) (assoc (f) :gate v) (assoc blocked :gate v))"
    :to   "    (assoc (f) :gate v)"
    :expect "a-blocked-gate-emits-nothing-and-does-not-move-the-cursor"
    :why "effects are emitted with the gate closed"}

   {:id :unfetched-is-fine
    :file "src/google_workspace_ingest/gmail.cljc"
    :from "                  :plan (if (pos? short)"
    :to   "                  :plan (if false"
    :expect "an-unfetched-message-holds-the-cursor"
    :why "the historyId moves past messages that were never fetched; they are gone"}

   {:id :mid-run-page-names-the-next-run
    :file "src/google_workspace_ingest/gmail.cljc"
    :from "           (and (nil? next-token) history-id (zero? short))"
    :to   "           (and history-id (zero? short))"
    :expect "a-mid-run-page-does-not-name-the-next-run"
    :why "the backfill ends early and the remaining pages are never read"}

   {:id :drive-conflates-its-tokens
    :file "src/google_workspace_ingest/drive.cljc"
    :from "                                   :next-token next-token :outcome :synced})"
    :to   "                                   :next-token (or next-token start-token) :outcome :synced})"
    :expect "drive-keeps-its-two-tokens-apart"
    :why "the next-run token is used as a mid-run token"}

   {:id :cancelled-events-are-records
    :file "src/google_workspace_ingest/calendar.cljc"
    :from "            {dead true live false} (group-by cancelled? items)"
    :to   "            {dead true live false} {true [] false items}"
    :expect "cancelled-events-are-tombstones-and-organizers-are-people"
    :why "an event the tenant cancelled stays in the corpus as if it were happening"}

   {:id :envelope-merges-imported-data
    :file "src/google_workspace_ingest/run.cljc"
    :from "   :imported record})"
    :to   "   :imported record} record)"
    :expect "imported-data-cannot-reach-the-top-of-the-envelope"
    :why "provider-controlled keys sit beside the actor's own claims"}

   {:id :a-404-is-just-a-failure
    :file "src/google_workspace_ingest/gmail.cljc"
    :from "       {:plan (mk {:outcome :failed :resync-required? true\n                   :note \"startHistoryId is older"
    :to   "       {:plan (mk {:outcome :failed\n                   :note \"startHistoryId is older"
    :expect "a-404-history-is-a-planned-resync"
    :why "a lapsed cursor is recorded as an incident and never triggers a resync"}

   {:id :an-unmeasured-did-is-named
    :file "actor-manifest.jsonld"
    :from "  \"did\": null,"
    :to   "  \"did\": \"did:web:actor-google-workspace-ingest.itonami.cloud\","
    :expect "no-unmeasured-did-is-named-in-this-repository"
    :why "the repository claims a DID nobody resolved -- m365-ingest's exact defect"}])

(defn- copy-tree! [src dst]
  (fs/mkdirSync dst #js {:recursive true})
  (doseq [e (fs/readdirSync src #js {:withFileTypes true})]
    (let [s (path/join src (.-name e)) d (path/join dst (.-name e))]
      (when-not (#{".git" "node_modules"} (.-name e))
        (if (.isDirectory e) (copy-tree! s d) (fs/copyFileSync s d))))))

(defn- run-suite [dir]
  (let [r (cp/spawnSync "npx" #js ["--yes" "nbb" "--classpath" (str "src:test:" kernel) "run-tests.cljs"]
                        #js {:cwd dir :encoding "utf8"})]
    {:code (.-status r) :out (str (.-stdout r) (.-stderr r))}))

(let [root (fs/mkdtempSync (path/join (os/tmpdir) "gws-mutate-"))
      bad (atom [])]
  (println "control:")
  (let [d (path/join root "control")]
    (copy-tree! "." d)
    (let [{:keys [code]} (run-suite d)]
      (println "  unmutated copy exits" code (if (zero? code) "(good)" "(the harness is broken, not the code)"))
      (when-not (zero? code) (js/process.exit 1))))
  (println "mutating" (count mutations) "invariants:")
  (doseq [{:keys [id file from to expect why]} mutations]
    (let [d (path/join root (name id))]
      (copy-tree! "." d)
      (let [p (path/join d file) before (fs/readFileSync p "utf8")]
        (if-not (str/includes? before from)
          (do (println "  REFUSED " (name id) "-- anchor not found") (swap! bad conj [id :anchor]))
          (do (fs/writeFileSync p (str/replace before from to))
              (let [{:keys [code out]} (run-suite d)]
                (cond
                  (zero? code) (do (println "  SURVIVED" (name id) "--" why) (swap! bad conj [id :survived]))
                  (not (str/includes? out expect))
                  (do (println "  WRONG   " (name id) "-- red, but" expect "did not name it")
                      (swap! bad conj [id :wrong-test]))
                  :else (println "  caught  " (name id) "->" expect))))))))
  (if (seq @bad)
    (do (println "\n" (count @bad) "not caught:" (pr-str @bad)) (js/process.exit 1))
    (do (println "\nall" (count mutations) "caught by the test that pins each") (js/process.exit 0))))
