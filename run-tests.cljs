#!/usr/bin/env nbb
;; nbb --classpath "src:test:../../kotoba-lang/importer/src" run-tests.cljs
;;
;; 終了コードは `:end-run-tests` の報告から取る。**`run-tests` の返り値からではない**
;; —— nbb ではそれが nil なので、よくある書き方
;;
;;   (let [{:keys [fail error]} (t/run-tests 'ns)]
;;     (js/process.exit (if (pos? (+ fail error)) 1 0)))
;;
;; は nil を 2 回分解して 0 に足し、**赤いまま exit 0 で終わる**。
;; 2026-08-30 に無改変の com-google-gmail で実測: 4 failures、exit 0。
(require '[clojure.test :as t] 'google-workspace-ingest.ingest-test)

(defmethod t/report [:cljs.test/default :end-run-tests] [m]
  (if (t/successful? m)
    (println "\nactor-google-workspace-ingest: all green")
    (println "\nactor-google-workspace-ingest: FAILED"))
  (js/process.exit (if (t/successful? m) 0 1)))

(t/run-tests 'google-workspace-ingest.ingest-test)
