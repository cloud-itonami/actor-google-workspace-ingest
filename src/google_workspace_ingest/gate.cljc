(ns google-workspace-ingest.gate
  "deny-by-default。attest が揃わなければ effect を 1 つも出さない。

   ## blocked な判定は record を持ち歩かない

   `:effects []` だけ返して records は返す、という形にすると『gate を通っていない
   書き込みを、gate の出力から組み立てられる』状態になる。兄弟 `m365-ingest` の
   gate が同じ理由で `:records` ごと落としている。

   ## attestation は 4 つの形を受ける

   set / keyword の map / string の map / string の set。1 つ落ちると **その形で
   attest している呼び出し側だけが黙って全 blocked** になり、安全側に倒れるので
   事故に見えない —— m365-ingest がその罠を測って test に書いた実例で、ここでも
   生存側（受けるべきものを受ける）を両方向から撃つ。"
  (:require [clojure.string :as str]))

(def required
  "1 本でも欠ければ blocked。AND であって OR ではない。"
  [:council-charter-attestation
   :tenant-consent-recorded
   :delegation-scope-is-read-only
   :no-platform-held-key-baseline
   :retention-policy-bound
   :murakumo-only-inference-baseline
   :append-only-gate-baseline])

(defn attested?
  "`false` を attest とみなさない —— key の有無ではなく値を見る。
   `{:no-platform-held-key-baseline false}` は『無い』と明示的に言っており、
   それを『言及があるから通す』と読むのが一番静かな緩み方である。"
  [attestations gate]
  (boolean
   (cond
     (set? attestations) (or (contains? attestations gate)
                             (contains? attestations (name gate)))
     (map? attestations) (or (get attestations gate)
                             (get attestations (name gate)))
     :else false)))

(defn missing [attestations] (vec (remove #(attested? attestations %) required)))

(defn verdict
  [attestations]
  (let [m (missing attestations)]
    (if (seq m)
      {:gate/status :blocked :gate/missing m :gate/required required}
      {:gate/status :ready :gate/missing [] :gate/required required})))

(defn ready? [v] (= :ready (:gate/status v)))

(defn authorize
  "gate が ready のときだけ `f` を呼んでその結果を返す。blocked なら `blocked`。

   関数を渡させるのが要点: 呼び出し側で `(if (ready? v) ...)` と書けるようにすると、
   書き忘れた経路が 1 つできた瞬間に gate は無いのと同じになる。"
  [attestations f blocked]
  (let [v (verdict attestations)]
    (if (ready? v) (assoc (f) :gate v) (assoc blocked :gate v))))
