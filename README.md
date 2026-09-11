# actor-google-workspace-ingest

**1 tenant の Google Workspace を、3 系統の cursor で追う importer actor。**
deny-by-default の gate の後ろに居り、**HTTP を送らず、credential を持たない。**

契約は [`kotoba-lang/importer`](https://github.com/kotoba-lang/importer)。
兄弟は [`cloud-itonami/m365-ingest`](https://github.com/cloud-itonami/m365-ingest)。

## なぜ Gmail / Drive / Calendar で 3 repo ではないのか

connector 面ではそれが正しい —— `com-google-gmail` / `com-google-drive` /
`com-google-calendar` は 3 API・1 OAuth client で 3 repo である（ADR-2608097000）。

**importer の単位は API ではなく tenant grant である。** 3 つに割ると、1 つの
admin 同意に対して resident が 3 体・gate が 3 枚・governance 記録が 3 つできる。
だから actor は 1 本で、その中に cursor が 3 系統ある。

| stream | API | cursor | 失効の仕方 |
|---|---|---|---|
| `:mail` | `users.history.list` | `startHistoryId` | 古すぎると 404 → 全読み直し |
| `:files` | `changes.list` | `pageToken` | 最終ページの `newStartPageToken` が次回の起点 |
| `:calendar` | `events.list` | `syncToken` | 410 で失効 |

兄弟の Microsoft 365 は Graph 1 API なので、cursor は folder ごとの delta 1 系統。
**この非対称が「1 つの importer を parameterise する」を否定する。**

## bots はこれを走らせない。読む

resident は tenant ごとに 1 体で、bot は多数。bot がそれぞれ同期すると、1 本の
delta 系列を N 本の cursor が奪い合い、credential が N 個に増え、1 つの throttle
予算が N 分割される —— そして**全員が「最新です」と答える**。cursor の内側からは
何も見えないからである。

bot が触るのは `importer.connector` の `corpus_*` tool で、その auth profile は
`bearer` である。`connector.validate` は scope 機構を持たない provider に scope を
宣言することを **error** にするので、`corpus_*` は Google の scope を宣言できない
——規約ではなく、load 時に throw する。**N 体の bot が tenant のメールを読み、
そのうち 0 体が `gmail.readonly` を持つ。**

## Gmail の history は id しか返さない

Graph の delta が message そのものを返すのに対し、`users.history.list` が返すのは
**stub**（id / threadId / labelIds）である。だから `gmail/page` は 2 つの入力を取る
—— history の応答と、**既に取得済みの message**。

取得できていない id が 1 つでも残っていれば outcome は `:unmeasured` になり、
cursor は動かない。ここを「取れた分だけ `:synced`」にすると、取れなかった message は
historyId が進んだ後の history にも現れず、**恒久的に消える**。

## DID を名乗っていない

`docs/identity-claims.edn` に測定がある:

- `did:web:itonami.cloud` は解決する（**200**）。ただし org の DID であって
  actor のものではない。これを名乗ると actor の effect が org に帰属する。
- `did:web:itonami.cloud:actor:google-workspace-ingest` は **404**。兄弟 actor で
  測っても同じ。

なので `actor-manifest.jsonld` の `"did"` は `null` である。**test が、この repo の
どこかに現れる `did:web:` で claims が測っていないものを拒否する** —— m365-ingest が
「名乗る DID は解決せず、解決する DID は誰も名乗っていない」に至った経路を、
機械で塞いである。配信が始まったら測り直して `:claims` に移す。

## 確かめる

```bash
kbb --backend sci --classpath "src:test:../../kotoba-lang/importer/src" run-tests.cljs   # 26 tests / 77 assertions
kbb --backend sci --classpath "src:../../kotoba-lang/importer/src" mutate.cljs \
    --kernel ../../kotoba-lang/importer/src                                # 10 mutations
```

`mutate.cljs` は不変条件を 1 つずつ壊した写しで suite を回し、赤くなることと
**それを pin している test が名前で出ること**の両方を要求する。無改変の写しが
緑であることを先に確かめてから始める（harness 自身が壊れていれば、全部の
mutation が「捕まえた」に見える）。

## ここにあるもの

| | |
|---|---|
| `src/.../gate.cljc` | 7 本 AND の deny-by-default。`false` を attest とみなさない。attestation の 4 形を受ける |
| `src/.../source.cljc` | 3 stream・3 cursor style・governance の宣言。`importer.model` が検査する |
| `src/.../gmail.cljc` | history → id、取得済み message → record。純関数 |
| `src/.../drive.cljc` | changes → metadata。**本文は通らない**（block 面に直行する） |
| `src/.../calendar.cljc` | events → record、cancelled → tombstone |
| `src/.../run.cljc` | gate → effect（`step`）と、sink の報告 → cursor（`land`） |
| `docs/identity-claims.edn` | 名乗らない根拠の実測 |

## やらないこと

- **`step` と `land` を 1 本にしない。** 書く前に cursor を進める形が書けてしまう。
- **取り込んだ record を封筒の top level に merge しない。** provider が選べる
  文字列が actor 自身の主張と同じ場所に並ぶ。`envelope-problems` は「予約 key を
  持たないこと」ではなく「**自分の key しか無いこと**」を見る。
- **測っていない DID を名乗らない。**
