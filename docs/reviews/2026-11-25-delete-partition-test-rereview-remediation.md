<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# delete-partition-test 完整重審修正

狀態：四項原始問題與另外兩項確認的邊界修正均已完成，最終回歸及獨立 review 通過。

本次承接 `6b95ada57d` 的[完整重審](2026-11-25-delete-partition-test-full-rereview.md)。上一輪是找出問題並保留 RED 證據，本輪依使用者指示修好四項問題，並盤點工具。工作目錄為 `/Users/al02475442/Desktop/java_project/kafka-delete-partition-review`，branch `delete-partition-test`；保留 review 新增測試，沒有停用失敗案例。

## 四項修正

| 問題 | 修正後行為 | 驗證 |
|---|---|---|
| P1：remote cleanup 阻塞 heartbeat | `ReplicaManager` 在完整 `applyDelta` 結束後，以單一 volatile write 發布 immutable applied image 與 local drain candidates。Heartbeat 採樣不再取得持有 remote I/O 的 global replica-state monitor；仍逐次執行目前 partition 的 UUID、leader、epoch、fence、ISR／transaction／HW 安全檢查。 | 原本 ThreadMXBean 證明 BLOCKED 的測試轉綠；新增四個 parameterized cases 覆蓋 incomplete apply、舊 offset、resume 與失去 leadership。P1 covering gate176／0。 |
| P2：v0 API 使用 permissive quota | DeletePartitions 的 strict-since 門檻從3改成0；耗盡 quota 的 v0 request 由正式 recorder 拒絕，保留 topic error 與 throttle 時間。 | 原真實 request／quota sensor 的 NONE0→預期THROTTLING89 測試保留，full ControllerApis／quota manager／client retry suites 通過 covering gate。 |
| P2：KIP metrics 不一致 | 補上 `PendingDrainRemovals` 與 `DrainCompletedTotal`。兩個 drain gauges 在 active controller 回傳 committed cardinality，standby／未知 leader 為0，底層保留最新值供接任使用。Completed meter 是 process-local、active 時觀察到的 committed explicit individual removals，處理 coalesced removal＋topic deletion，不對 snapshots／standby 進行歷史補算，也不重算 missing／duplicate ID。 | registry RED 與九個 lifecycle RED 先重現，再轉綠；另有非0 node42 測試，KIP 同步精確描述 gauge／meter scope。 |
| P2：測試 teardown annotation 錯置 | `@AfterEach` 移回真正的 `tearDown()`，request test 僅保留 `@Test`。實際 API／ACL purgatory 清理恢復。 | Full ControllerApisTest 通過；真實 quota regression 及 duplicate／authorization assertions 未弱化。 |

Heartbeat 的修正保證不等待原來的 global monitor／remote-cleanup 路徑；沒有宣稱採樣完全 wait-free，因為 fresh partition 檢查仍使用其安全鎖。沒有快取 `Ready=true`，也沒有以 loader 進度假裝 metadata 已完整套用。

## 另外納入的兩項修正

1. **Streams reset 過期／不完整 CSV。** `resetOffsetsFromResetPlan` 現在先驗證計畫 key set 與目前選定的 input partitions 完全相同，才進行任何 offset query 或 seek。錯誤列出 missing／unexpected IDs，不靜默丟掉舊 ID，也不先移動部分 positions。兩個新 MockConsumer 案例先重現原本 NPE，合法 retained plan 的控制案例也保留；help 同步說明 `--from-file` scope。
2. **Metrics 關閉時漏掉 registered gauge。** 修正既有 post-close registry assertion 的無效 prefix 後，實際重現 broker0 MBean 未清除：它已註冊 gauge，但沒有 state-map entry。現在以實際註冊的 metric ID set 管理釋放，不再由 state-map keys 決定資源 ownership。原 fixture 與有效關閉斷言保留；沒有靠更改 fixture 隱藏問題。

這兩項都有獨立的 RED 證據。沒有把未重現的工具拓撲風險當成 bug 直接改寫。

## 工具盤點

已整理[工具影響矩陣](2026-11-25-delete-partition-tools-inventory.md)：43 個 Unix shell 入口、36 個 Windows wrappers，及 shared resetter／transactional copier／metadata shell view。

- `kafka-topics` 的 active／retained／draining 邏輯與 reassignment verify 先前已適配。
- 這輪確認並修正的是 Streams application reset 的 stale／incomplete CSV。
- producer／consumer／performance 類工具多數使用共用 SDK，仍需區分 subscribe 與手動 assignment、明確 ID 與新送出的未分區紀錄。
- replica verification、e2e latency、transactional copier 的 cached topology／明確 ID 路徑，以及 metadata shell 的新狀態顯示，列出具體補測或診斷增強項目；未宣稱都已有 production bug。
- 較舊 client 升級、同 UUID ID 不重用，以及 remote GC／`.stray` 限制仍遵守 KIP。

## 驗證

P1 gate：core176個案例全部通過，並經獨立 concurrency review。P2 covering gate：core225、metadata127、server8、clients303，共663個案例全部通過，quota／teardown 與 metrics 各自經獨立 review。之後追加的 node42 是通過的控制案例；關閉清理與 CSV 無效輸入則先重現 RED，再於修正後轉綠。最終強制重新執行相關 Test tasks，**3,223 個案例全部通過，0 failures／errors／skips**，包含17個實際 KRaft 案例及229個工具案例；原本三個失敗重現與原始79個 review 案例皆覆蓋且通過。這是單次完整回歸結果，未加總前面重跑的 gates。

| 模組 | 最終案例 |
|---|---:|
| `metadata` | 251 |
| `clients` | 856 |
| `core` | 258 |
| `group-coordinator` | 1,008 |
| `share-coordinator` | 90 |
| `storage` | 101 |
| `streams` | 97 |
| `tools` | 229 |
| `coordinator-common` | 6 |
| `server-common` | 306 |
| `clients/clients-integration-tests` | 17 |
| `server` | 4 |

113個變更 Java 檔案的 scoped Spotless、受影響模組的 Checkstyle 與 tracked／untracked whitespace 檢查通過。KIP 的 metrics 語意、測試要求及最新驗證紀錄已同步。這不是 Kafka 全專案 system suite，未使用外部 object store。

最終 review 已檢查所有 current production／KIP diffs 與對應測試，包括後加的 gauge ownership 與 CSV prevalidation，沒有剩餘重要 finding。執行結果由 final test log／JUnit XML 確認。

原始證據位於 `/private/tmp/kafka-delete-partition-rereview-fixes-20261125/`：

- [P1 GREEN／metrics RED](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/p1-green-metrics-red-summary.json)。此混合驗證整體 exit1 來自當時刻意保留的 metrics RED，core176為0失敗。
- [P2 unit gate](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/p2-unit-summary.json)。
- [工具與關閉清理 RED](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/tools-cleanup-red-summary.json)。
- [最終計數及原始 RED 覆蓋](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/VERIFICATION.json)、[逐案例 XML 路徑](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/FINAL-CASES.json)、[修正後 compiled annotations](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/controllerapis-fixed-bytecode.txt)。
- [最終完整命令](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/final-command.json)、[最終 log](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/final.log)。

修正與驗證階段未 commit。依後續使用者指示，修正、測試、KIP 與工具盤點整理為 `delete-partition-test` 上的六個本地 commits：heartbeat readiness、quota 與 teardown、controller metrics、Streams reset CSV 驗證、實體移除整合測試，以及 KIP／review／工具盤點文件。未 push 或 merge；原 `kafka-m1` checkout 保持 `KAFKA-21501@9bef9f19f6`，review worktree 保留。

此次整理僅更新文件的提交狀態；實作、測試及 KIP 內容與上述 3,223 個通過案例的最終 SHA256 快照相同。未因整理 commits 重新執行 Gradle，也未宣稱逐一建置或測試中間 commits。
