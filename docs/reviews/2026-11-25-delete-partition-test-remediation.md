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

# delete-partition-test 修正與驗證

狀態：十二項問題均已修正，相關驗證及獨立複核完成。

工作目錄：`/Users/al02475442/Desktop/java_project/kafka-delete-partition-review`，branch `delete-partition-test`，起始 HEAD `8cf29f25c6`。先完成五項 P1 的修正、測試及獨立複核，再處理七項 P2。後續依使用者指示，將已驗證的修正與測試整理為六個本機 commits，保留於同一 branch。

## 十二項修正

| # | 等級 | 修正後行為 | 主要回歸測試 |
|---|---|---|---|
| 1 | P1 | Uniform 同質／異質分配先移除失效 ownership，再計算配額與 owner；保留 revoke、ack、stable 流程。 | ConsumerAssignmentPartitionDeletionReviewTest、AssignmentPartitionShrinkTest、真實 consumer 4→2 收縮 |
| 2 | P1 | Simple 同質／異質 share 分配排除已刪除 ID，累積跨 topic 的更新並涵蓋所有存活 partition。 | SharePartitionDeletionReviewTest、AssignmentPartitionShrinkTest |
| 3 | P1 | 同一 delta 的 ELR 清理不再產生已刪除 partition 的 leader、follower 或 directory 工作。 | ImagePartitionDeletionReviewTest、ReplicaManagerTest.testDeletionReviewClearElrDoesNotRecreateRemovedReplica |
| 4 | P1 | 每個 topic 的 drain records 先暫存，再經 BoundedList 原子加入；容量不足不留下部分變更，也不撤銷已接受 topic。 | ControllerPartitionDeletionReviewTest 的超量／剩餘容量邊界 |
| 5 | P1 | Metadata v0–v13 回覆所有仍保留的可讀 partitions；v14 附帶 drain flag。新版 Sticky／CooperativeSticky 也處理實際移除。 | MetadataCachePartitionDeletionReviewTest、LegacyConsumerDeletionReviewTest、既有 assignor suites |
| 6 | P2 | Snapshot 保留 UUID、略去尾端 ID 時，從原 image 產生 local deletes；保留存活 ID 的 drain／force／HWM 狀態。 | ImagePartitionDeletionReviewTest、ReplicaManager snapshot leader／follower 案例 |
| 7 | P2 | Producer 先驗證及記錄 leader epoch／topic incarnation，再過濾不可寫的 draining partitions，保留 metadata error 的更新請求。 | ProducerMetadataDeletionReviewTest、Metadata／ProducerMetadata／Sender suites |
| 8 | P2 | Adaptive routing 的暖機以目前 active IDs 的 queue 覆蓋率判定，舊 queues 不會提前啟用不完整統計。 | AccumulatorRoutingDeletionReviewTest、RecordAccumulator／BuiltInPartitioner suites |
| 9 | P2 | Share 初始化以持久化 records 清理兩組初始化 ID；舊 callback 不能復活已刪除 ID 或另一 UUID 的狀態。 | ShareInitializationPartitionCleanupTest、SharePartitionDeletionReviewTest |
| 10 | P2 | 明確移除依移除前 registration 選擇遠端清理 owner；snapshot 缺少歷史 owner 或明確移除時無 leader，允許舊 host 執行冪等清理；reassignment 保持不刪遠端資料。 | ReplicaManager 遠端角色／snapshot／leaderless 案例、RemoteCleanupPartitionDeletionReviewTest |
| 11 | P2 | Share delivery state 依 UUID／HWM 的明確移除證據產生 tombstones；支援 load、首個可用 image、重試及冷快照掃描，並允許 runtime 切分大量 tombstones。 | SharePartitionCleanupTest、ShareStatePartitionDeletionReviewTest、ShareColdSnapshotBatchingTest |
| 12 | P2 | 正向 drain timeout 加法溢位飽和至 Long.MAX_VALUE，保留負數取消 sentinel。 | ControllerPartitionDeletionReviewTest 正常／force 大 timeout 案例 |

## 驗證

最終 **2,812 個不重複執行案例全部通過，0 failures／errors／skips**。先完成 P1 gate 的 490 個案例；接著完整回歸 2,807 個案例。最後修正無 leader 的遠端清理邊界後，再執行 core 171、metadata 147、storage 101 個案例，均通過。其他模組沿用同一輪完整回歸結果；重跑的類別取最新結果，沒有重複加總。JDK 為 17.0.12-oracle，Gradle 為 9.4.1。

| 模組 | 最終案例 | 結果來源 |
|---|---:|---|
| `clients` | 812 | final-green |
| `clients/clients-integration-tests` | 14 | final-green |
| `coordinator-common` | 3 | final-green |
| `core` | 171 | round2-green |
| `group-coordinator` | 1,008 | final-green |
| `metadata` | 147 | round2-green |
| `server-common` | 306 | final-green |
| `share-coordinator` | 90 | final-green |
| `storage` | 101 | round2-green |
| `streams` | 97 | final-green |
| `tools` | 63 | final-green |

所有原始 79 個 review 案例均有覆蓋且通過：其中最大 timeout 案例擴成 normal／force 兩個參數化案例，保留原斷言並加上精確的 Long.MAX_VALUE 驗證。真實 KRaft 案例共 14 個，包括新的 PartitionDeletionReviewTest 全部 9 個與既有 DeletePartitionsTest 全部 5 個；consumer physical removal 後的實際重新分配也已通過。

受影響模組的 Checkstyle、修改 Java 檔案的 scoped SpotlessCheck，以及 tracked／untracked whitespace 檢查均通過。此處是相關套件驗證，未宣稱執行 Kafka 全專案的所有測試。

原始證據及可重跑的命令：

- [最終去重計數與原 79 案例覆蓋](/private/tmp/kafka-delete-partition-fixes-20261125/VERIFICATION.json)、[逐案例及 XML 路徑](/private/tmp/kafka-delete-partition-fixes-20261125/FINAL-CASES.json)。
- [完整回歸命令](/private/tmp/kafka-delete-partition-fixes-20261125/final-command.json)、[完整回歸 log](/private/tmp/kafka-delete-partition-fixes-20261125/final-verify.log)。
- [最後修正的覆蓋命令](/private/tmp/kafka-delete-partition-fixes-20261125/round2-command.json)、[覆蓋 log](/private/tmp/kafka-delete-partition-fixes-20261125/round2-verify.log)。
- [RED／GREEN 命令索引](/private/tmp/kafka-delete-partition-fixes-20261125/verification-commands.md)。

獨立複核涵蓋全部十二項修正。複核發現並補上 snapshot 的遠端清理、冷快照 tombstone 批次切分、明確移除時無 leader 的遠端清理三個邊界；修正後均完成 scoped re-review，沒有剩餘的重要 finding。無 leader 的四個 broker 案例先全數重現預期 remote=true、實際 false，再於修正後全數通過；controller 對照案例仍允許 all-offline force 刪除。

新增的冷快照批次測試使用真正的 CoordinatorRuntime、shard、serde、replay 與 commit，讓儲存 writer 強制每批最多 512 bytes；兩種路徑在修正前均重現 RecordTooLargeException。遠端清理測試使用兩個真正的 RemoteLogManager、真正的 metadata cache、記憶體內的 transport/storage substitutes 與 barriers，確保重複 physical deletion 和 terminal metadata 更新交錯確實執行；未連接外部 object store。

既有 DeletePartitionsTest 修正 metadata 等待上限及各叢集方法 timeout。原先期望重新使用已刪除 ID 的斷言，改為驗證拒絕擴充、存活資料正確及已刪除 log directory 維持不存在。四個既有 share cold-snapshot 測試改用含存活 partitions 的真實 metadata image，取代缺少 topics() 的不完整 mock；行為斷言保留。

## 相容性及範圍

舊版 Metadata request 無法可靠區分 producer 與 consumer。舊 producer 需要升級才能使用 drain flag 自動改道；舊二進位的 Sticky／CooperativeSticky 也需要升級才能處理實際 partition 收縮。KIP 與設計文件已同步此限制。保留原本 partition ID 永不重用的 fencing。

此修正涵蓋執行中 broker 的 metadata delta／snapshot catch-up；不新增持久化遠端 GC 任務或全新 broker 從空白狀態開機的 object-store 掃描機制。

原 checkout `/Users/al02475442/Desktop/java_project/kafka-m1` 保留於 `KAFKA-21501@9bef9f19f6`，沒有修改。六個 commits 僅建立於本機，未 push 或 merge。

## 本機 commit 整理

每個修正主題與其對應測試一起提交，所有 commits 均含專案要求的 `Generated-by: Codex (GPT-6)` trailer。

1. `MINOR: Reconcile group assignments after partition removal`
2. `MINOR: Make partition removal terminal across metadata and replicas`
3. `MINOR: Preserve draining metadata and client routing state`
4. `MINOR: Clean removed share partition state durably`
5. `MINOR: Cover partition deletion across clients and tooling`
6. `MINOR: Record partition deletion review fixes and verification`

提交整理前，46 個檔案的 SHA256 均與上述驗證快照相符。程式碼與測試內容保持一致，只更新此報告與計畫中的本機提交狀態；各中間 commit 的依賴關係另經靜態檢查，沒有宣稱逐個中間版本都重新執行測試。
