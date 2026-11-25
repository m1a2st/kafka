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

# delete-partition-test 設計與測試 Review

> 後續修正：本文保留修正前的 review 與 RED 證據。十二項問題現已修正，最終 2,812 個相關案例全部通過；請見[修正與驗證報告](2026-11-25-delete-partition-test-remediation.md)。

Review 對象：`delete-partition-test@8cf29f25c6`。比較基底是與本機 trunk 的 merge base：`da3b5e78ed2b3d5a9efec85760df7ff2ea6ad2cb`。

修正前結論：不建議合併。當時確認 **5 個 P1、7 個 P2** 問題，包括實際 consumer 進入 fatal state，以及已刪除 leader 重新接受寫入。此次新增 **79 個測試執行案例（包含參數化）**，分佈於 14 個新 Java 測試檔及 1 個既有 Scala 測試檔的追加。變更保留在此獨立 worktree，尚未 commit。

## 發現

### 1. [P1] 預設 consumer assignor 無法處理 partition 數量減少

位置：[UniformHeterogeneousAssignmentBuilder.java:299](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHeterogeneousAssignmentBuilder.java:299)；[UniformHomogeneousAssignmentBuilder.java:202](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHomogeneousAssignmentBuilder.java:202)；[GroupCoordinatorService.java:2964](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java:2964)

既有群組持有 partitions 0–3，物理移除 2、3 後，下一次 ConsumerGroupHeartbeat 仍會攜帶舊 ownership。Uniform assignor 沒有先排除已移除 ID：同質訂閱拋出 `Partitions were left unassigned`（對外為 UnknownServerException）；異質訂閱在長度 2 的陣列寫入 index 2，拋出 ArrayIndexOutOfBoundsException。這些 assignor 原本的假設是 partition 數只增不減；這次引入同一 UUID 的收縮，卻未配套修改，會使重平衡失敗並中斷消費進度。 實際 KRaft cluster 的 4→2 測試也重現：coordinator 回 UNKNOWN_SERVER_ERROR，consumer 隨即進入 fatal state，無法完成撤銷。

修正方向：產生 target assignment 前處理已移除 ID，同時保留正常的 ownership 撤銷流程；同質、異質訂閱都要支援收縮。

重現測試：

- [ConsumerAssignmentPartitionDeletionReviewTest.homogeneousDefaultConsumerAssignorMustRebalanceAfterTailRemoval](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/ConsumerAssignmentPartitionDeletionReviewTest.java:57)
- [ConsumerAssignmentPartitionDeletionReviewTest.heterogeneousDefaultConsumerAssignorMustRebalanceAfterTailRemoval](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/ConsumerAssignmentPartitionDeletionReviewTest.java:62)
- [PartitionDeletionReviewTest.consumerProtocolGroupReconcilesAfterPhysicalRemoval](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/PartitionDeletionReviewTest.java:175)

### 2. [P1] ShareConsumer 可能只被分配到已刪除的 partitions

位置：[SimpleHomogeneousAssignmentBuilder.java:236](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/SimpleHomogeneousAssignmentBuilder.java:236)；[GroupCoordinatorService.java:2964](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java:2964)

實際 SimpleAssignor 在 4→2 收縮案例回傳 `{2,3}`，正確結果應為 `{0,1}`。保留既有 assignment 的邏輯只檢查 topic 是否仍在訂閱，沒有檢查 partition 是否仍存在。結果同時保留無效 ID、漏掉仍有資料的有效 ID；不能以 client 重試讀取錯誤解決。

修正方向：讓 share assignment 依目前 topic partition 集合裁切舊 assignment，再分配所有有效 partitions。

重現測試：

- [SharePartitionDeletionReviewTest.realShareAssignmentMustRevokeDeletedIdsAndKeepEveryLivePartition](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/SharePartitionDeletionReviewTest.java:54)

### 3. [P1] Force 刪除後，ELR 清理會重建已刪除 replica

位置：[TopicDelta.java:67](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicDelta.java:67)；[TopicDelta.java:165](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicDelta.java:165)；[TopicDelta.java:253](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicDelta.java:253)；[ReplicaManager.scala:2399](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ReplicaManager.scala:2399)

可達情境是 RF=2、min ISR=2、ISR 只剩 leader、另一 replica 在 ELR：使用 force 到期移除尾端 partition，接著變更 min ISR，讓 RemovePartitionRecord 與 ClearElrRecord 進入同一個 metadata delta。Clear ELR 重新從舊 image 將已移除 ID 加回 partitionChanges；localChanges 同時輸出 delete 與 leader/follower 更新。ReplicaManager 先刪除 log，再依更新重建 replica，且原 draining fence 已清除。Force 授權資料移除，沒有授權重新建立或接收後續寫入。 最後的 broker 測試直接向已刪除 leader 寫入新紀錄，實際得到 Errors.NONE，正確應為 UNKNOWN_TOPIC_OR_PARTITION。

修正方向：ELR replay 與 localChanges 都排除已移除 IDs，維持「最終 metadata 不存在的 partition 不得產生建立 replica 工作」的不變條件。

重現測試：

- [ImagePartitionDeletionReviewTest.testElrClearAfterRemovalDoesNotRecreateReplicaWork](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/image/ImagePartitionDeletionReviewTest.java:95)
- [ReplicaManagerTest.testDeletionReviewClearElrDoesNotRecreateRemovedReplica](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala:6553)

### 4. [P1] 大型刪除請求回報失敗，卻提交部分 drain records

位置：[ReplicationControlManager.java:1177](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1177)；[ReplicationControlManager.java:1093](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1093)

將已有 10,002 partitions 的 topic 收縮至 1，需要 10,001 筆 drain records。程式直接向上限 10,000 的共用 BoundedList 逐筆加入；最後一筆失敗被逐 topic 的 catch 轉為錯誤回覆，但先前 10,000 筆仍留在 ControllerResult 並可提交。最高 ID 仍是 ACTIVE，較低 ID 已 DRAINING；正常由最高尾端往下移除的程序因此無法推進，producer 也會看到不連續的 active IDs。

修正方向：先暫存在每個 topic 的 records，確認容量及驗證皆成功後再原子加入共用結果，失敗回覆不得殘留該 topic 的部分變更。

重現測試：

- [ControllerPartitionDeletionReviewTest.testFailedOversizedDeletionProducesNoPartialDrainRecords](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/controller/ControllerPartitionDeletionReviewTest.java:238)

### 5. [P1] 舊版 consumer 在 drain 開始時就看不到仍可讀的 partitions

位置：[KRaftMetadataCache.java:285](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java:285)

Metadata v0–v13 的回覆一律排除 draining partitions。相同 Metadata API 同時服務 producer 與 consumer，因此舊 consumer 的 metadata／classic assignment 也會提早遺失尾端，無法利用宣稱保留給 consumer 的 drain 期間讀完資料。實測 v0、v9、v13 都將仍保留且可讀的 0–3 回成只有 0、1；v14 正常保留 draining 狀態。

修正方向：重新確定舊 client 相容性策略與最低版本要求。同一份舊版 Metadata request 無法可靠區分 producer／consumer，僅依 response version 分流，無法同時達成 KIP 對兩者的相容性承諾。

重現測試：

- [MetadataCachePartitionDeletionReviewTest.testLegacyConsumerMetadataRetainsReadableDrainingPartitions](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/metadata/MetadataCachePartitionDeletionReviewTest.java:84)

### 6. [P2] Snapshot 追上進度時漏刪已移除的本地 replica

位置：[TopicsDelta.java:75](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicsDelta.java:75)；[TopicsDelta.java:167](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicsDelta.java:167)

已運行 broker 從保留相同 topic UUID、但省略已刪除尾端的 snapshot 追上進度時，TopicDelta 以空 partition image 建立，finishSnapshot 只補整個 topic 的刪除。缺少的 partition IDs 因而沒有產生 local deletes。image 已不含該 ID，broker 仍持有 online replica 與本地 log；leader、follower 均已重現。空 image 的作法雖早已存在，這次首次允許個別 partition 消失後就需要補上這項 reconciliation。

修正方向：完成 snapshot 時，以先前 image 與 snapshot 最終 partition 集合計算個別移除，而非只比對 topic UUID。

重現測試：

- [ImagePartitionDeletionReviewTest.testSnapshotOmittingRemovedTailDeletesExistingLocalReplicas](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/image/ImagePartitionDeletionReviewTest.java:80)
- [ReplicaManagerTest.testDeletionReviewSnapshotRemovesMissingTailReplica](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala:6518)

### 7. [P2] Producer 的 drain 過濾繞過 leader epoch 驗證

位置：[Metadata.java:520](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/main/java/org/apache/kafka/clients/Metadata.java:520)

retainPartition 在 updateLatestMetadata 之前直接略過 draining entries。較舊 epoch 9 的 drain 回覆會移除已快取 epoch 10 的 active partition；反過來，新 drain 的 epoch 未被記錄，後續較舊 active 回覆又能把 draining IDs 放回路由。full 與 partial refresh 都失敗，可能造成錯誤分區路由、重試及逾時。

修正方向：先完成 epoch／topic incarnation 的狀態更新，再產生 producer 的可寫 partition 視圖；被路由排除不應等於遺失其最新狀態。

重現測試：

- [ProducerMetadataDeletionReviewTest.testOlderDrainCannotEvictNewerActivePartition](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/internals/ProducerMetadataDeletionReviewTest.java:51)
- [ProducerMetadataDeletionReviewTest.testOlderActiveMetadataCannotRestoreDrainingPartitions](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/internals/ProducerMetadataDeletionReviewTest.java:64)

### 8. [P2] 舊 accumulator queues 可能讓存活 partition 長期無法收到 unkeyed 流量

位置：[BuiltInPartitioner.java:115](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java:115)；[RecordAccumulator.java:662](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/main/java/org/apache/kafka/clients/producer/internals/RecordAccumulator.java:662)

先前只使用過 ID 0、2，完成送出後空 queues 仍存在。4→2 drain 後 active 是 0、1，但 batches.size()==2 提早滿足 adaptive warmup；統計只收進目前有 leader 的 0，從未建立 queue 的有效 ID 1 就持續被排除。新增 fallback 只在挑中不存在的 partition 時觸發，選到仍有效的 0 時無法修復這種飢餓。實測控制隨機 index=1，仍被導向 0。

修正方向：以目前 active partition 的覆蓋率判斷 warmup，或在 active 集合改變時重建 adaptive 統計，不能使用包含已移除 IDs 的 queue 總數。

重現測試：

- [AccumulatorRoutingDeletionReviewTest.testRemovedPartitionQueuesCannotStarveAnUnvisitedActivePartition](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/internals/AccumulatorRoutingDeletionReviewTest.java:50)

### 9. [P2] Share 群組即使取得正確 assignment，仍不斷重平衡

位置：[GroupMetadataManager.java:3055](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java:3055)；[GroupCoordinatorService.java:2964](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupCoordinatorService.java:2964)

initializedTopics 仍記錄原先 `{0,1,2,3}`。即使某次分配正確為 `{0,1}`，initializedAssignmentPending 仍認為初始化集合與 assignment 不一致，所以下一個內容未改變的 heartbeat 又把 group epoch 從 11 提升至 12。這是獨立於 SimpleAssignor 錯配的持久狀態問題。

修正方向：移除 partition 時同步寫入初始化／待初始化狀態的有效集合，並處理 coordinator 載入時漏接 metadata 變更的情況。

重現測試：

- [SharePartitionDeletionReviewTest.aSettledShrinkMustNotTriggerAnotherRebalanceOnEveryHeartbeat](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/SharePartitionDeletionReviewTest.java:68)

### 10. [P2] 個別 partition 的遠端 log 未被刪除

位置：[ReplicaManager.scala:2404](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ReplicaManager.scala:2404)；[RemoteLogManager.java:550](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/storage/src/main/java/org/apache/kafka/server/log/remote/storage/RemoteLogManager.java:550)

deleteRemoteLog 只在整個 topic 被刪除時為 true。新加入的 RemovePartitionRecord 路徑讓本地 log 刪除成功，但原 leader 傳給 RemoteLogManager 的 deleteRemoteLog=false，遠端 segments 的刪除程序沒有執行。測試使用真實本地 LogManager／UnifiedLog，僅替換遠端 manager 邊界；follower 的 false 行為保留為對照並通過。

修正方向：保留永久 partition 移除與 replica reassignment 的差別，讓負責永久刪除的 leader 提交遠端 cleanup。

重現測試：

- [ReplicaManagerTest.testDeletionReviewRemoteTailCleanupRunsOnLeaderOnly](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala:6589)

### 11. [P2] Share delivery state 在 partition 移除與 coordinator 重載後仍殘留

位置：[TopicsDelta.java:105](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/image/TopicsDelta.java:105)；[ShareCoordinatorService.java:1144](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorService.java:1144)

Share coordinator 只監聽 deletedTopicIds，沒有個別 partition 移除的 tombstone／載入後 reconciliation。已刪除 ID 的 delivery state 會保留，snapshotColdPartitions 也仍能為它產生新快照。測試顯示多餘 key 讓可裁切 offset 暫時留在 1，而有效 key 已前進到 100。cold snapshot 可能推進這個 fence，所以不能把它描述成永久阻塞 log pruning；已證實的是 orphan state、額外 snapshot 工作及延後 pruning。

修正方向：以 topic UUID 與已移除 partition IDs 清理 share state，並在載入時從目前 metadata 補做清理。

重現測試：

- [ShareStatePartitionDeletionReviewTest.metadataRemovalMustReleaseShareStateAndItsLogPruningFence](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/share-coordinator/src/test/java/org/apache/kafka/coordinator/share/ShareStatePartitionDeletionReviewTest.java:64)
- [ShareStatePartitionDeletionReviewTest.reloadedCoordinatorMustNotWriteFreshSnapshotsForPermanentlyRemovedPartitions](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/share-coordinator/src/test/java/org/apache/kafka/coordinator/share/ShareStatePartitionDeletionReviewTest.java:82)

### 12. [P2] 合法的大 timeout 會溢位為取消 drain

位置：[ReplicationControlManager.java:1170](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java:1170)

partition.drain.timeout.ms 接受 Long.MAX_VALUE，但 now+timeout 的加法會產生負 deadline。replay 將負數當成 cancellation，造成 API 成功卻沒有進入 draining。這是較少見的設定邊界問題。

修正方向：對 deadline 加法做溢位檢查，選擇飽和到最大值或在寫入 records 前清楚拒絕。

重現測試：

- [ControllerPartitionDeletionReviewTest.testMaximumDrainTimeoutCannotBecomeCancellation](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/controller/ControllerPartitionDeletionReviewTest.java:208)

## 驗證結果

使用 JDK 17.0.12、Gradle 9.4.1，測試 worker 上限 1，未啟用自動重試。以下按 module、class、case 名稱去重，以最後一次有效執行的結果計算。

| 範圍 | Cases | 通過 | 失敗 | 跳過 |
|---|---:|---:|---:|---:|
| 新增測試 | 79 | 53 | 26 | 0 |
| 分支原有測試 | 1,815 | 1,812 | 3 | 0 |
| 合計 | 1,894 | 1,865 | 29 | 0 |

新增案例的 26 個失敗，分別重現上述 12 個問題；測試保留正確的預期行為，沒有停用失敗案例。原有的 3 個失敗另列於下方。

| 新增案例所在測試類別 | Cases | 通過 | 失敗 |
|---|---:|---:|---:|
| [AdminRetriesDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/admin/AdminRetriesDeletionReviewTest.java) | 4 | 4 | 0 |
| [ProducerRoutingDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/ProducerRoutingDeletionReviewTest.java) | 4 | 4 | 0 |
| [AccumulatorRoutingDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/internals/AccumulatorRoutingDeletionReviewTest.java) | 1 | 0 | 1 |
| [ProducerMetadataDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/src/test/java/org/apache/kafka/clients/producer/internals/ProducerMetadataDeletionReviewTest.java) | 7 | 3 | 4 |
| [PartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/PartitionDeletionReviewTest.java) | 9 | 8 | 1 |
| [ReplicaManagerTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala) | 7 | 2 | 5 |
| [ConsumerAssignmentPartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/ConsumerAssignmentPartitionDeletionReviewTest.java) | 2 | 0 | 2 |
| [OffsetPartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/OffsetPartitionDeletionReviewTest.java) | 10 | 10 | 0 |
| [SharePartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/SharePartitionDeletionReviewTest.java) | 2 | 0 | 2 |
| [ControllerPartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/controller/ControllerPartitionDeletionReviewTest.java) | 9 | 7 | 2 |
| [ImagePartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/image/ImagePartitionDeletionReviewTest.java) | 8 | 4 | 4 |
| [MetadataCachePartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/metadata/MetadataCachePartitionDeletionReviewTest.java) | 5 | 2 | 3 |
| [ShareStatePartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/share-coordinator/src/test/java/org/apache/kafka/coordinator/share/ShareStatePartitionDeletionReviewTest.java) | 2 | 0 | 2 |
| [StreamsChangelogPartitionDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/streams/src/test/java/org/apache/kafka/streams/processor/internals/StreamsChangelogPartitionDeletionReviewTest.java) | 4 | 4 | 0 |
| [TopicCommandDeletionReviewTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/test/java/org/apache/kafka/tools/TopicCommandDeletionReviewTest.java) | 5 | 5 | 0 |

實際本機 KRaft cluster 測試共 9 個：8 個通過，1 個重現物理移除後的 consumer fatal state。通過案例涵蓋正常刪除等待未完成交易、force 後交易 coordinator 收斂、重新開啟保留 log／offset、keyed 與 unkeyed 路由、兩種 consumer protocol 在 draining 期間讀取、永久移除 ID 的 fencing，以及 broker 重啟後恢復 drain。

另外，新補的 offset 交易／重放／UUID fencing 案例 10 個全通過；Streams changelog 收縮與設定保護案例 4 個全通過；Admin retry 與 CLI preflight 的新增案例也通過。Java 編譯、相關 Checkstyle、限定此次新增 Java 測試檔的 Spotless，以及差異空白檢查均完成。遠端儲存案例使用真實本地 log，替換遠端 manager 邊界來確認 cleanup 參數；驗證範圍是本機定向測試與周邊回歸。

## 分支原有的整合測試問題

以下位於 [DeletePartitionsTest](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/DeletePartitionsTest.java:69)：

- `testDeletePartitionsFullLifecycle` 與 `testDeletePartitionsWithMultipleReplicas` 都在 60 秒逾時。前者已用 JDK thread dump 確認卡在 `verifyProduceToDrainingRejected → KafkaProducer.send → waitOnMetadata → ProducerMetadata.awaitUpdate`。共用 helper 只調低 delivery timeout，仍使用預設 60 秒的 max.block.ms；`@ClusterTest` 的 composed timeout 也是 60 秒，導致後續驗證尚未執行就中止。新增的 cluster 測試已明確設定 max.block.ms 與方法 timeout。
- `testRemovedPartitionDataIsGone` 嘗試在物理移除後重新建立 ID 2，實際得到 InvalidPartitionsException；測試仍假設能重用已刪除 IDs，與目前 KIP／controller 的 fencing 契約不一致。前述兩個 lifecycle 案例後段也有相同的重建假設。
- 原有 ShareConsumer 的 `testShareConsumerDeletePartitionsSubscriptionAdjusts` 使用預設一小時 drain，驗證仍存活的 partitions 可讀，沒有等待物理移除。它無法覆蓋此次發現的收縮後 assignment 問題。

## 需要保留在產品契約中的限制

目前一旦發生物理移除，同一 topic UUID 就無法再擴容：即使目標比原先更多，也會先碰到禁止重用的 IDs。draining 期間尚可用 CreatePartitions 重新開啟保留中的 partitions；真正移除後需要新 topic UUID。這是目前 [KIP](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/KIP.md:332) 明示的取捨，並有新增的 `physicalRemovalPreventsPartitionIdReuse` 實際叢集案例確認，沒有列為程式錯誤。對外宣傳「可縮容」時應明確說明此操作限制。

## 重現新增案例

在此 worktree 使用支援的 JDK 執行：

```bash
./gradlew \
  :metadata:test --tests '*PartitionDeletionReviewTest' \
  :clients:test --tests '*DeletionReviewTest' \
  :core:test --tests 'kafka.server.ReplicaManagerTest.testDeletionReview*' \
  :group-coordinator:test --tests '*PartitionDeletionReviewTest' \
  :share-coordinator:test --tests '*PartitionDeletionReviewTest' \
  :streams:test --tests '*PartitionDeletionReviewTest' \
  :tools:test --tests '*DeletionReviewTest' \
  :clients:clients-integration-tests:test \
    --tests org.apache.kafka.clients.admin.PartitionDeletionReviewTest \
  --continue --max-workers=4 -PmaxParallelForks=1 -Dorg.gradle.parallel=false
```

原始證據保存在 `/private/tmp/kafka-delete-partition-review-20261125/`：

- [RESULTS.json](/private/tmp/kafka-delete-partition-review-20261125/RESULTS.json)：去重統計、每個失敗案例與 traceback。
- [CASES.json](/private/tmp/kafka-delete-partition-review-20261125/CASES.json)：全部案例結果。
- [run-final.sh](/private/tmp/kafka-delete-partition-review-20261125/run-final.sh)：較大範圍的完整驗證命令；[review-final.log](/private/tmp/kafka-delete-partition-review-20261125/review-final.log) 為其輸出。
- [review-targeted-final.log](/private/tmp/kafka-delete-partition-review-20261125/review-targeted-final.log)：最終 broker／cluster 補充驗證。
- [original-lifecycle-wait-dump.txt](/private/tmp/kafka-delete-partition-review-20261125/original-lifecycle-wait-dump.txt)：既有 lifecycle timeout 的執行緒證據。
- `broad-final/`、`targeted-final/`：保留的 JUnit XML。後續篩選測試覆寫 build reports 時仍可核對。
