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

# delete-partition-test 完整重審

> 修正後更新：本文保留修正前的四項 finding 與 RED 證據。四項現已修正，另補上 Streams reset 與 metrics 關閉清理邊界；最終3,223案例全數通過，請見[本輪修正報告](2026-11-25-delete-partition-test-rereview-remediation.md)與[工具盤點](2026-11-25-delete-partition-tools-inventory.md)。

修正前結論：不建議合併。當時確認 **1 個 P1、3 個 P2**：兩項產品執行路徑問題、一項 KIP 指標契約落差，以及一項既有測試生命週期錯誤。三項已有失敗測試，測試生命週期另有編譯後 annotations 證據。實作與 KIP 保持不變，新增測試及此報告留為未提交變更。

Review 對象為 `delete-partition-test@6b95ada57d3b082bc7c8e8eb0a92612b0fc9e11e`，與 trunk 的 merge base 為 `da3b5e78ed2b3d5a9efec85760df7ff2ea6ad2cb`。完整範圍含 140 個變更檔案，涵蓋 KIP、協定、Admin／CLI、controller／broker、producer、consumer assignors、group／share coordinators、Streams、遠端儲存及測試。各範圍由獨立 reviewer 檢查，未直接沿用上一輪 review 的結論。

## 發現

### 1. [P1] 遠端清理會阻塞整台 broker 的 heartbeat

位置：[ReplicaManager.scala:482](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ReplicaManager.scala:482)、[BrokerLifecycleManager.java:590](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/server/src/main/java/org/apache/kafka/server/BrokerLifecycleManager.java:590)。

每次 heartbeat 都會在送出網路請求前，同步執行 `partitionDrainStatusProvider.get()`。目前 provider 直接呼叫 `ReplicaManager.partitionDrainStatuses()`，必須取得 `replicaStateChangeLock`。同一把鎖也包住 `applyDelta` 的 replica 刪除流程，其中 `RemoteLogManager.stopPartitions` 會同步等待 metadata futures 及遠端物件刪除。

因此，只要一次正常、合法的遠端清理耗時超過 broker session timeout，heartbeat 也無法送出，controller 就可能 fence 整台 broker，影響其餘無關 partitions。預設 session timeout 為 9,000 ms；遠端清理沒有相同時間上限。即使目前沒有其他 draining partition，status 方法進入時也仍要取得這把鎖。原先 heartbeat 的 metadata-offset provider 是 atomic read，沒有這個等待關係。

實際呼叫鏈：`BrokerLifecycleManager.sendBrokerHeartbeat` → `ReplicaManager.partitionDrainStatuses`；另一執行緒為 `ReplicaManager.applyDelta` → `stopPartitions` → `RemoteLogManager.stopPartitions` → deletion metadata futures／`deleteLogSegmentData`。參考 [ReplicaManager.scala:2395](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ReplicaManager.scala:2395)、[RemoteLogManager.java:575](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/storage/src/main/java/org/apache/kafka/server/log/remote/storage/RemoteLogManager.java:575)、[KRaftConfigs.java:44](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/raft/src/main/java/org/apache/kafka/raft/KRaftConfigs.java:44)。

重現：[ReplicaManagerTest.testReviewDrainHeartbeatSamplingDoesNotWaitForRemoteDeletion](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala:6588)。測試用真實 `applyDelta` 刪除已建立的 remote-enabled replica，僅以 latch 延後 remote stop。ThreadMXBean 確認採樣執行緒處於 **BLOCKED**，等待的 monitor owner 正是遠端清理執行緒；釋放 latch 後即可完成。失敗訊息包含兩條完整 stack。

建議：heartbeat 不應同步等待可能持有慢 I/O 的 replica-state lock。應提供已安裝 fence／registration 的一致快照，或在忙碌時省略此次 readiness 回報，並保留 controller 的 epoch／offset 驗證；也可重整 remote I/O 的持鎖範圍。測試實際證明鎖等待；超過 session timeout 導致 fencing 的結論來自正式呼叫鏈與 lease 設定，本輪未用外部 object store 製造實際 fencing。

### 2. [P2] DeletePartitions v0 永遠使用 permissive mutation quota

位置：[ControllerApis.scala:831](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ControllerApis.scala:831)。

handler 呼叫 `newQuotaFor(..., strictSinceVersion = 3)`，但 DeletePartitions 只支援 v0。正式 quota manager 只有在 request version 大於等於門檻時才選擇 strict quota，因此這個新 API 永遠走 permissive 路徑。耗盡 quota 後，permissive recorder 只記錄 throttle 時間、仍接受 mutation，不會拒絕該 topic。

這與 KIP 的 quota enforcement、`THROTTLING_QUOTA_EXCEEDED` 與 Admin per-topic retry 契約不一致。現有 `ControllerApisTest` 將 `newQuotaFor` 的版本參數整個 mock 掉；直接注入 quota exception 的 controller 測試也不會檢查這個選擇錯誤。參考 [ControllerMutationQuotaManager.java:169](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/server/src/main/java/org/apache/kafka/server/quota/ControllerMutationQuotaManager.java:169)、[PermissiveControllerMutationQuota.java:50](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/server/src/main/java/org/apache/kafka/server/quota/PermissiveControllerMutationQuota.java:50)。

重現：[ControllerApisTest.testReviewDeletePartitionsEnforcesExhaustedRealMutationQuota](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ControllerApisTest.scala:1072)。真實 v0 request 經序列化／解析後進入 handler，使用同 principal／client ID 的實際 quota manager 與已耗盡 sensor，controller stub 仍執行正式 context recorder。控制斷言先確認 strict quota 確實拒絕後續 permit；實際 response 卻是 **`NONE (0)`，預期為 `THROTTLING_QUOTA_EXCEEDED (89)`**。

建議：新 API 自 v0 起使用 strict quota，並保留此 handler-level 測試，避免只測 recorder 的 mock exception。

### 3. [P2] KIP 宣告的 drain 指標與實際 metrics registry 不一致

位置：[KIP.md:905](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/KIP.md:905)、[ControllerMetadataMetrics.java:146](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/controller/metrics/ControllerMetadataMetrics.java:146)。

KIP 宣告 `PendingDrainRemovals` 與 `DrainCompletedTotal`，但程式沒有註冊或更新這兩個指標。現有 metrics test 的完整 registry 期待值也沒有它們，因此既有測試會通過。按照 KIP 建立的監控無法取得所宣告的 pending／completed 資料。

此外，KIP 說 `DrainingPartitionCount` 僅由 active controller 回報，但每個 controller 都安裝 metrics publisher，publisher 沒有 leadership 判斷，snapshot／delta 都會更新此 gauge。standby 也會回報 cluster count，直接加總各 controller 數值會重複計算。參考 [ControllerServer.scala:384](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ControllerServer.scala:384)、[ControllerMetadataMetricsPublisher.java:157](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/main/java/org/apache/kafka/controller/metrics/ControllerMetadataMetricsPublisher.java:157)。

重現：[ControllerMetadataMetricsTest.testMetricNames](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/metadata/src/test/java/org/apache/kafka/controller/metrics/ControllerMetadataMetricsTest.java:39)。將 KIP 承諾的兩個 MBean 名稱加入既有 exact-registry assertion 後，實際 Yammer registry 缺少兩者，測試失敗；此測試比較 registry 的名稱，未直接查詢 JMX MBeanServer。名稱缺失有執行證據；standby scope 的落差由 publisher 安裝及更新路徑確認。

建議：補上指標實作與 leadership／取消／topic deletion／failover 更新規則，或先明確修訂 KIP 的公開契約；名稱、語意及測試應一致。

### 4. [P2] 新增 request 測試誤掛 `@AfterEach`，原 teardown 已失效

位置：[ControllerApisTest.scala:1360](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/test/scala/unit/kafka/server/ControllerApisTest.scala:1360)（本輪補測插入前，原 HEAD 為第 1311 行）。

`testDeletePartitionsRequest` 同時有 `@AfterEach` 與 `@Test`，而真正的 `tearDown()` 已沒有 annotation。JUnit 因此在每個測試後重新執行 request 測試，而不是呼叫原來的 shutdown／close。該方法還會覆寫 `controllerApis`，使當次測試建立的 API instance 失去正常清理機會。其 ACL purgatory 原本需要 `ControllerApis.close()` 關閉，這會留下測試資源與背景 reaper，破壞 suite 的隔離性。

證據：對本輪乾淨基準編譯出的 class 執行 `javap -v -p`，`testDeletePartitionsRequest` 的 RuntimeVisibleAnnotations 同時包含 `org.junit.jupiter.api.AfterEach` 與 `Test`；`tearDown` 沒有 runtime annotation。原始 bytecode 輸出保存在本輪 evidence 目錄。關閉鏈可見 [ControllerApis.scala:94](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/ControllerApis.scala:94)、[AclApis.scala:55](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/server/AclApis.scala:55)。

建議：把 `@AfterEach` 移回 `tearDown()`，request 測試只保留 `@Test`。這是測試生命週期問題，並非已觀察到的 broker 產品資源洩漏。

## 執行結果與覆蓋

乾淨 HEAD 基準強制重新執行 Test tasks，**3,032 個案例通過、0 failures／errors／skips**。另檢查完整 branch 變更中的 111 個 Java 檔案 Spotless，以及受影響模組 Checkstyle，均通過。JDK 為 17.0.12-oracle，Gradle 9.4.1。

| 模組 | 基準案例 |
|---|---:|
| `clients` | 856 |
| `clients/clients-integration-tests` | 14 |
| `coordinator-common` | 6 |
| `core` | 252 |
| `group-coordinator` | 1,008 |
| `metadata` | 235 |
| `server` | 4 |
| `server-common` | 306 |
| `share-coordinator` | 90 |
| `storage` | 101 |
| `streams` | 97 |
| `tools` | 63 |

接著新增 5 個案例，並依 KIP 強化 1 個既有 registry assertion；這 6 個案例結果為 **3 失敗、3 通過**。失敗正是上列 heartbeat、quota、metrics 契約測試，沒有停用或修改實作使它們轉綠。補測後的 Checkstyle 與 scoped Spotless 亦通過。這兩輪數字分開記錄，避免把重跑的既有 metrics 案例重複加總。

三個通過的新增實際 KRaft 案例：

- [ShareConsumerTest.testShareConsumerReconcilesPhysicalPartitionRemovalWithoutEpochChurn](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/ShareConsumerTest.java:2280)：open transaction 暫時阻擋最高尾端移除，明確讀到四個 partition 的 offset 0，確認其中 2／3 正處於 draining；ack 後 abort transaction，等待 full metadata 實際只剩 0／1，再驗證同一個 share consumer 收到兩個存活 ID 的新資料，並觀察超過兩個預設 heartbeat 週期，group／target epoch 不再增加。
- [PartitionDeletionReviewTest.classicGroupOffsetsAreDeletedAfterPhysicalRemoval](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/PartitionDeletionReviewTest.java:253)：active classic group 先提交四個 offsets，實際移除尾端後確認 2／3 的 offsets 消失、0／1 保留，再讀取及提交新資料。
- [PartitionDeletionReviewTest.consumerProtocolGroupOffsetsAreDeletedAfterPhysicalRemoval](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/PartitionDeletionReviewTest.java:259)：同上，使用 consumer protocol。

加上基準的 14 個既有 KRaft 案例，本輪共執行 17 個不同的叢集案例，皆通過。這補上原有 share 測試未真正等待 physical removal，以及 offset integration 未斷言尾端 tombstones 的缺口。

## 其餘範圍及限制

未發現額外具體問題的範圍包括：controller 尾端順序、per-topic record 原子性、timeout saturation、force／reopen／failover、新舊 leader 的 readiness epochs／offset、snapshot omission／取消 drain／ELR、local／future log 清理、遠端 owner 與冪等清理、transaction marker 收斂；Admin input/options snapshot、重試子集合與 deadline、descriptions pagination／ID／fallback、producer epoch／incarnation 與 adaptive queues、classic sticky／Uniform／Simple 收縮；offset pending-only transactions／rollback／EMPTY image、share 初始化 callback fencing 與 delivery tombstone batching，以及兩種 Streams protocol 的 internal-topic 規則。

協定 API／record IDs `94/29/30`、errors `136/137`、Force tag `0`、heartbeat tag `2`、Metadata v14／Describe v1、config 型別／預設值／範圍、MetadataVersion `33` 與 downgrade fencing 均與 KIP 對應。舊 producer／classic Sticky client 升級要求、partition ID 不重用、`.stray` 保留及未新增 durable remote GC 都是 KIP 已明示的限制，不列為本輪新問題。

本輪不是全 Kafka system suite，也未連接外部 object store。IDE 只開啟原 checkout，未開啟此 review worktree；未建立 debugger session 或 breakpoint，heartbeat 的執行證據來自測試內 ThreadMXBean 的實際 state／monitor owner／stacks。沒有把同 epoch metadata 的既有時序限制、未證明可達的 offset UUID race，或尚未執行的 KIP 測試計畫情境當成產品缺陷。

## 原始證據與工作樹

- [基準命令](/private/tmp/kafka-delete-partition-rereview-20261125/baseline-command.json)、[基準結果](/private/tmp/kafka-delete-partition-rereview-20261125/baseline-summary.json)、[基準 log](/private/tmp/kafka-delete-partition-rereview-20261125/baseline.log)。
- [重現及補測命令](/private/tmp/kafka-delete-partition-rereview-20261125/targeted-command.json)、[補測結果](/private/tmp/kafka-delete-partition-rereview-20261125/targeted-summary.json)、[補測 log](/private/tmp/kafka-delete-partition-rereview-20261125/targeted.log)。
- [ControllerApisTest bytecode annotations](/private/tmp/kafka-delete-partition-rereview-20261125/controllerapis-bytecode.txt)。各輪 JUnit XML 位於同一 evidence 目錄的 `baseline-results/`、`targeted-results/`。

Branch HEAD 保持 `6b95ada57d`；沒有修改產品實作或 KIP，沒有 commit、push 或 merge。工作樹保留五個測試檔的補強與此報告，失敗測試維持啟用，供後續修正使用。原 checkout `kafka-m1` 保持 `KAFKA-21501@9bef9f19f6`。
