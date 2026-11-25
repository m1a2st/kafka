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

# Partition deletion tools 盤點

盤點基準為 `delete-partition-test@6b95ada57d` 加上本輪修正；涵蓋 **43 個 Unix shell 入口、36 個 Windows wrappers**，以及共用 offset resetter、transactional copier 和 metadata shell view。具固定 Java／Scala target 的 Windows wrappers 與對應 Unix 入口一致；通用 launcher／process scripts 依平台保留不同實作。未在 Windows 執行。

並非每支工具都需要改程式。已確認需要修的 `StreamsResetter` CSV 問題已納入本輪；`TopicCommand` 與 reassignment verify 已有適配，其餘依操作契約區分 SDK 處理、直接相容、需要補測或診斷增強。未重現的風險不列為已確認 bug。

## 判斷基準

- Retained 是仍在 metadata 的所有 partitions，包含 DRAINING；active 才排除 draining。讀取／offset 查詢／leader election 不應一律改成只看 active。
- 此設計只移除 eligible 尾端，因此 retained IDs 仍為連續前綴。不能把所有 `0..count-1` 或 list-size 用法都判成 bug。
- 同 UUID 的已移除 ID 永不重用；舊 plan／明確指定 ID 不得靜默轉到其他存活 ID。reopen 僅限尚未物理移除的 partitions。
- 更新後 SDK 能處理一般新 producer routing 與支援的 consumer assignment；明確 partition、已選好 partition 的 batch、手動 assignment、cached topology 與輸出／reset plan 仍需由工具處理。

## 完整入口矩陣

「需補測」表示此輪確認了相關程式路徑，仍有特定情境需要額外驗證，不表示已證明程式出錯。

| 入口 | 分類 | 影響與後續 | Delegated class |
|---|---|---|---|
| `connect-distributed` | 啟動器相容／runtime 另驗 | 只啟動 Connect distributed runtime；source producer 與 sink consumer 仍受 SDK／assignor 升級要求約束，未將啟動器比對當作完整 connector 相容證明。 | [org.apache.kafka.connect.cli.ConnectDistributed](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/connect/runtime/src/main/java/org/apache/kafka/connect/cli/ConnectDistributed.java) |
| `connect-mirror-maker` | 啟動器相容／runtime 另驗 | MM2 入口沒有 partition 索引邏輯；來源／目標收縮與 offset-sync 行為屬 runtime 整合測試範圍，不應自動把刪除傳播到目標。 | [org.apache.kafka.connect.mirror.MirrorMaker](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorMaker.java) |
| `connect-plugin-path` | 無 partition 特別邏輯 | 掃描或管理 connector plugin 路徑，沒有 partition count／ID 或 offset 操作。 | [org.apache.kafka.tools.ConnectPluginPath](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ConnectPluginPath.java) |
| `connect-standalone` | 啟動器相容／runtime 另驗 | 同 distributed 入口；單機 task／connector 在 topic 收縮時仍須另驗。 | [org.apache.kafka.connect.cli.ConnectStandalone](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/connect/runtime/src/main/java/org/apache/kafka/connect/cli/ConnectStandalone.java) |
| `kafka-acls` | 無 partition 特別邏輯 | ACL 以 topic／group／cluster 等 resource 名稱處理；沒有 partition index 轉換。 | [org.apache.kafka.tools.AclCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/AclCommand.java) |
| `kafka-broker-api-versions` | 通用 API 相容 | 顯示 broker 的 API versions；新 API key 94 由更新後 ApiKeys／SDK 提供，不需硬編碼 partition 邏輯。 | [org.apache.kafka.tools.BrokerApiVersionsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/BrokerApiVersionsCommand.java) |
| `kafka-client-metrics` | 無 partition 特別邏輯 | 管理 client metric subscription，沒有 topic partition 拓撲操作。 | [org.apache.kafka.tools.ClientMetricsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ClientMetricsCommand.java) |
| `kafka-cluster` | 無 partition 特別邏輯 | cluster ID／broker 管理按 identity 呼叫 Admin；drain 後 broker fencing 行為由 server 負責。 | [org.apache.kafka.tools.ClusterTool](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ClusterTool.java) |
| `kafka-configs` | 通用設定相容 | 一般 topic config 路徑可讀寫 partition.drain.timeout.ms；設定 long／範圍驗證由 LogConfig 負責，現有 drain deadline 不因後續改設定重算。 | [org.apache.kafka.tools.ConfigCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ConfigCommand.java) |
| `kafka-console-consumer` | SDK 處理／需補測 | subscribe 模式由新版 assignor 撤銷已刪除 IDs；--partition 是明確手動 assignment，不可悄悄改成另一 ID，需清楚處理刪除後錯誤。 | [org.apache.kafka.tools.consumer.ConsoleConsumer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleConsumer.java) |
| `kafka-console-producer` | SDK 處理／需補測 | 一般未指定 partition 的新紀錄使用 drain-aware producer routing；已排入尾端的 batch 或自訂 reader 明確指定 ID 仍可能失敗，既有 callback／sync 錯誤要保留。 | [org.apache.kafka.tools.ConsoleProducer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ConsoleProducer.java) |
| `kafka-console-share-consumer` | SDK 處理／需補測 | share subscription／ack 交由新版 share consumer 與 coordinator；本輪真實 share physical-removal case 驗證下層行為，未另驗 CLI 顯示。 | [org.apache.kafka.tools.consumer.ConsoleShareConsumer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/consumer/ConsoleShareConsumer.java) |
| `kafka-consumer-groups` | 相容／需情境測試 | 以真實 assignment、committed-offset keys、明確 TopicPartition 操作；shared resetter 比對真實 IDs，不會把舊 CSV ID 轉成存活 ID。 | [org.apache.kafka.tools.consumer.group.ConsumerGroupCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/consumer/group/ConsumerGroupCommand.java) |
| `kafka-consumer-perf-test` | SDK 處理／需補測 | 使用 subscribe 與 rebalance listener，沒有固定 partition count；需加上 drain→removed 的吞吐／停等終止測試。 | [org.apache.kafka.tools.ConsumerPerformance](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ConsumerPerformance.java) |
| `kafka-delegation-tokens` | 無 partition 特別邏輯 | token identity 管理不涉及 partition 拓撲。 | [org.apache.kafka.tools.DelegationTokenCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/DelegationTokenCommand.java) |
| `kafka-delete-records` | 相容／需情境測試 | JSON 明確指定 TopicPartition；保留 draining partitions 可截斷，已移除 ID 應回既有 per-ID missing error，不可改寫其他 ID。 | [org.apache.kafka.tools.DeleteRecordsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/DeleteRecordsCommand.java) |
| `kafka-dump-log` | 通用 metadata decoder 相容 | cluster metadata decoder 使用 MetadataRecordSerde／MetadataJsonConverters，可處理新 record 及 tagged HWM／force；應使用同版 binary 讀取新格式。 | [org.apache.kafka.tools.DumpLogSegments](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/DumpLogSegments.java) |
| `kafka-e2e-latency` | 需補測／文件 | setupConsumer 在啟動時 discover 後手動 assign，沒有自行重建拓撲；需驗證收縮期間的讀取進度／timeout，尚未證實會產生錯誤結果。 | [org.apache.kafka.tools.EndToEndLatency](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/EndToEndLatency.java) |
| `kafka-features` | 通用 feature 相容 | 新 MetadataVersion33 透過通用 feature 管理；仍須遵守 unstable feature opt-in 及不可降版的 metadata-change fencing。 | [org.apache.kafka.tools.FeatureCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/FeatureCommand.java) |
| `kafka-get-offsets` | 相容／需情境測試 | 從實際 metadata IDs 枚舉與篩選；draining 仍可查 offsets，查詢時與移除競賽的 per-partition exception 已有錯誤處理。 | [org.apache.kafka.tools.GetOffsetShell](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/GetOffsetShell.java) |
| `kafka-groups` | 無 partition 特別邏輯 | 列出 group identity／type／protocol，不讀寫 partition count。 | [org.apache.kafka.tools.GroupsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/GroupsCommand.java) |
| `kafka-jmx` | 通用查詢相容 | 依使用者 MBean 名稱查詢，不硬編碼 drain 指標；原缺少的兩項指標已由本輪 server metrics 修正，非 JmxTool 路由問題。 | [org.apache.kafka.tools.JmxTool](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/JmxTool.java) |
| `kafka-leader-election` | 相容／需情境測試 | 明確 ID／JSON 或 all-partitions 模式；draining 可選 leader，已移除 ID 應維持 missing error，不需以 active count 排除仍可讀 replica。 | [org.apache.kafka.tools.LeaderElectionCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/LeaderElectionCommand.java) |
| `kafka-log-dirs` | 相容／需情境測試 | 直接列 broker 回傳的 TopicPartition→ReplicaInfo；輸出是 local live-log 狀態，不保證 .stray／.delete 檔案或遠端 objects 已物理清完。 | [org.apache.kafka.tools.LogDirsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/LogDirsCommand.java) |
| `kafka-metadata-quorum` | 無 topic partition 特別邏輯 | 查詢 Raft metadata quorum replicas／offsets，不把使用者 topic partition count 當作索引。 | [org.apache.kafka.tools.MetadataQuorumCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/MetadataQuorumCommand.java) |
| `kafka-metadata-shell` | 診斷顯示可增強 | MetadataLoader 可套用新 metadata records；TopicImageNode 目前只列 name／id／partition registrations，未顯示 drain deadlines／force／allocation HWM。這是診斷能力缺口，非已證明狀態套用錯誤。 | [org.apache.kafka.shell.MetadataShell](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/shell/src/main/java/org/apache/kafka/shell/MetadataShell.java) |
| `kafka-producer-perf-test` | SDK 處理／需補測 | ProducerRecord 不固定 partition，新紀錄走新版 producer；舊 batch 的 delivery error 仍要計入報告，不可將其當成 SDK 一定會重分區。 | [org.apache.kafka.tools.ProducerPerformance](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ProducerPerformance.java) |
| `kafka-reassign-partitions` | 已適配 | verify 已用真實 ID 查找，移除的 ID 回報 empty current replicas；generate 仍列 retained draining IDs，執行時由 server 拒絕這些 reassignment，需補測／文件說明部分結果。 | [org.apache.kafka.tools.reassign.ReassignPartitionsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/reassign/ReassignPartitionsCommand.java) |
| `kafka-replica-verification` | 需補測／明確拓撲限制 | 已 deprecated；啟動後保存 topic IDs、partition／replica／offset maps 與 barriers，沒有重新 discover。運行中收縮需測試明確停止或更新集合；不能僅靠新 SDK 宣稱會自動調整。 | [org.apache.kafka.tools.ReplicaVerificationTool](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ReplicaVerificationTool.java) |
| `kafka-run-class` | 啟動器無需修改 | 通用 JVM classpath／class launcher，沒有 partition 業務語意。 | 通用 launcher／process script |
| `kafka-server-start` | 啟動器無需修改 | 啟動 Kafka server，實際 drain／removal 邏輯在 broker/controller。 | [kafka.Kafka](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/Kafka.scala) |
| `kafka-server-stop` | 啟動器無需修改 | 停止 server process，沒有 partition 業務語意。 | 通用 launcher／process script |
| `kafka-share-consumer-perf-test` | SDK 處理／需補測 | share subscribe／poll／ack 沒有固定 ID 範圍；需覆蓋收縮後測量及沒有新紀錄時的結束條件。 | [org.apache.kafka.tools.ShareConsumerPerformance](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/ShareConsumerPerformance.java) |
| `kafka-share-groups` | 相容／需情境測試 | Admin describe／list／offset reset 以真實 IDs 與 shared resetter 操作；需 CSV 舊 ID、ack／state cleanup 後輸出測試。 | [org.apache.kafka.tools.consumer.group.ShareGroupCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/consumer/group/ShareGroupCommand.java) |
| `kafka-storage` | 通用 metadata-version 相容 | 儲存格式／metadata version 操作，沒有使用 topic count 重建 partition IDs；新 record 讀取仍需相容版本。 | [kafka.tools.StorageTool](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/core/src/main/scala/kafka/tools/StorageTool.scala) |
| `kafka-streams-application-reset` | 本輪修正 | 舊 CSV 含已移除 ID 或漏掉目前選定 ID 時，原本會 NPE／可能先 seek 部分資料；現在先比對完整 key set，清楚拒絕並保持所有 positions 不變。 | [org.apache.kafka.tools.StreamsResetter](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/StreamsResetter.java) |
| `kafka-streams-groups` | 相容／需情境測試 | assignment／offset 查詢與 reset 以真實 TopicPartition；需收縮後 source task／surplus changelog 及舊 CSV error 的命令整合測試。 | [org.apache.kafka.tools.streams.StreamsGroupCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/streams/StreamsGroupCommand.java) |
| `kafka-topics` | 已適配 | alter 分離 active／retained counts，支援刪除、force、reopen、assignment 驗證；describe 顯示 draining 狀態與 deadline。acceptance 不代表已物理移除。 | [org.apache.kafka.tools.TopicCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/TopicCommand.java) |
| `kafka-transactions` | 相容／需情境測試 | find-hanging 依 retained partitions 查詢，這對診斷阻擋 drain 的交易是正確行為；明確 removed ID 保留原有 Admin error／marker 收斂規則。 | [org.apache.kafka.tools.TransactionsCommand](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/TransactionsCommand.java) |
| `kafka-verifiable-consumer` | SDK 處理／需補測 | subscribe 與 assignment／revocation callbacks 記錄實際 IDs；需檢查 consumer protocol／classic shrink 後事件與 offset 輸出。 | [org.apache.kafka.tools.VerifiableConsumer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/VerifiableConsumer.java) |
| `kafka-verifiable-producer` | SDK 處理／需補測 | 一般新 send 沒有明確 partition，使用更新 producer；success／error 事件需保留真正的 retry／delivery 結果。 | [org.apache.kafka.tools.VerifiableProducer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/VerifiableProducer.java) |
| `kafka-verifiable-share-consumer` | SDK 處理／需補測 | share subscribe／ack 的 TopicIdPartition 已保留 UUID，沒有固定 count 迴圈；需收縮事件及舊 ack 的命令測試。 | [org.apache.kafka.tools.VerifiableShareConsumer](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/VerifiableShareConsumer.java) |
| `trogdor` | 啟動器相容／workload 另驗 | 入口只啟動 agent/coordinator；各 workload 可能自行固定 partition／目標數，未將 launcher 相容視作所有 workload 已支援 shrink。 | [org.apache.kafka.trogdor.agent.Agent](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/trogdor/src/main/java/org/apache/kafka/trogdor/agent/Agent.java)、[org.apache.kafka.trogdor.coordinator.Coordinator](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/trogdor/src/main/java/org/apache/kafka/trogdor/coordinator/Coordinator.java)、[org.apache.kafka.trogdor.coordinator.CoordinatorClient](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/trogdor/src/main/java/org/apache/kafka/trogdor/coordinator/CoordinatorClient.java)、[org.apache.kafka.trogdor.agent.AgentClient](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/trogdor/src/main/java/org/apache/kafka/trogdor/agent/AgentClient.java) |

## 已確認並修正的工具缺口

`StreamsResetter.resetOffsetsFromResetPlan` 先查目前 input partitions 的 earliest／latest offsets，卻對舊 CSV 的全部 key 取值。4→2 後，CSV 中 2／3 的 map lookup 會得到 null 再 unbox 成 long。若 CSV 反而漏掉目前 partition，舊程式可能先 seek 合法 partition，之後才在缺漏項目 NPE。

本輪在任何 consumer offset query／seek 之前驗證計畫 key set 必須與目前選定 input partitions 完全一致，錯誤列出 missing 與 unexpected IDs；不靜默丟棄或重分配舊 ID。合法計畫的 bounds handling 與 seeks 保持原樣，`--from-file` help 同步說明。

證據：原 public-method probe 的 retained-only control 通過、stale plan 重現 NPE；兩個新增 MockConsumer regression 先確認因 NPE 失敗，再驗證具體拒絕訊息與原 positions40／50 不變。第三個控制案例覆蓋完整 retained set0..3；它代表 retained IDs，不是額外的 CLI／broker draining 整合測試。

- [StreamsResetter.java](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/main/java/org/apache/kafka/tools/StreamsResetter.java:384)
- [StreamsResetterTest.java](/Users/al02475442/Desktop/java_project/kafka-delete-partition-review/tools/src/test/java/org/apache/kafka/tools/StreamsResetterTest.java)
- [原 public-method 重現 log](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/streams-resetter-tool-probe.log)
- [RED 測試結果](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/tools-cleanup-red-summary.json)

## 非 shell 入口與診斷項目

| 元件 | 結論 | 後續 |
|---|---|---|
| `TransactionalMessageCopier` | 明確把來源 `record.partition()` 複製到同一目標 ID，不能用一般 producer 自動 reroute 契約解讀。目標單獨 shrink 或 input／output count 不一致時需處理 abort／重試，尚未列為本輪已重現新 bug。 | 補目標移除時的失敗／終止測試，明確說明來源／目標 partition 對應前提；不可悄悄改變 copy identity。 |
| `GroupOffsetsResetter` | 被三種 group commands 共用，以真實 ID 驗證存在與查詢 offsets；與 StreamsResetter 是不同路徑，沒有上述同一個 NPE 證據。 | 加 CSV export→drain→remove→replay 的命令整合測試，確定失敗時 surviving offsets 不變。 |
| `TopicImageNode`／metadata shell | 正確的新版 image 能保留 drain／force／HWM，但 view 不直接顯示這些欄位。 | 若要用 shell 診斷 deletion，可新增只讀節點與 snapshot-render tests；目前可用 dump-log metadata decoder 看原始 records。 |
| `ReplicaVerificationTool` | 啟動快照及固定 barriers 需要穩定拓撲假設；本輪沒把未驗證的 shrink runtime 行為宣稱為已支援。 | 優先測試 running shrink／removed fetch response，選擇同步更新集合或明確提示重啟；該工具已 deprecated。 |

## 建議優先補測

1. reassignment generate→execute 遇到 draining IDs：確認 retained IDs、active 部分結果及 draining 的 `PARTITION_OPERATION_IN_PROGRESS`，再驗證 removed ID 的 stale verify。
2. consumer／share／Streams group commands 的舊 CSV：retained reset、physical removal 後 describe、過期 plan 拒絕且沒有 surviving-offset mutation。
3. console／verifiable consumers 的 subscribe 與 `--partition` 模式分開測；後者不能自動換 ID。
4. producer／performance tools：區分 refresh 後的新 send 成功與原 batch 失敗；不要把失敗 callback 當成已重分區成功。
5. e2e latency／replica verification／transactional copier：測試快照或明確 ID 的刪除與有限時間終止，避免靜默重試。
6. log-dirs／transactions／get-offsets：在 draining、removed 以及發生 discovery/query 競賽時核對 per-ID 錯誤與保留項目的輸出。
7. metadata shell：新 snapshot 的 drain／force／HWM rendering；舊 binary 讀新格式須明確版本限制。

本盤點沒有要求一次改動所有工具，也沒有把修改共用 SDK 視作所有 CLI 情境都已測過。Connect／MM2／Trogdor 的完整 runtime 或外掛行為沒有在這份入口盤點中重新審查；它們需要自己的端到端情境測試。

## 驗證狀態

本輪 `tools:test` 已完成 **229 個案例，0 failures／errors／skips**，包含下列實際執行的 suites。靜態盤點與執行證據分開記錄；新增工具程式只限已重現且有回歸測試的 StreamsResetter 修正。

| 實際執行的 tool suite | 案例 |
|---|---:|
| `org.apache.kafka.tools.ConsoleProducerTest` | 21 |
| `org.apache.kafka.tools.ConsumerPerformanceTest` | 20 |
| `org.apache.kafka.tools.EndToEndLatencyTest` | 10 |
| `org.apache.kafka.tools.GetOffsetShellParsingTest` | 16 |
| `org.apache.kafka.tools.ProducerPerformanceTest` | 34 |
| `org.apache.kafka.tools.ReplicaVerificationToolTest` | 1 |
| `org.apache.kafka.tools.ShareConsumerPerformanceTest` | 13 |
| `org.apache.kafka.tools.StreamsResetterTest` | 22 |
| `org.apache.kafka.tools.TopicCommandDeletionReviewTest` | 5 |
| `org.apache.kafka.tools.TopicCommandDrainingTest` | 20 |
| `org.apache.kafka.tools.TopicCommandForceTest` | 12 |
| `org.apache.kafka.tools.TransactionsCommandTest` | 29 |
| `org.apache.kafka.tools.reassign.ReassignPartitionsUnitTest` | 26 |

[逐類別原始計數](/private/tmp/kafka-delete-partition-rereview-fixes-20261125/tools-executed-classes.json)。本輪沒有額外執行 GetOffsetShell／LogDirs／LeaderElection 的完整工具專屬 KRaft suites，不能將其靜態相容結論當成那些工具的 physical-removal 整合測試已完成。最終全元件測試結果見[修正報告](2026-11-25-delete-partition-test-rereview-remediation.md)。
