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

# Delete Partitions Public Interface Coverage Audit

本次由整隻 `delete-partition-test` 的實作 diff 反查 KIP Public Interfaces，補齊 RPC、工具、
Java SDK、設定、authorization／quota 與 metrics 的介面定義。章節沿用 KIP-1279 的安排。

比對基準為 merge base `da3b5e78ed2b3d5a9efec85760df7ff2ea6ad2cb` 至
`939047d520149b30f6f9ab10eab964d44fbf28d1`；後者是本次文件補全前的分支 HEAD。
審查涵蓋新增／變更的 schema 與對既有 RPC、CLI 可觀察的行為，不以檔案中出現 `public`
關鍵字取代 SDK／wire 介面範圍判定。

## RPC 與 metadata schema 對照

| 實作 schema | KIP 定義 |
|-------------|----------|
| [BrokerHeartbeatRequest.json](../../clients/src/main/resources/common/message/BrokerHeartbeatRequest.json) | [schema／版本與欄位](../../KIP.md#broker-heartbeat-readiness) |
| [DeletePartitionsRequest.json](../../clients/src/main/resources/common/message/DeletePartitionsRequest.json) | [schema／版本與欄位](../../KIP.md#deletepartitionsrequest-v0) |
| [DeletePartitionsResponse.json](../../clients/src/main/resources/common/message/DeletePartitionsResponse.json) | [schema／版本與欄位](../../KIP.md#deletepartitionsresponse-v0) |
| [DescribeTopicPartitionsRequest.json](../../clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json) | [schema／版本與欄位](../../KIP.md#protocol-changes) |
| [DescribeTopicPartitionsResponse.json](../../clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json) | [schema／版本與欄位](../../KIP.md#describetopicpartitionsresponse-v0--v1) |
| [MetadataRequest.json](../../clients/src/main/resources/common/message/MetadataRequest.json) | [schema／版本與欄位](../../KIP.md#protocol-changes) |
| [MetadataResponse.json](../../clients/src/main/resources/common/message/MetadataResponse.json) | [schema／版本與欄位](../../KIP.md#metadataresponse-v13--v14) |
| [PartitionDrainingRecord.json](../../metadata/src/main/resources/common/metadata/PartitionDrainingRecord.json) | [schema／版本與欄位](../../KIP.md#partitiondrainingrecord-record-id-29) |
| [RemovePartitionRecord.json](../../metadata/src/main/resources/common/metadata/RemovePartitionRecord.json) | [schema／版本與欄位](../../KIP.md#removepartitionrecord-record-id-30) |
| [TopicRecord.json](../../metadata/src/main/resources/common/metadata/TopicRecord.json) | [schema／版本與欄位](../../KIP.md#topicrecord) |

七份 client message schema 與三份 metadata schema 全部列入。六個完整 JSON 定義與三個新增
response fields 逐項比對 type、version、flexible version、tag、default、nullable、ignorable、
mapKey 等結構屬性；`about` 文字保留 KIP 對最早 deadline 與安全條件的精確說明。

另在 [Existing RPC Semantics](../../KIP.md#existing-rpc-semantics) 明列既有 schema 下的行為：
CreatePartitions；Produce；Fetch／ListOffsets；WriteTxnMarkers；OffsetCommit／TxnOffsetCommit／
OffsetFetch；AlterPartitionReassignments；JoinGroup／SyncGroup／ConsumerGroupHeartbeat；
ShareGroupHeartbeat；ShareFetch／ShareAcknowledge／share-state RPCs；StreamsGroupHeartbeat；
DeleteTopics；ElectLeaders。設定與 quota 管理沿用的 RPC 也在對應章節具名列出。

## Java SDK 對照

範圍依 `build.gradle` 的 clients Javadoc include 與既有公開 package 判定。

| 公開型別／介面 | 新增或變更契約 | KIP 定義 |
|---------------|----------------|----------|
| `Admin` | 兩個 deletePartitions overload，default／abstract 邊界 | [Admin Deletion Methods](../../KIP.md#admin-deletion-methods) |
| `DeletePartitionsOptions` | constructor、完整 fluent／getter 簽章、繼承 timeout、預設值 | [Options](../../KIP.md#deletepartitionsoptions) |
| `DeletePartitionsResult` | typed per-topic futures／all future、非公開 constructor | [Result](../../KIP.md#deletepartitionsresult) |
| `KafkaAdminClient` | 新增公開實作方法 | [Implementations](../../KIP.md#admin-implementations-and-forwardingadmin) |
| `ForwardingAdmin` | 新增委派 override、便利方法的繼承 | [Implementations](../../KIP.md#admin-implementations-and-forwardingadmin) |
| `NewPartitions` | 既有 factories／getters 的 active target 與 physically new assignments 語意 | [Reopening](../../KIP.md#reopening-with-createpartitions) |
| `TopicPartitionInfo` | 新 constructor／accessors，ELR null、deadline、equality／hash／string | [Partition Descriptions](../../KIP.md#partition-descriptions) |
| `TopicConfig` | CONFIG／DOC／DEFAULT 三個公開常數 | [Topic Configuration](../../KIP.md#topic-configuration) |
| `PartitionOperationInProgressException` | 兩個公開 constructor、ApiException 基底 | [Error Codes](../../KIP.md#error-codes) |
| `InvalidDeletePartitionCountException` | 兩個公開 constructor、ApiException 基底 | [Error Codes](../../KIP.md#error-codes) |

[Admin Source and Binary Compatibility](../../KIP.md#admin-source-and-binary-compatibility)
補上自訂 Admin implementor 必須實作新增抽象方法，以及舊實作被呼叫新方法時可能出現
`AbstractMethodError` 的範圍；既有 caller 方法與 ForwardingAdmin 繼承行為分別說明。

## 工具、設定與 metrics 對照

| 實作範圍 | 契約補全 | KIP 定義 |
|----------|----------|----------|
| `TopicCommand` | 參數型別／預設、force 組合、active／retained count、preflight 與跨 topic 部分成功、describe／error output | [Topic Management](../../KIP.md#topic-management) |
| `StreamsResetter` | from-file help、CSV exact key set、missing／unexpected diagnostics、offset query／seek 前拒絕、既有 skip 與 force side-effect 邊界 | [Streams Application Reset](../../KIP.md#streams-application-reset) |
| `ReassignPartitionsCommand` | verify 依 ID 查找、removed ID 的空 replica mismatch／terminal 狀態、throttle cleanup | [Partition Reassignment Verification](../../KIP.md#partition-reassignment-verification) |
| Controller request admission | Topic ALTER、strict quota v0、validate-only quota、targeted retained tail 計費 | [Authorization and Mutation Quotas](../../KIP.md#authorization-and-mutation-quotas) |
| `TopicConfig`／`LogConfig` | LONG／range／default／importance、dynamic topic scope、無新 broker-default synonym、既有 delete.topic.enable 的實際範圍 | [Configuration](../../KIP.md#configuration) |
| Controller metrics | 三個新 metric 的 committed／active／process-local 生命週期 | [New Controller Metrics](../../KIP.md#new-controller-metrics) |
| Request metrics | 十個標準 metric family、version／error tags 與完整 MBean | [Standard Request Metrics](../../KIP.md#standard-request-metrics) |
| Existing metrics | Topic writes 與 active partitions 的 contribution、local／global counts、broker fetcher lag scope | [Existing Metrics Behavior](../../KIP.md#existing-metrics-behavior) |

三支工具的 Unix 與 Windows wrappers 共用上述行為；wrapper 本身沒有新增參數格式。
較完整的工具相容性與尚待補測範圍保留在[工具影響盤點](2026-11-25-delete-partition-tools-inventory.md)。

## 修正的契約落差

- 刪除回應的 `Results.Name` 沒有 `mapKey=true`，只在 request topics 具有 keyed name。
- Error 137 驗證的是絕對 `Count` 與 physical retained count，而不是 CLI 的相對 deleteCount。
- Force RPC 可只升級既有 draining tail 的子集，不會重開其他 drain；CLI 採較嚴格的 active target 驗證。
- 已 forced 的 retained IDs 可不產生新 records，但重複 force 與 validate-only 仍評估 mutation quota。
- Readiness 補齊 ISR／effective min ISR／HW=LEO、重複／遺漏 status、expired heartbeat 與 offset 上下界。
- `delete.topic.enable` 在目前實作控制 whole-topic deletion，DeletePartitions 不檢查此開關；KIP 揭露實際 gate。
- Request metric 使用標準名稱，RequestsPerSec 具 version tag；topic-level MessagesInPerSec 在部分 tail draining 時不保證歸零。
- CSV 拒絕保證不部分 reset offsets；既有 no-input skip 與 reset-tool force 移除 member 的邊界明列。

## 驗證與範圍

本次執行文件／原始碼契約核對、JSON 結構比對、Java 簽章與常數檢查、CLI diagnostics／metrics
核對、章節／table／連結／whitespace 檢查，並由三個獨立範圍 reviewer 重審。實作與測試檔案
沒有變更，因此沒有為這次文件補全重新執行 Gradle。KIP 的既有 3,223 個通過案例驗證紀錄維持
其原始範圍，不新增 runtime test 通過宣稱。

本次文件與檢查證據位於 `/private/tmp/kafka-delete-partition-public-interfaces-20261125/`。
文件補全以一個本地文件 commit 納入 `delete-partition-test`，未 push 或 merge。
