# KIP-XXX: Delete Partitions (Number Assigned on Submission)

## Overview

A new `DeletePartitions` API (API key 94) that allows reducing the partition count of a topic by removing partitions
from the tail. Partitions enter a "draining" state (stop accepting writes, allow reads) before being removed after a
configurable minimum interval and a transaction-safety check, unless explicitly forced.

This requires:

- 1 new API (request + response): `DeletePartitions` (API key 94)
- 2 new metadata records: `PartitionDrainingRecord` (ID 29), `RemovePartitionRecord` (ID 30)
- 1 new MetadataVersion: `IBP_4_4_IV2`
- 2 new error codes: `PARTITION_OPERATION_IN_PROGRESS` (136), `INVALID_DELETE_PARTITION_COUNT` (137)
- 1 new topic config: `partition.drain.timeout.ms`
- Version bumps to: `MetadataResponse` (v14), `DescribeTopicPartitionsResponse` (v1)

## Motivation

### The Problem

Kafka supports increasing the partition count of a topic (`CreatePartitions` API) but provides no mechanism to decrease
it. This asymmetry has been a known limitation since 2012 (KAFKA-347) and was explicitly requested as a feature in 2014
(KAFKA-1231, resolved "Won't Fix" in 2020). The StackOverflow question "How to decrease number partitions Kafka topic?"
has accumulated 37 votes and over 50,000 views, indicating real-world demand.

The only workaround today is to delete and recreate the topic, which causes:

- Complete data loss of all unconsumed messages
- Consumer group offset reset across all consumer groups
- Hard failures in downstream systems (Kafka Streams, Connect, MirrorMaker)
- Loss of ACLs, configs, and quotas that must be manually re-applied

### Why Kafka Never Supported This

The commonly cited reason — "reducing partitions causes data loss" — is imprecise. The true difficulty is that Kafka
guarantees message ordering within each partition but not across partitions. If data in a removed partition were
redistributed to remaining partitions, the ordering guarantee of the target partitions would be violated. There is no
way to merge messages from partition N into partition M while preserving the total order of both.

However, this analysis only applies to a design that attempts to **redistribute data** from removed partitions. It does
not apply to a design that simply **removes partitions and lets their data expire**.

### Why Partition Removal Is Symmetric With Partition Expansion

Partition expansion (3 → 5) works as follows:

- Existing partitions 0, 1, 2 are untouched — data and ordering preserved
- New partitions 3, 4 are created empty
- Future messages route via `hash(key) % 5` instead of `hash(key) % 3`
- Key affinity breaks: a key previously routed to partition 1 may now route to partition 4

Our partition removal (5 → 3) works identically in reverse:

- Remaining partitions 0, 1, 2 are untouched — data and ordering preserved
- Partitions 3, 4 enter a draining state (reject writes, allow reads until retention expires)
- Future messages route via `hash(key) % 3` instead of `hash(key) % 5`
- Key affinity breaks: a key previously routed to partition 4 must now route elsewhere

| Property | Expansion (3→5) | Removal (5→3) |
|----------|-----------------|---------------|
| Existing partition data | Untouched | Untouched |
| Ordering guarantee | Preserved | Preserved |
| Key affinity | Breaks | Breaks |
| Data loss | None | Removed partition data is deleted after the drain |

The only additional cost of removal is that historical data in the draining partitions will eventually be lost after
the retention period. This is an explicit, operator-controlled trade-off — not an ordering violation.

### Use Cases

1. **Capacity right-sizing** — A topic was provisioned with 256 partitions for anticipated load that never
   materialized. Each idle partition still costs ~1 MB heap, file handles, and ISR heartbeat overhead.
2. **Over-partition recovery** — An operator accidentally set partition count too high. Today the only fix is to
   delete the entire topic and recreate it, disrupting all producers and consumers.
3. **Cost reduction in cloud environments** — Partition count directly affects billing in managed Kafka offerings.
   Reducing unused partitions saves cost without service interruption.
4. **Post-peak scale-down** — A topic was scaled up for a seasonal traffic peak and should be scaled back down
   afterward, mirroring how brokers and consumer instances are scaled down.
5. **Multi-tenant platform management** — Platform operators need to reclaim resources from tenants whose usage has
   decreased, without forcing a destructive topic recreation.

---

## Public Interfaces

### New API: DeletePartitions (API Key 94)

#### DeletePartitionsRequest.json

File: `clients/src/main/resources/common/message/DeletePartitionsRequest.json`

```json
{
  "apiKey": 94,
  "type": "request",
  "listeners": [
    "broker",
    "controller"
  ],
  "name": "DeletePartitionsRequest",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "Topics",
      "type": "[]DeletePartitionsTopic",
      "versions": "0+",
      "about": "Each topic from which we want to delete partitions.",
      "fields": [
        {
          "name": "Name",
          "type": "string",
          "versions": "0+",
          "mapKey": true,
          "entityType": "topicName",
          "about": "The topic name."
        },
        {
          "name": "Count",
          "type": "int32",
          "versions": "0+",
          "about": "The new reduced partition count. For a topic with 10 partitions, Count=7 drains partitions 7, 8 and 9."
        },
        {
          "name": "Force",
          "type": "bool",
          "versions": "0+",
          "taggedVersions": "0+",
          "tag": 0,
          "default": "false",
          "about": "Whether to bypass transaction safety checks after the drain deadline."
        }
      ]
    },
    {
      "name": "TimeoutMs",
      "type": "int32",
      "versions": "0+",
      "about": "The time in ms to wait for controller acceptance, separate from the configured minimum drain interval."
    },
    {
      "name": "ValidateOnly",
      "type": "bool",
      "versions": "0+",
      "default": "false",
      "about": "Whether to validate without changing metadata."
    }
  ]
}
```

#### DeletePartitionsResponse.json

File: `clients/src/main/resources/common/message/DeletePartitionsResponse.json`

```json
{
  "apiKey": 94,
  "type": "response",
  "name": "DeletePartitionsResponse",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "ThrottleTimeMs",
      "type": "int32",
      "versions": "0+",
      "about": "The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota."
    },
    {
      "name": "Results",
      "type": "[]DeletePartitionsTopicResult",
      "versions": "0+",
      "about": "The partition deletion results for each topic.",
      "fields": [
        {
          "name": "Name",
          "type": "string",
          "versions": "0+",
          "mapKey": true,
          "entityType": "topicName",
          "about": "The topic name."
        },
        {
          "name": "ErrorCode",
          "type": "int16",
          "versions": "0+",
          "about": "The result error, or zero if there was no error."
        },
        {
          "name": "ErrorMessage",
          "type": "string",
          "versions": "0+",
          "nullableVersions": "0+",
          "default": "null",
          "about": "The result message, or null if there was no error."
        }
      ]
    }
  ]
}
```

#### API Key Registration

File: `clients/src/main/java/org/apache/kafka/common/protocol/ApiKeys.java`

```java
DELETE_PARTITIONS(ApiMessageType.DELETE_PARTITIONS, false,true),  // key 94, forwardable to controller
```

The API is forwardable (`true`) because brokers forward the request to the active controller, consistent with
`CreatePartitions` and `DeleteTopics`.

---

### Modified API: MetadataResponse Version Bump (v13 → v14)

File: `clients/src/main/resources/common/message/MetadataResponse.json`

Changes:

- Bump `validVersions` from `"0-13"` to `"0-14"`
- Add comment: `// Version 14 adds IsDraining to partition metadata for partition deletion support.`

New field under `MetadataResponsePartition`:

```json
{
  "name": "IsDraining",
  "type": "bool",
  "versions": "14+",
  "default": "false",
  "ignorable": true,
  "about": "True if this partition is draining and eligible for removal after its deadline and transaction safety checks. Draining partitions do not accept produce requests."
}
```

**Backward compatibility behavior:**

| MetadataResponse version | Draining partition handling                                                                                                                             |
|:------------------------:|---------------------------------------------------------------------------------------------------------------------------------------------------------|
|          v0–v13          | Draining partitions **excluded** from partition list entirely. Old producers see reduced partition count and rehash keys immediately.                   |
|           v14            | Draining partitions **included** with `IsDraining=true`. New producers explicitly skip them. This allows clients to observe and log the draining state. |

File: `clients/src/main/resources/common/message/MetadataRequest.json`

- Bump `validVersions` to include v14 (matching response). No new request fields.

---

### Modified API: DescribeTopicPartitionsResponse Version Bump (v0 → v1)

File: `clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json`

Changes:

- Bump `validVersions` from `"0"` to `"0-1"`

New fields under `DescribeTopicPartitionsResponsePartition`:

```json
{
  "name": "IsDraining",
  "type": "bool",
  "versions": "1+",
  "default": "false",
  "ignorable": true,
  "about": "True if this partition is in the draining state."
},
{
"name": "DrainDeadlineMs",
"type": "int64",
"versions": "1+",
"default": "-1",
"ignorable": true,
"about": "The earliest removal timestamp in ms, or -1 if not draining. Transaction safety may delay removal."
}
```

File: `clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json`

- Bump `validVersions` to include v1 (matching response). No new request fields.

---

### Broker Heartbeat Readiness

`BrokerHeartbeatRequest` adds `DrainingPartitions`, a flexible tagged array (tag 2, versions 0+, empty by default)
of `PartitionDrainStatus`. The fields are `TopicId`, `PartitionId`, `LeaderEpoch`, `PartitionEpoch`,
`MetadataOffset` (default -1), `Ready` (default false), and `BlockedReason` (default empty string). Each heartbeat
reports the draining partitions currently led by that broker. Missing, stale or blocked reports cannot authorize
normal removal. This adds no Describe response fields: `IsDraining` and `DrainDeadlineMs` remain the public
partition-status fields, while controller logs expose blockers.

### New AdminClient Methods

The Admin API accepts reduced target counts:

```java
default DeletePartitionsResult deletePartitions(Map<String, Integer> partitionCounts);
DeletePartitionsResult deletePartitions(Map<String, Integer> partitionCounts,
                                       DeletePartitionsOptions options);
```

`DeletePartitionsOptions` provides `validateOnly(boolean)` and `force(boolean)` with matching getters; both
default to `false`. `force(true)` sets each requested topic's tagged `Force` field. For example, reducing a
ten-partition topic to seven with the explicit transaction-safety override is:

```java
admin.deletePartitions(Map.of("orders", 7), new DeletePartitionsOptions().force(true)).all().get();
```

The result's `values()` and `all()` futures complete when the controller accepts and persists the drain or force
upgrade. They do not wait for the deadline, transaction completion, removal, or storage cleanup. Without force,
a drain can remain pending indefinitely when the controller cannot obtain a fresh safe report from its leader.
With force, the configured minimum interval still applies, and unresolved transactional data may be discarded.

---

### New Topic Configuration

File: `clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java`

```java
public static final String PARTITION_DRAIN_TIMEOUT_MS_CONFIG = "partition.drain.timeout.ms";
public static final String PARTITION_DRAIN_TIMEOUT_MS_DOC = "The time in milliseconds to wait " +
        "after marking tail partitions as draining before removal becomes eligible. During this window, " +
        "consumers can continue to read from draining partitions but producers cannot write to them. " +
        "Set this value high enough for all consumer groups to drain their lag. " +
        "A value of 0 removes the minimum wait; transaction safety still applies unless force is explicit.";
public static final long PARTITION_DRAIN_TIMEOUT_MS_DEFAULT = 3600000L; // 1 hour
```

Config definition registration:

```java
.define(TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_CONFIG,
        ConfigDef.Type.LONG,
        TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_DEFAULT,
        atLeast(0),

ConfigDef.Importance.MEDIUM,
TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_DOC)
```

**Notes:**

- `atLeast(0)` validation — 0 is valid and means no minimum wait; transaction safety still applies unless force is explicit
- This is a dynamic topic-level config; can be altered via `AlterConfigs` without broker restart
- Config is only read at the time `DeletePartitionsRequest` is processed; changing it after draining starts has no
  effect on the existing deadline

---

### New Error Codes

File: `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`

```java
PARTITION_OPERATION_IN_PROGRESS(136,
                                        "A partition operation is already in progress for this topic.",
                                PartitionOperationInProgressException::new),

INVALID_DELETE_PARTITION_COUNT(137,
                                       "The requested delete partition count is invalid.",
                               InvalidDeletePartitionCountException::new),
```

#### Error Codes Summary

| Code | Name | Retriable | Returned By | Condition |
|:----:|------|:---------:|-------------|-----------|
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | Yes | `DeletePartitions` | Topic does not exist |
| 6 | `NOT_LEADER_OR_FOLLOWER` | Yes | `Produce` | Produce request targets a draining partition |
| 29 | `TOPIC_AUTHORIZATION_FAILED` | No | `DeletePartitions` | Caller lacks ALTER permission on topic |
| 35 | `UNSUPPORTED_VERSION` | No | `DeletePartitions` | MetadataVersion < IBP_4_4_IV2 |
| 42 | `INVALID_REQUEST` | No | `DeletePartitions` | Topic is internal (`__consumer_offsets`, etc.) |
| 89 | `THROTTLING_QUOTA_EXCEEDED` | Yes | `DeletePartitions` | Controller mutation quota exhausted |
| 136 | `PARTITION_OPERATION_IN_PROGRESS` | No | `DeletePartitions`, `AlterPartitionReassignments` | Conflicting existing drain or targeted reassignment; force may upgrade the same target |
| 137 | `INVALID_DELETE_PARTITION_COUNT` | No | `DeletePartitions` | deleteCount <= 0, OR deleteCount >= currentPartitionCount |

#### Exception Classes

File: `clients/src/main/java/org/apache/kafka/common/errors/PartitionOperationInProgressException.java`

```java
package org.apache.kafka.common.errors;

public class PartitionOperationInProgressException extends ApiException {
    public PartitionOperationInProgressException(String message) {
        super(message);
    }
}
```

File: `clients/src/main/java/org/apache/kafka/common/errors/InvalidDeletePartitionCountException.java`

```java
package org.apache.kafka.common.errors;

public class InvalidDeletePartitionCountException extends ApiException {
    public InvalidDeletePartitionCountException(String message) {
        super(message);
    }
}
```

---

### MetadataVersion Gate

File: `server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java`

```java
IBP_4_4_IV2(33,"4.4","IV2",false),  // Partition deletion support (DeletePartitions API)
```

Feature gate method:

```java
public boolean isDeletePartitionsSupported() {
    return this.isAtLeast(IBP_4_4_IV2);
}
```

The `false` stability flag marks this as unstable until the feature is production-ready.

---

## Proposed Changes

### Architecture Overview

```mermaid
graph TB
    subgraph Client Layer
        AdminClient[Admin Client / CLI]
        Producer[Producer]
        Consumer[Consumer Group]
    end

    subgraph Controller
        RCM[ReplicationControlManager]
        Scheduler[Controller periodic removal check]
        MetaLog[(KRaft Metadata Log)]
    end

    subgraph Broker
        ReplicaMgr[ReplicaManager]
        LogMgr[LogManager]
        RLM[RemoteLogManager]
        OffsetMgr[OffsetMetadataManager]
    end

    AdminClient -->|DeletePartitionsRequest| RCM
    RCM -->|Write PartitionDrainingRecord| MetaLog
    RCM -->|Check deadline and safety| Scheduler
    Scheduler -->|Eligible partitions| RCM
    RCM -->|Safe or explicitly forced: RemovePartitionRecord| MetaLog

    MetaLog -->|Metadata propagation| ReplicaMgr
    ReplicaMgr -->|isDraining check| Producer
    Producer -->|NOT_LEADER_OR_FOLLOWER| Producer
    ReplicaMgr -->|Serve fetch normally| Consumer
    ReplicaMgr -->|Heartbeat: readiness and BlockedReason| RCM

    ReplicaMgr -->|On removal| LogMgr
    LogMgr -->|asyncDelete| LogMgr
    ReplicaMgr -->|On removal| RLM
    RLM -->|Async remote cleanup| RLM
    ReplicaMgr -->|On removal| OffsetMgr
    OffsetMgr -->|Write tombstones| OffsetMgr
```

### New Metadata Records

#### PartitionDrainingRecord (ID 29)

File: `metadata/src/main/resources/common/metadata/PartitionDrainingRecord.json`

```json
{
  "apiKey": 29,
  "type": "metadata",
  "name": "PartitionDrainingRecord",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "TopicId",
      "type": "uuid",
      "versions": "0+",
      "about": "The unique ID of the topic whose partitions are being drained."
    },
    {
      "name": "PartitionId",
      "type": "int32",
      "versions": "0+",
      "about": "The partition entering or leaving the draining state."
    },
    {
      "name": "DeadlineMs",
      "type": "int64",
      "versions": "0+",
      "about": "Earliest removal time in epoch milliseconds; -1 cancels draining."
    },
    {
      "name": "Force",
      "type": "bool",
      "versions": "0+",
      "taggedVersions": "0+",
      "tag": 0,
      "default": "false",
      "about": "Whether removal may bypass transaction safety checks."
    }
  ]
}
```

#### RemovePartitionRecord (ID 30)

File: `metadata/src/main/resources/common/metadata/RemovePartitionRecord.json`

```json
{
  "apiKey": 30,
  "type": "metadata",
  "name": "RemovePartitionRecord",
  "validVersions": "0",
  "flexibleVersions": "0+",
  "fields": [
    {
      "name": "TopicId",
      "type": "uuid",
      "versions": "0+",
      "about": "The unique ID of the topic."
    },
    {
      "name": "PartitionId",
      "type": "int32",
      "versions": "0+",
      "about": "The partition ID to permanently remove. After replay, this partition no longer exists in any metadata image."
    }
  ]
}
```

**Design decision: Why two records instead of one?**

Separating draining initiation from removal provides:

1. **Crash safety** — If the controller crashes between writing `PartitionDrainingRecord` and the deadline, the new
   controller replays the record and restores which partitions are draining, their earliest removal times and force overrides. Fresh safety reports are required again.
2. **Auditability** — Operators can inspect the metadata log and see exactly when draining started and when removal
   happened.
3. **Cancellation** — CreatePartitions reopens retained partitions with PartitionDrainingRecord DeadlineMs=-1,
   preserving the existing log and canceling its removal deadline.

---

### State Machine

```mermaid
stateDiagram-v2
    [*] --> ACTIVE : PartitionRecord created
    ACTIVE --> DRAINING : DeletePartitionsRequest accepted\n(write PartitionDrainingRecord)
    DRAINING --> ACTIVE : CreatePartitions reopens retained ID\n(PartitionDrainingRecord DeadlineMs=-1)
    DRAINING --> REMOVED : deadline reached and safe, or forced\n(write RemovePartitionRecord)
    ACTIVE --> REMOVED : DeleteTopics\n(RemoveTopicRecord)
    DRAINING --> REMOVED : DeleteTopics\n(RemoveTopicRecord, implicit cancel)
    REMOVED --> [*]
```

**Invariant:** A partition is in exactly one state at any time. State is derived from metadata replay:

- No `PartitionDrainingRecord` for this partition → ACTIVE
- Latest `PartitionDrainingRecord` has non-negative DeadlineMs, no `RemovePartitionRecord` → DRAINING
- Latest `PartitionDrainingRecord` has DeadlineMs=-1 → ACTIVE again, retaining log and offsets
- `RemovePartitionRecord` written → REMOVED (partition no longer in TopicImage)

---

### Replica Lifecycle During Partition Deletion

This section defines how replicas (leader and followers) behave through each state transition, covering normal
operation, failure scenarios, and recovery. This is the most complex part of the design because it intersects with
replication, leader election, transactions, controlled shutdown, and log cleanup.

#### Design Principles

1. **Draining partitions maintain full replication** — Consumers need the partition to be available for reads;
   if the leader fails, a follower must be able to take over.
2. **Normal removal requires fresh leader confirmation** — The controller waits for transaction safety at the
   current broker, leader and partition epochs after the drain metadata is applied. Offline followers clean up later.
3. **The wall-clock deadline is an earliest removal time** — It never moves during outages, but normal removal
   waits beyond it while transactions or unavailable/stale leader reports block safety. Explicit force bypasses
   safety only after that same deadline.
4. **Transaction control records bypass draining write block** — To allow in-flight transactions to resolve cleanly.

#### During DRAINING: Replication Behavior

| Mechanism                           | Behavior                          | Rationale                                                                              |
|-------------------------------------|-----------------------------------|----------------------------------------------------------------------------------------|
| Follower fetch from leader          | **Continues normally**            | Leader failover requires caught-up followers                                           |
| ISR expansion (follower catches up) | **Allowed**                       | More ISR members = better availability during drain                                    |
| ISR shrink (follower falls behind)  | **Allowed**                       | Normal ISR mechanism; no special handling                                              |
| Leader epoch increment              | **Allowed**                       | If leadership changes, epoch must advance                                              |
| High watermark propagation          | **Continues**                     | Consumers rely on HW to determine readable data                                        |
| `min.insync.replicas` enforcement | **Unchanged** | Transaction markers retain acks=all and normal ISR checks |
| Preferred leader election           | **Allowed**                       | Consumer benefits from optimal read path                                               |
| Unclean leader election             | **Follows topic config**          | If `unclean.leader.election.enable=true`, allow; no override for draining              |

**Why maintain full replication during draining?**

If replication stopped during draining and the leader died, consumers would lose access to unconsumed data —
defeating the entire purpose of the drain period. The cost of maintaining idle replication (no new data flowing)
is negligible compared to the risk of data inaccessibility.

#### During DRAINING: Failure Scenarios

##### Scenario A: Leader Crashes

```
Partition 8: replicas=[B0*, B1, B2], ISR=[B0, B1, B2], state=DRAINING
                                                        (* = leader)
B0 crashes:
  1. Controller detects B0 fenced (heartbeat timeout)
  2. Controller elects new leader from ISR (e.g., B1)
  3. Writes PartitionChangeRecord { topicId, partitionId=8, leader=B1, leaderEpoch++ }
  4. B1 becomes leader; consumers reconnect to B1
  5. Draining state unchanged; drain deadline unchanged
  6. B2 continues fetching from new leader B1
```

Normal leader election still applies. The old readiness confirmation is invalidated; the new leader must apply the drain metadata and report fresh transaction safety.

##### Scenario B: All ISR Members Crash (Leader Included)

```
Partition 8: replicas=[B0*, B1, B2], ISR=[B0, B1], state=DRAINING
B0 and B1 both crash.

Case 1: unclean.leader.election.enable=true
  → Controller elects B2 (out-of-ISR) as leader
  → Consumers reconnect to B2 (may see fewer messages due to lag)
  → Draining continues

Case 2: unclean.leader.election.enable=false
  → No leader elected; partition becomes unavailable
  → Consumers cannot fetch → they will experience errors
  → Drain deadline continues ticking (NOT paused)
  → If deadline expires while partition is unavailable:
    - Normal drain waits for a recovered leader and a fresh transaction-safety confirmation
    - Explicit force permits RemovePartitionRecord despite the unavailable leader
    - After forced removal, recovered replicas replay the record and delete local logs
```

**The earliest removal time stays fixed during unavailability.** Normal removal cannot proceed without a
fresh safe leader confirmation. Operators may explicitly force the same drain, preserving its deadline and
accepting possible loss of unresolved transactional data.

##### Scenario C: Follower Crashes (Leader Alive)

```
Partition 8: replicas=[B0*, B1, B2], ISR=[B0, B1, B2], state=DRAINING
B2 crashes.

  1. ISR shrinks to [B0, B1] (normal ISR shrink mechanism)
  2. Consumer unaffected (still fetching from leader B0)
  3. No impact on draining behavior
  4. If B2 recovers before deadline: re-joins ISR, resumes fetching
  5. If B2 recovers after removal: finds stale log dir, deletes it
```

##### Scenario D: Follower Permanently Gone (Never Recovers)

```
Partition 8: replicas=[B0*, B1, B2], ISR=[B0, B1], state=DRAINING
B2 is decommissioned / never coming back.

  After RemovePartitionRecord is written:
  → B0 and B1 delete their local logs
  → B2's log dir remains as an orphan on its (dead) disk
  → No cluster impact; the data is simply abandoned on dead hardware
  → If B2's disk is later attached to a new broker:
    - New broker starts with fresh metadata
    - StalePartitionDirectoryDetector identifies orphan → schedules cleanup
```

#### During DRAINING: Controlled Shutdown

```
Partition 8: replicas=[B0*, B1, B2], ISR=[B0, B1, B2], state=DRAINING
B0 initiates controlled shutdown.

  1. B0 sends BrokerHeartbeat with wantShutDown=true
  2. Controller migrates leadership: writes PartitionChangeRecord { leader=B1 }
  3. B1 becomes new leader for partition 8
  4. Consumers reconnect to B1
  5. B0 shuts down; its replica data remains on disk
  6. Draining continues under B1's leadership

When B0 restarts:
  - If partition still draining: B0 resumes as follower, fetches from B1
  - If partition already removed: B0 finds stale dir, deletes it
```

Standard controlled shutdown leadership migration applies. A new leader must establish a fresh transaction-safety confirmation before normal removal.

#### During DRAINING: Transaction Control Records

COMMIT and ABORT markers use `AppendOrigin.COORDINATOR` and bypass the draining data-write
block. The actual `KafkaApis.handleWriteTxnMarkersRequest` path keeps
`TransactionCoordinator.EnforcedRequiredAcks = -1`: normal `min.insync.replicas` validation
and follower acknowledgement remain required. Draining does not reduce transaction durability.

If ISR is too small, the transaction coordinator retries. A normal drain remains blocked beyond its deadline
while transactions remain ongoing or their completion is not replicated. The leader must report readiness
after applying the drain metadata. An explicit force override permits removal after the original deadline even
with pending transactions; only then can metadata propagation cause the coordinator to drop the removed
partition from its pending marker set. Force does not relax marker replication or min ISR checks.

A stale or explicitly addressed `AddPartitionsToTxn` request can still reference a draining
partition. Metadata filtering improves new producer routing; the broker draining check is the
authoritative block on data writes. Existing transactions may send their control markers normally.

#### On REMOVAL: Broker-Side Replica Cleanup

```mermaid
flowchart TD
    A[RemovePartitionRecord received via MetadataDelta] --> B[BrokerMetadataPublisher.onMetadataUpdate]
    B --> C[ReplicaManager.applyDelta]
    C --> D[Stop fetchers\nReplicaFetcherManager.removeFetcherForPartitions]
    D --> E[Complete delayed operations\nDelayedProduce → error\nDelayedFetch → empty]
    E --> F[Partition.delete\nClear replica map, reset state, notify listeners]
    F --> G[Remove from allPartitions map]
    G --> H[LogManager.asyncDelete\nRename dir to .delete suffix]
    H --> I[Update checkpoint files\nrecovery-point, log-start-offset, HW]
    I --> J{remote.log.storage.enable?}
    J -->|Yes| K[RemoteLogManager.stopPartitions\nAsync remote segment deletion]
    J -->|No| L[Done]
    K --> L
```

When a broker receives `RemovePartitionRecord` via `MetadataDelta` → `BrokerMetadataPublisher.onMetadataUpdate()`:

```
ReplicaManager.applyDelta() processes the removal:

1. STOP FETCHERS (if this broker is a follower):
   - ReplicaFetcherManager.removeFetcherForPartitions({partition})
   - Removes partition from AbstractFetcherThread.partitionStates
   - Unregisters lag metrics for this partition

2. COMPLETE IN-FLIGHT DELAYED OPERATIONS:
   - Any DelayedProduce waiting on this partition → complete with error
   - Any DelayedFetch waiting on this partition → complete with empty response
   - Purgatory watchKeys for this partition removed

3. STOP PARTITION:
   - Partition.delete() called:
     a. Acquires leaderIsrUpdateLock (write)
     b. Clears remoteReplicasMap
     c. Resets assignmentState, log, futureLog
     d. Invokes onDeleted() listeners (RemoteLogManager cleanup)
     e. Removes partition-level metrics

4. REMOVE FROM REPLICA MAP:
   - allPartitions.remove(topicPartition)
   - Partition object becomes eligible for GC

5. ASYNC LOG DELETION:
   - LogManager.asyncDelete(topicPartition, checkpoint=true):
     a. Removes from currentLogs / futureLogs map
     b. Aborts any pending log cleaner tasks for this partition
     c. Renames log directory: "{topic}-{partition}" → "{topic}-{partition}.{uuid}-delete"
     d. Adds to logsToBeDeleted queue (background thread processes)
     e. Updates checkpoint files (recovery-point-offset, log-start-offset)
        to exclude the deleted partition

6. REMOTE STORAGE CLEANUP (if remote.log.storage.enable=true):
   - RemoteLogManager.stopPartitions(StopPartition(tp, deleteRemoteLog=true)):
     a. Schedules async deletion of remote log segments
     b. Cleans remote log metadata via RemoteLogMetadataManager
     c. May take significant time (cloud storage API calls)

7. CHECKPOINT FILE UPDATES:
   - recovery-point-offset-checkpoint: rewritten excluding this partition
   - log-start-offset-checkpoint: rewritten excluding this partition
   - replication-offset-checkpoint (HW): rewritten excluding this partition
   All three checkpoints updated atomically per log directory.
```

#### On REMOVAL: Offline Broker Recovery

```mermaid
flowchart TD
    A[Broker comes back online] --> B{RemovePartitionRecord\nstill in metadata log?}
    B -->|Yes: Case 1| C[Replay metadata log from last offset]
    C --> D[Replay PartitionDrainingRecord → mark draining]
    D --> E[Replay RemovePartitionRecord → remove from TopicImage]
    E --> F[ReplicaManager.applyDelta → standard cleanup]
    F --> G[asyncDelete local log dir]

    B -->|No: Case 2\nCompacted into snapshot| H[Load latest metadata snapshot]
    H --> I[Partition not present in image]
    I --> J[Scan local log dirs vs metadata image]
    J --> K[Detect orphan log directory]
    K --> L[Rename to .stray suffix]
    L --> M[Background cleanup]
```

When a broker that was offline during partition removal comes back:

```
Case 1: RemovePartitionRecord still in metadata log (not yet snapshotted)
  1. Broker connects to Raft cluster, replays metadata log from last known offset
  2. Replays PartitionDrainingRecord (may be present) → marks partition draining in local image
  3. Replays RemovePartitionRecord → removes partition from local TopicImage
  4. BrokerMetadataPublisher.onMetadataUpdate() called with delta showing partition removed
  5. ReplicaManager.applyDelta() processes removal → same cleanup as above
  6. If local log dir exists for removed partition → asyncDelete cleans it up

Case 2: RemovePartitionRecord already compacted into metadata snapshot
  1. Broker loads latest metadata snapshot → partition not present in image
  2. Broker scans local log directories vs metadata image
  3. Finds "orphan" log directory for partition that doesn't exist in metadata
  4. Marks as stale → renames to "{topic}-{partition}.{uuid}-stray"
  5. Stray directories cleaned up by background process
     (This is the existing "stale partition directory" detection mechanism)
```

#### On REMOVAL: Remote Storage Segment Lifecycle

For topics with tiered storage enabled (`remote.log.storage.enable=true`):

```
Timeline:
  T0: Partition is ACTIVE; some segments uploaded to remote storage
  T1: Partition enters DRAINING; no new data → no new remote uploads
      (existing uploaded segments remain accessible for consumer fetch)
  T2: Partition enters REMOVED
      → RemoteLogManager.stopPartitions(deleteRemoteLog=true)
      → Async remote segment deletion begins:
        a. Enumerate remote segments for this partition
        b. Delete each segment from remote storage (S3/GCS/HDFS)
        c. Delete remote log metadata entries
      → Deletion may take minutes/hours for large partitions
      → No impact on broker operation (async, fire-and-forget with retries)
```

**Edge case: Consumer fetching from remote storage during removal**

- After `RemovePartitionRecord` is replayed, the broker stops serving fetch requests for this partition
- Any in-flight remote fetch returns an error
- The consumer refreshes metadata and discovers the partition is gone
- Remote segments become inaccessible immediately from the client's perspective, even if the async
  deletion hasn't completed yet

#### Replica Lifecycle Summary Table

| Event                              | Leader Behavior                                                       | Follower Behavior                    | Controller Action                                                     |
|------------------------------------|-----------------------------------------------------------------------|--------------------------------------|-----------------------------------------------------------------------|
| Partition enters DRAINING          | Rejects data produce; allows control batches; continues serving fetch | Continues fetching from leader       | Writes PartitionDrainingRecord                                        |
| Leader crashes during DRAINING     | N/A (dead)                                                            | One follower elected leader from ISR | Writes PartitionChangeRecord (new leader)                             |
| All replicas crash during DRAINING | N/A                                                                   | N/A                                  | Normal removal waits for a fresh safe leader report; force may bypass safety after deadline |
| Controlled shutdown of leader      | Steps down leadership                                                 | New leader elected                   | Writes PartitionChangeRecord                                          |
| Drain deadline expires             | Still serving fetch until RemovePartitionRecord arrives               | Still fetching                       | Removes only when fresh safety confirmation is valid, or explicit force applies                            |
| RemovePartitionRecord replayed     | Stops serving; deletes log                                            | Stops fetching; deletes log          | N/A (already written)                                                 |
| Broker recovers after removal      | Finds stale dir; async delete                                         | Finds stale dir; async delete        | No action needed                                                      |

---

### Command Workflows

#### Delete Partitions (Happy Path)

```mermaid
sequenceDiagram
    participant Client as Admin Client
    participant Broker as Broker
    participant Controller as Active Controller
    participant Scheduler as Controller periodic check
    participant Replicas as Replica Brokers

    Client->>Broker: DeletePartitionsRequest(topic, targetCount, force=false)
    Broker->>Controller: Forward request
    Controller->>Controller: Validate (feature gate, auth, counts, no conflict)
    Controller->>Controller: Compute deadline = now + partition.drain.timeout.ms
    Controller->>Controller: Write PartitionDrainingRecord to metadata log
    Controller->>Scheduler: Track deadline and transaction safety
    Controller-->>Broker: DeletePartitionsResponse(success)
    Broker-->>Client: Response(success)

    Note over Replicas: Metadata propagation via Raft log
    Replicas->>Replicas: Replay PartitionDrainingRecord
    Replicas->>Replicas: Mark partitions as draining (reject produce, allow fetch)

    Replicas->>Controller: Heartbeat with applied metadata, matching epochs, Ready=true
    Note over Scheduler: At or after the minimum drain deadline
    Scheduler->>Controller: Check eligible removals
    Controller->>Controller: Verify fresh current-leader safety confirmation
    Controller->>Controller: Write RemovePartitionRecord (per partition)

    Note over Replicas: Metadata propagation
    Replicas->>Replicas: Replay RemovePartitionRecord
    Replicas->>Replicas: Stop fetchers, async delete logs, update checkpoints
```

#### Producer Convergence on Draining Partition

```mermaid
sequenceDiagram
    participant Producer as Producer
    participant Broker as Broker (leader)
    participant Meta as Metadata Service

    Note over Broker: Partition 8 enters DRAINING
    Producer->>Broker: Produce(partition=8, data)
    Broker-->>Producer: NOT_LEADER_OR_FOLLOWER
    Producer->>Meta: MetadataRequest (forced refresh)
    Meta-->>Producer: MetadataResponse (partition 8 excluded or IsDraining=true)
    Producer->>Producer: Repartition batch via hash(key) % activeCount
    Producer->>Broker: Produce(partition=2, data)
    Broker-->>Producer: Success
```

#### Consumer Rebalance on Partition Removal

```mermaid
sequenceDiagram
    participant Consumer as Consumer Group Member
    participant GC as Group Coordinator
    participant Broker as Broker

    Note over Broker: RemovePartitionRecord replayed
    Consumer->>Broker: FetchRequest(partition=8)
    Broker-->>Consumer: UNKNOWN_TOPIC_OR_PARTITION
    Consumer->>Consumer: requestMetadataUpdate()
    Consumer->>GC: Heartbeat / JoinGroup (detects partition gone)
    GC->>GC: Trigger rebalance (partition count changed)
    GC-->>Consumer: JoinGroupResponse (new assignment, without partition 8)
    Consumer->>Consumer: Resume consuming from remaining partitions
```

#### Transaction Resolution During Draining

```mermaid
sequenceDiagram
    participant Producer as Txn Producer
    participant Broker as Broker (leader P8)
    participant TxnCoord as Transaction Coordinator

    Note over Broker: Partition 8 enters DRAINING
    Producer->>Broker: Produce(partition=8, txn data)
    Broker-->>Producer: NOT_LEADER_OR_FOLLOWER
    Producer->>Producer: abortTransaction()
    Producer->>TxnCoord: EndTxnRequest(ABORT)
    TxnCoord->>TxnCoord: Transition T1 → PREPARE_ABORT
    TxnCoord->>Broker: WriteTxnMarkers(partition=8, ABORT)
    Note over Broker: Control batch bypasses draining check; normal acks=all and min ISR
    Broker->>Broker: Append ABORT marker to partition 8 log
    Broker-->>TxnCoord: Success
    TxnCoord->>TxnCoord: T1 → COMPLETE_ABORT
```

---

### Controller Behavior

#### Processing DeletePartitionsRequest

The controller resolves and authorizes each topic, checks the metadata-version gate, rejects internal topics,
validates the reduced target `Count` and targeted reassignments, and applies mutation quota. `ValidateOnly`
performs validation without changing metadata. `Count` must leave at least one active partition.

For a new drain, the controller writes one `PartitionDrainingRecord` per targeted tail partition with an absolute
`DeadlineMs = now + partition.drain.timeout.ms` and the topic request's `Force` flag (default `false`). Success
means the drain was accepted; it does not mean physical removal completed.

A normal request cannot replace an existing drain. A force request for the same target may upgrade the existing
drain by writing `Force=true` while preserving every original deadline. It neither restarts nor shortens the
minimum drain interval. A force request may include additional validated tail partitions; these receive a new
minimum interval while existing drains retain their deadlines. After partial
tail removal, force upgrades visit only the actual retained draining IDs for that target. They neither regenerate
draining records for removed IDs nor infer retained IDs from the size of the partition map.

#### Removal Eligibility and Transaction Safety

The active controller checks draining partitions on its periodic event loop. The deadline is the earliest time
removal may occur. A normal drain remains DRAINING beyond that time until its current leader confirms it is
safe to remove. Missing leaders, open transactions, unreplicated transaction completion, and missing or stale
reports block removal; elapsed time alone never overrides them.

Within each topic the controller considers actual retained partition IDs in descending order, starting at the
highest ID. It removes only an eligible tail: the first retained partition that is not draining, has not reached
its deadline, or lacks required safety confirmation stops removal of every lower ID in that topic. A safe lower
partition cannot be removed around a blocked higher partition, so retained partition IDs remain a contiguous
prefix. A blocked topic does not prevent eligible tail removals in other topics. Force bypasses transaction
safety for the selected drain; it does not bypass the minimum interval or this tail-order requirement.

The leader reports transaction safety in the tagged `BrokerHeartbeatRequest.DrainingPartitions` array. Each
`PartitionDrainStatus` includes `TopicId`, `PartitionId`, `LeaderEpoch`, `PartitionEpoch`, `MetadataOffset`,
`Ready`, and `BlockedReason`. The broker evaluates status after applying the draining metadata and installing
the write fence. Ready requires no ongoing transaction and no unreplicated transaction completion.

The controller accepts a report only from the current, unfenced leader at its current broker, leader and
partition epochs, with applied metadata covering the drain record. A later blocked report or omitted status
invalidates readiness. Reports are soft state: leadership or partition-epoch changes, broker fencing, a new
drain or cancellation, and controller activation/deactivation invalidate cached confirmations. The controller
logs blockers, including the broker's `BlockedReason`, and retries on later checks. Describe responses retain
`IsDraining` and `DrainDeadlineMs`; they do not add a blocker field or promise a completion time.

With explicit `Force=true`, the controller may remove the partition once the original deadline is reached even
without a ready leader report. Force can discard unresolved transactional data. It bypasses the removal safety
gate only: transaction markers still use `acks=all` and normal `min.insync.replicas` enforcement.

#### Metadata Replay and Snapshots

`PartitionDrainingRecord.Force` is a tagged boolean (tag 0, default `false`). Controller replay persists the
per-partition override alongside the deadline. `TopicImage.forcedDrainingPartitions()` is an immutable subset
of the currently draining IDs; snapshot records include each partition's force setting. Unrelated deltas retain
it. Cancellation (`DeadlineMs=-1`) and removal clear it; a subsequent normal drain starts with `Force=false`.

Broker readiness confirmations are neither logged nor snapshotted. After controller failover, the new active
controller restores deadlines and force overrides, clears all cached readiness, and requests fresh leader
confirmations. An expired normal drain waits for them. An expired forced drain remains eligible for removal.

#### Topic Deletion During Draining

`DeleteTopics` supersedes the drain. Replaying `RemoveTopicRecord` discards its deadlines, force overrides and
cached readiness; later removal checks skip the absent topic.

---

### Broker Behavior

#### Produce Request Handling (Draining Check)

```mermaid
flowchart TD
    A[Produce Request arrives for partition P] --> B{Partition P exists in allPartitions?}
    B -->|No| Z1[Return UNKNOWN_TOPIC_OR_PARTITION]
    B -->|Yes| C{P.isDraining()?}
    C -->|No| D[Normal produce path]
    C -->|Yes| E{Is control batch?\nCOMMIT/ABORT marker}
    E -->|Yes| F[Allow coordinator origin; retain acks=all and min ISR]
    E -->|No| Z2[Return NOT_LEADER_OR_FOLLOWER]
    F --> G[Append to local log]
    D --> H[Append to local log + ISR ack]
```

#### Produce Requests to Draining Partitions

Location: `server/src/main/java/org/apache/kafka/server/ReplicaManager.java` (or equivalent)

When a produce request targets a draining partition:

- Return `NOT_LEADER_OR_FOLLOWER` (6) as the partition-level error
- This is intentionally reusing an existing error code because:
    1. Producers already handle this error by refreshing metadata
    2. After metadata refresh, the producer's partition list won't include the draining partition (v0-v13) or will skip
       it (v14)
    3. No client-side code changes required for existing producers

#### Fetch Requests to Draining Partitions

- Serve normally. Consumers continue to read from draining partitions.
- The consumer needs this data to drain its lag before the partition is removed.

#### ListOffsets / OffsetFetch / OffsetCommit

- Serve normally for draining partitions.
- Consumers may still commit offsets for draining partitions.

#### Delayed Produce Operations

If a `DelayedProduce` (acks=all) is in-flight when a partition transitions to DRAINING:

- `DelayedProduce.tryComplete()` already handles the case where the replica is no longer leader (Case B in existing
  code)
- The partition's leadership is not changed during draining, but the validation check will detect draining state and
  complete the delayed operation with an error
- Add draining check to `PartitionStatusValidator`: if partition is draining, return error immediately

#### Delayed Fetch Operations

- `DelayedFetch` operations continue normally for draining partitions
- Consumers can still fetch; no change needed

#### On RemovePartitionRecord (Partition Fully Removed)

When broker receives `RemovePartitionRecord` via metadata update, the detailed cleanup flow is described in the
"Replica Lifecycle During Partition Deletion → On REMOVAL: Broker-Side Replica Cleanup" section above. Summary:

```
1. Stop fetchers (if follower) via ReplicaFetcherManager.removeFetcherForPartitions()
2. Complete in-flight delayed operations (DelayedProduce/DelayedFetch) with error
3. Partition.delete() — clear state, notify listeners (RemoteLogManager), remove metrics
4. Remove from allPartitions map
5. LogManager.asyncDelete() — rename dir to ".delete" suffix, queue for background cleanup
6. Update checkpoint files (recovery-point, log-start-offset, HW)
7. Remote storage cleanup (if enabled) — async segment deletion
```

---

### Producer Behavior

#### Metadata Update and Key Routing

**For producers using MetadataResponse v0–v13 (majority of existing deployments):**

The broker constructs the MetadataResponse by **excluding** draining partitions from the partition list. The producer
sees a reduced partition count and immediately rehashes: `hash(key) % newActiveCount`.

This means:

- Keys that previously mapped to partitions 0 through (newCount-1) MAY remap (since `% newCount` differs from
  `% oldCount`)
- Keys that previously mapped to the removed tail partitions WILL remap
- **This is a one-time disruption.** After the metadata refresh settles, routing is stable.

**For producers using MetadataResponse v14:**

Draining partitions appear with `IsDraining=true`. `ProducerMetadata` excludes them from its cached partition
metadata, so both keyed and sticky partitioning use only active partitions. Consumer metadata retains them for reads.

**Sticky partitioner impact:**

The `StickyPartitionCache` (for null-key messages) naturally adapts: when the current sticky partition becomes
unavailable (draining), the next `nextPartition()` call picks from the remaining active partitions.

#### Producer Convergence Timeline (Detailed)

When a partition enters DRAINING, the producer must discover this change. There are two discovery paths:

**Path 1: Error-driven discovery (fast, ~1 RTT)**

```
Timeline:
  T0: Controller writes PartitionDrainingRecord
  T1: Broker receives metadata update (near-instant via Raft log)
  T2: Producer sends Produce to draining partition
  T3: Broker returns NOT_LEADER_OR_FOLLOWER (partition-level error)
  T4: Producer calls metadata.requestUpdate(false) — forces immediate metadata refresh
  T5: Producer receives new metadata (draining partition excluded or IsDraining=true)
  T6: Newly partitioned records are routed to active partitions

  Convergence time: T6 - T2 ≈ 1-2 network RTTs (typically 2-10ms in same DC)
```

This path is **the dominant path** for active producers. As soon as one batch fails, metadata is refreshed.

**Path 2: Periodic refresh discovery (slow, up to `metadata.max.age.ms`)**

```
Timeline:
  T0: Controller writes PartitionDrainingRecord
  T1: Broker receives metadata update
  ...
  T(idle): Producer has no pending batches for draining partition
  T(refresh): metadata.max.age.ms expires (default 5 minutes = 300000ms)
  T(refresh): Producer fetches new metadata → discovers draining partition excluded

  Convergence time: up to metadata.max.age.ms (worst case 5 minutes)
```

This path applies to **idle producers** that happen to have the draining partition's metadata cached but aren't
actively writing to it. No impact on data flow — they just hold stale metadata until periodic refresh.

**Worst case for active producers:** Even with the error-driven path, there is a brief window where the broker
has received the draining metadata update but the producer hasn't learned yet. During this window:
- Batch is in-flight → broker returns NOT_LEADER_OR_FOLLOWER
- Producer retries the batch (if `retries > 0` and `delivery.timeout.ms` not exceeded)
- Retries keep the original partition; refreshing metadata does not repartition an existing batch

Records already assigned to a draining partition may fail after exhausting retries or `delivery.timeout.ms`.
There is no bounded successful-delivery delay for those records. Explicitly targeting a draining partition also fails.

#### Producer Retry Behavior with retries + delivery.timeout.ms

When a produce to a draining partition returns `NOT_LEADER_OR_FOLLOWER`:

```java
// Sender.java canRetry() logic:
boolean canRetry =
    !batch.hasReachedDeliveryTimeout(deliveryTimeoutMs, now)  // delivery.timeout.ms not exceeded
    && batch.attempts() < retries                              // retry count not exhausted
    && error.exception() instanceof RetriableException;        // NOT_LEADER_OR_FOLLOWER is retriable

// If canRetry == true:
//   1. metadata.requestUpdate(false) — schedule metadata refresh
//   2. reenqueueBatch(batch, now) — re-queue batch for retry after backoff
//   3. Next attempt keeps the original partition; newly partitioned sends use active partitions

// If canRetry == false (delivery.timeout.ms exceeded):
//   1. batch.done() with TimeoutException
//   2. Producer callback receives TimeoutException
//   3. Message is LOST (from producer's perspective)
```

**Key insight:** `NOT_LEADER_OR_FOLLOWER` is classified as `InvalidMetadataException` (which extends
`RetriableException`). This means the producer's existing retry logic handles draining partitions
correctly with **zero code changes** to the producer.

**Scenario: delivery.timeout.ms too short**

```
Producer config: delivery.timeout.ms=5000 (5s), retries=3, retry.backoff.ms=100

T0: Batch appended to RecordAccumulator for partition 8
T1 (4.9s later): Batch finally sent (was stuck behind other batches or linger.ms)
T2: Broker returns NOT_LEADER_OR_FOLLOWER
T3: delivery.timeout.ms exceeded → batch.done(TimeoutException)
     → Message LOST

Fix: This is a pre-existing producer misconfiguration, not specific to draining.
     delivery.timeout.ms should always be >> linger.ms + request.timeout.ms.
```

**Scenario: retries=0 (no retry)**

```
Producer config: retries=0

T0: Produce to draining partition → NOT_LEADER_OR_FOLLOWER
T1: canRetry = false (retries exhausted)
T2: Callback receives NotLeaderOrFollowerException
     → Message LOST

Note: retries=0 is a deliberate "fire and forget" config. The producer explicitly
accepts message loss. Not specific to draining.
```

---

### Consumer Behavior

#### During DRAINING State

- Consumer group assignment **still includes** draining partitions
- Consumers continue to fetch and commit offsets normally
- The group coordinator does NOT trigger rebalance just because partitions entered draining
- This allows consumers to drain remaining data in the partition
- Consumer continues to see data in the partition (all data written before draining started)
- High watermark continues to be served correctly

#### Consumer Lag Draining to Zero

```
Timeline for a consumer fetching from draining partition 8:
  T0: Partition 8 enters DRAINING. Current end offset = 1000. Consumer position = 800.
      Lag = 200 messages.
  T1: Consumer fetches batches, position advances: 800 → 900 → 950 → 1000
  T2: Consumer position = end offset. Lag = 0.
  T3: Subsequent fetch requests return empty (no new data, since produce is blocked)
  T4: Consumer remains assigned to partition 8, polling returns empty batches
  ...
  T(deadline): partition.drain.timeout.ms expires
  T(ready, at or after deadline): fresh safety confirmation → RemovePartitionRecord → rebalance
```

**After lag reaches 0:**
- Consumer continues polling (returns empty `ConsumerRecords`)
- No harm — it's just idle polling
- On removal, rebalance removes partition 8 from assignment
- Consumer continues with remaining partitions

**Design decision: Why not complete the drain early when lag=0?**
- Not all consumers are in registered consumer groups (manual assign, Streams)
- New consumers could start consuming during the drain period
- "All known consumer groups at lag=0" is not the same as "all consumers have consumed everything"
- A fixed minimum interval with explicit force is predictable and eliminates this class of ambiguity

#### Consumer Disconnect and Reconnect During Draining

```
Scenario: Consumer C1 is assigned partition 8 (DRAINING). C1 crashes.

T0: C1 crashes. Session timeout starts (session.timeout.ms, default 45s).
T1 (+45s): Group coordinator triggers rebalance due to C1 timeout.
T2: Remaining consumers C2, C3 get new assignment.
    Partition 8 (still DRAINING) is assigned to C2.
T3: C2 starts fetching from partition 8 at C1's last committed offset.
    C2 drains remaining data.

If C1 recovers before session timeout:
  → C1 re-joins group, may get partition 8 back (or another member gets it)
  → Either way, draining partition continues to be consumed
```

**No special handling needed.** Standard consumer group rebalance handles this. The key property is that
draining partitions remain in the assignment pool throughout the drain period.

#### Manual Assignment Consumers (assign())

Consumers using `consumer.assign(List<TopicPartition>)` do NOT participate in consumer group rebalance.
They will NOT be automatically notified when a partition is removed.

**During DRAINING:**
- Manually assigned consumer continues to fetch from draining partition normally
- No issue — same as consumer group member

**On REMOVAL:**
```
T0: Partition 8 removed (RemovePartitionRecord replayed)
T1: Consumer calls poll() → sends fetch request to broker for partition 8
T2: Broker returns UNKNOWN_TOPIC_OR_PARTITION for partition 8
T3: Consumer logs warning, triggers metadata refresh
T4: Consumer's metadata no longer includes partition 8
T5: Consumer continues calling poll() but partition 8 returns errors
T6: Consumer does NOT automatically remove partition 8 from assignment
    → User code must detect this and call assign() with updated partition list
```

**Consumer-side detection for manual assign users:**

```java
// Application code pattern for manual-assign consumers:
ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));

// Check for partitions that are no longer valid
Set<TopicPartition> currentAssignment = consumer.assignment();
Set<TopicPartition> validPartitions = new HashSet<>();
for (TopicPartition tp : currentAssignment) {
    // If metadata shows topic exists but partition doesn't, it's been removed
    List<PartitionInfo> partitions = consumer.partitionsFor(tp.topic());
    if (partitions != null && partitions.stream().anyMatch(p -> p.partition() == tp.partition())) {
        validPartitions.add(tp);
    }
}
if (validPartitions.size() < currentAssignment.size()) {
    consumer.assign(validPartitions);  // Shrink assignment
}
```

**Documentation responsibility:** The AdminClient `deletePartitions()` Javadoc must warn operators that
manually assigned consumers require application-level handling.

#### Consumer In-Flight Fetch at Removal Instant

```
Timeline:
  T0: Consumer sends FetchRequest for partition 8 (DRAINING, data still available)
  T1: Broker receives RemovePartitionRecord (between receiving fetch and responding)
  T2: Broker processes the fetch:

  Case A: Metadata update applied BEFORE fetch processed
    → Partition no longer in allPartitions map
    → Broker returns UNKNOWN_TOPIC_OR_PARTITION for partition 8
    → Consumer triggers metadata refresh
    → On next poll, consumer discovers partition gone → rebalance (group) or error (manual assign)

  Case B: Metadata update applied AFTER fetch processed
    → Fetch returns data normally (last successful fetch)
    → On NEXT fetch, Case A applies
```

**Error handling in consumer (FetchCollector.java):**

```java
// When UNKNOWN_TOPIC_OR_PARTITION is received:
case UNKNOWN_TOPIC_OR_PARTITION:
    log.warn("Received unknown topic or partition error in fetch for partition {}", tp);
    requestMetadataUpdate(metadata, subscriptions, tp);
    break;
```

The consumer handles this gracefully:
1. Logs a warning
2. Requests metadata refresh
3. Does NOT crash or throw to user code
4. On next poll, metadata is refreshed and partition is gone
5. For group consumers: coordinator triggers rebalance
6. For manual-assign consumers: subsequent fetches for that partition keep failing until user removes it

#### Orphaned Offsets in __consumer_offsets

When a partition is removed, committed offsets for that partition remain in `__consumer_offsets` as orphaned entries.

**Cleanup mechanism:**

- The group coordinator's `OffsetMetadataManager.onTopicsDeleted()` generates tombstones when an entire topic is deleted
- For individual partition deletion, we extend this: `onPartitionsDeleted(topicId, removedPartitionIds)` generates
  tombstone records for offsets committed to those specific partitions across all consumer groups
- Tombstones are written to `__consumer_offsets` and will be compacted away

**Implementation:**

File: `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java`

```java
/**
 * Called when individual partitions are removed from a topic (not full topic deletion).
 * Generates tombstone records for committed offsets to the removed partitions.
 */
public CoordinatorResult<Void, CoordinatorRecord> onPartitionsDeleted(
        Uuid topicId,
        Set<Integer> removedPartitionIds
) {
    List<CoordinatorRecord> records = new ArrayList<>();
    // Iterate all groups, find offsets committed to removed partitions, write tombstones
    offsetsByGroup.forEach((groupId, offsets) -> {
        offsets.forEach((tp, offsetAndMetadata) -> {
            if (tp.topicId().equals(topicId) && removedPartitionIds.contains(tp.partition())) {
                records.add(CoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, tp));
            }
        });
    });
    return new CoordinatorResult<>(records);
}
```

---

### Deletion Failure Handling

Partition deletion involves multiple subsystems, each of which can fail independently. This section defines the
failure semantics and recovery strategies for each layer.

#### Layer 1: Controller Writes RemovePartitionRecord — Failure

**Can it fail?** Only if the controller loses Raft leadership mid-write.

**Behavior:**
- If the controller is fenced before the record is committed, the write is lost
- The draining state persists (PartitionDrainingRecord still in log)
- New controller activates → sees draining partitions → re-schedules removal
- On a later check, the new controller removes only after fresh safety confirmation or an explicit force override
- Failover restores the drain and force policy. Normal removal remains pending until its safety conditions
  are satisfied; explicit force retains its documented unresolved-transaction risk.

#### Layer 2: Broker Log Deletion (LogManager.asyncDelete) — Failure

**What can fail:**
- Disk full (cannot create `.delete` suffixed renamed directory)
- Permission error (filesystem permissions changed)
- I/O error (bad disk sectors)

**Existing behavior in LogManager:**

```java
// LogManager.deleteLogs() background thread:
try {
    removedLog.delete();
    LOG.info("Deleted log for partition {} in {}.", ...);
} catch (KafkaStorageException kse) {
    LOG.error("Exception while deleting {} in dir {}.", removedLog, removedLog.parentDir(), kse);
    // NO immediate retry — logs error and moves on
}
// Background thread reschedules itself after fileDeleteDelayMs
```

**Failure handling design:**

```
Step 1: asyncDelete() renames directory:
  "topic-8" → "topic-8.{uuid}-delete"

  If rename fails:
    → KafkaStorageException thrown
    → errorHandler callback invoked (ReplicaManager logs it)
    → Partition still exists as a local directory but is NOT served
      (already removed from allPartitions map and metadata image)
    → No client impact (partition is already removed from metadata)
    → Next LogManager startup scan detects orphan dir → retries deletion

Step 2: Background deletion thread processes ".delete" directories:

  If file deletion fails:
    → Logged at ERROR level
    → Directory remains on disk with ".delete" suffix
    → Background thread will retry on next scheduled run (fileDeleteDelayMs, default 60s)
    → On broker restart: LogManager.loadLogs() scans for ".delete" dirs, re-queues them
```

**Key guarantees:**
1. **Partition is gone from metadata** — regardless of whether local files are cleaned up
2. **Local cleanup is eventually consistent** — retries happen periodically
3. **No client impact from failed cleanup** — partition is already removed from all serving paths
4. **Crash-safe** — rename is atomic; `.delete` suffix survives broker restart
5. **Disk full edge case** — if rename itself fails (rare), directory becomes an orphan;
   detected as "stale partition" on next broker restart and re-queued for deletion

#### Layer 3: Remote Storage Deletion (RemoteLogManager.stopPartitions) — Failure

**What can fail:**
- Network error to cloud storage (S3/GCS/Azure)
- Cloud storage rate limiting or throttling
- Partial deletion (some segments deleted, others failed)

**Existing behavior in RemoteLogManager:**

```java
// RemoteLogManager.stopPartitions():
try {
    if (stopPartition.deleteRemoteLog) {
        deleteRemoteLogPartition(tpId);  // May throw RemoteStorageException
    }
} catch (Exception ex) {
    errorHandler.accept(tp, ex);  // Error passed to caller
    LOGGER.error("Error while stopping the partition: {}", stopPartition, ex);
}
```

**Failure handling design:**

```
If deleteRemoteLogPartition() fails:
  1. Exception logged at ERROR level
  2. errorHandler callback invoked
  3. Remote log segments remain in cloud storage (orphaned)
  4. Local partition cleanup continues (NOT blocked by remote failure)
  5. No client impact — partition already removed from metadata

Recovery:
  - Remote segment metadata still exists in RemoteLogMetadataManager
  - A background cleanup job (existing in RemoteLogManager) periodically scans
    for orphaned segments (segments whose partition no longer exists in metadata)
  - Orphaned segments are retried for deletion
  - If background cleanup is not yet implemented: segments remain as storage waste
    until manual operator intervention (cloud storage lifecycle policy)
```

**Cost of failed remote cleanup:**
- Storage cost of orphaned segments in cloud storage
- No correctness impact — partition is gone from metadata, consumers cannot access it
- This is acceptable for MVP; a background orphan-segment cleaner is a future enhancement

#### Layer 4: Offset Tombstone Generation (OffsetMetadataManager) — Failure

A loading or unavailable offsets shard may miss the immediate partition deletion event.
The coordinator reconciles committed and pending transactional offsets against durable deletion
metadata on shard load and metadata updates. Scheduled cleanup regenerates tombstones after
failed appends. Entries remain in memory until tombstones replay successfully.

Only allocated-and-removed partition IDs are reconciled; arbitrary never-created IDs and
absent-topic offsets retain existing behavior. A topic ID mismatch never authorizes cleanup of
a newer topic incarnation. Retention is not a reliable fallback because an actively subscribed
group can retain offsets indefinitely. Physically removed IDs cannot be reused within the same
topic UUID, so missed cleanup cannot seed offsets for a replacement partition.

#### Summary: Failure Impact Matrix

| Layer | Failure | Client Impact | Data Impact | Recovery |
|-------|---------|:---:|:---:|----------|
| Controller RemovePartitionRecord write | None | None — retried on failover | Automatic — new controller re-schedules |
| Broker log rename (asyncDelete step 1) | None — partition already gone from metadata | Disk space not freed | Retry on broker restart (stale dir detection) |
| Broker log file deletion (asyncDelete step 2) | None | Disk space not freed | Background thread retries every 60s; survives restarts |
| Remote storage deletion | None | Cloud storage cost (orphaned segments) | Background cleanup or manual; segments inaccessible to clients |
| Offset tombstone generation | None | Stale offsets in __consumer_offsets | Reconcile durable removed IDs and retry tombstones |

**Design principle:** Partition removal from metadata is the **authoritative action**. All local/remote cleanup
is best-effort and eventually consistent. No cleanup failure can cause the partition to "come back" or become
accessible to clients again.

---

### Impact on CreatePartitions API

`CreatePartitions` may increase the active partition count while a drain is in progress. The controller first
validates the complete request, then reopens retained draining IDs below the requested target by writing
`PartitionDrainingRecord` with `DeadlineMs=-1`. Reopening keeps the same topic UUID, partition ID, local log,
producer state, data and offsets. Brokers clear the write fence; cancellation clears both the removal deadline
and force override. Retained IDs above the target keep draining with their original deadlines and force settings.

For example, with five retained partitions and IDs 3 and 4 draining, `CreatePartitions(totalCount=4)` reopens ID 3
and leaves ID 4 draining. A target above the retained count reopens the retained draining IDs first and creates
new IDs only if the requested contiguous prefix contains no physically removed ID. Validation-only or rejected
requests write neither cancellation nor creation records.

Physically removed IDs cannot be reused within the same topic UUID. `TopicRecord.PartitionIdHighWatermark`
records the highest allocated ID plus one and survives replay and snapshots even after tail removal. If an
increase would cross a removed ID, the controller returns `INVALID_PARTITIONS`; a fresh local log cannot fence
delayed requests to that old topic UUID and partition ID. Reopening before removal preserves the existing log,
while recreating an already removed ID requires a new topic UUID.

---

### Impact on AlterPartitionReassignments API

Two interactions:

1. **DeletePartitions targeting partitions being reassigned** → Rejected with `PARTITION_OPERATION_IN_PROGRESS`.
   Operator must cancel the reassignment first (via `AlterPartitionReassignments` with null assignment) or wait for
   it to complete.

2. **AlterPartitionReassignments targeting draining partitions** → Rejected with `PARTITION_OPERATION_IN_PROGRESS`.
   There is no point in reassigning replicas for a partition that will be removed.

---

### Impact on ElectLeaders API

- `ElectLeaders` for a draining partition → **Allowed.** There is no harm in electing a preferred leader for a
  draining partition. Consumers still need to fetch from it, so having the preferred leader is beneficial.
- The election logic itself does not need modification; it operates on the partition's replica set which remains
  intact during draining.

---

### Interaction with Transactional Producers

A normal drain fences all producer data writes, including transactional data, before its leader reports
readiness. Existing COMMIT and ABORT markers use the coordinator append path and bypass that data-write fence,
with unchanged `acks=all`, follower acknowledgement and `min.insync.replicas` checks. The leader reports blocked
while an ongoing transaction or an unreplicated transaction completion exists.

The controller removes a normal draining partition only after both its deadline and a fresh matching-epoch
leader confirmation. A missing leader, stale report, unavailable log or unavailable ISR may keep it draining
indefinitely. The heartbeat `BlockedReason` and controller logs explain why; Describe retains only its existing
draining flag and deadline. Readiness is soft state and must be re-established after failover or leadership changes.

`DeletePartitionsOptions.force(true)` and CLI `--force` explicitly bypass this transaction-safety gate after the
same minimum interval. The per-topic request flag becomes per-partition metadata and survives snapshots.
Upgrading an existing drain for the same target preserves its deadline. Force can discard unresolved transactional
data, including one participant of a transaction spanning retained partitions.

After a metadata refresh, producer routing avoids draining partitions. Stale or explicit `AddPartitionsToTxn`
requests can still reference them; the broker's data-write fence remains authoritative. The transaction
coordinator uses the separate `__transaction_state` internal topic, which partition deletion cannot target.

#### WriteTxnMarkers Retry After Partition Removal

When the `TransactionCoordinator` sends `WriteTxnMarkers` (COMMIT or ABORT) to a partition that has been removed:

```mermaid
sequenceDiagram
    participant TC as Transaction Coordinator
    participant Broker as Broker (former leader)
    participant MetaCache as Metadata Cache

    Note over Broker: RemovePartitionRecord replayed, partition gone
    TC->>Broker: WriteTxnMarkers(partition=8, ABORT)
    Broker-->>TC: UNKNOWN_TOPIC_OR_PARTITION
    TC->>TC: Classify as retriable → re-enqueue
    TC->>MetaCache: getPartitionLeaderEndpoint(topic, 8)
    alt Metadata cache shows partition 8 removed
        MetaCache-->>TC: None
        TC->>TC: Remove partition from pending set
        TC->>TC: Finalize transaction (skip this partition)
    else Stale metadata (propagation delay)
        MetaCache-->>TC: Stale leader endpoint
        TC->>Broker: WriteTxnMarkers(partition=8, ABORT) [retry]
        Note over TC: Retries until metadata propagates
    end
```

**Current behavior in `TransactionMarkerRequestCompletionHandler`:**
- `UNKNOWN_TOPIC_OR_PARTITION` is classified as **retriable** (no retry limit, no backoff)
- The escape hatch: when `metadataCache.getPartitionLeaderEndpoint()` returns `None`, the partition is
  removed from the transaction's pending set and the transaction proceeds to finalize

**Correctness for DeletePartitions:**
- The escape hatch works at partition granularity: when a specific partition is removed from the metadata image,
  `getPartitionLeaderEndpoint(topic, partitionId)` returns `None` even though the topic still exists
- Verification: `MetadataCache.getPartitionLeaderEndpoint()` queries `metadataSnapshot.partitions(topicName)`
  which is keyed by partition ID. A removed partition simply won't have an entry. ✓
- **Convergence window:** Between `RemovePartitionRecord` replay on the coordinator's broker and the metadata
  cache update, the coordinator retries. This window is typically < 1s (metadata propagates via Raft log).
- The metadata retry path converges. This is a liveness property, not a transaction-safety guarantee:
  forced deletion can discard unresolved transactional data. Normal deletion requires the separate safety gate.

**Design decision: No new error code needed.**

`UNKNOWN_TOPIC_OR_PARTITION` is semantically correct (the partition is gone). The existing retry + metadata
check loop terminates correctly for individual partition removal. Introducing a non-retriable
`PARTITION_DELETED` error was considered but rejected:
- It would require modifying `WriteTxnMarkers` response handling across all existing coordinator versions
- The existing mechanism already terminates within the metadata propagation window (~100ms–1s)
- The benefit (faster convergence by ~100ms) does not justify the protocol change

#### 2PC Late Marker After Removal (Race Condition)

**Scenario:** Controller writes `RemovePartitionRecord`. A late `WriteTxnMarkers(COMMIT)` arrives at the broker
for the now-removed partition.

**Behavior:**
1. Broker calls `replicaManager.onlinePartition(partition)` → returns `None` (partition not in `allPartitions`)
2. Returns `UNKNOWN_TOPIC_OR_PARTITION` in the response
3. Transaction coordinator retries → metadata cache shows partition gone → drops from pending set
4. Transaction finalizes without the marker on the removed partition

**Removal policy and late markers:**
- Normal deletion already required a fresh leader safety confirmation. Explicit force accepts the risk of
  discarding unresolved transactional data; a late marker does not undo that choice.
- The transaction's state in `__transaction_state` will record COMMIT/ABORT regardless
- No divergence between coordinator state and partition log because the partition log ceases to exist

**Comparison to Cluster Mirroring KIP's 2PC concern:**
Unlike Cluster Mirroring where a COMMIT marker might arrive AFTER an ABORT was already written (creating
conflicting markers), DeletePartitions has a simpler model: once the partition is removed, NO marker can be
written. The transaction coordinator simply skips the partition. There is no risk of dual COMMIT+ABORT markers.

---

### Interaction with Idempotent Producers

**ProducerState cleanup:**

- `ProducerStateManager` is per-partition. When the partition is removed and its log segments are deleted, the
  producer state (`.snapshot` files) are deleted as part of `LogManager.asyncDelete()`.
- No cross-partition impact.
- If the same producer later writes to a different partition (after rerouting), it gets a fresh sequence number
  window on that partition. The broker tracks producer state independently per partition.

**Two-phase cleanup mechanism (existing `LogManager.asyncDelete()`):**

1. Phase 1 (immediate): Directory renamed from `topic-8` to `topic-8.<uuid>-delete`.
   `ProducerStateManager.updateParentDir()` updates internal path references. Snapshots NOT deleted yet.
2. Phase 2 (deferred, after `file.delete.delay.ms`): `UnifiedLog.delete()` calls
   `deleteProducerSnapshots(deletedSegments, false)` which renames `.snapshot` → `.snapshot.deleted`,
   then physically deletes. `LocalLog.deleteEmptyDir()` does recursive `Utils.delete(dir)` as safety net.

**Partition ID reuse fencing:**

A physically removed partition ID cannot be allocated again within the same topic UUID.
CreatePartitions returns INVALID_PARTITIONS if the requested prefix contains a deleted ID.
The highest allocated ID plus one is retained in TopicImage and controller timeline state;
TopicRecord snapshots persist it through the tagged PartitionIdHighWatermark field.

A fresh log or producer snapshot would not fence delayed old Produce requests and transaction
markers, which still address the same topic UUID and partition index. A new topic UUID is
required for a new incarnation. Reopening before physical removal is safe: it cancels the
pending deadline and preserves the same log, producer state, committed offsets and partition
registration.

---

### Interaction with Share Groups (KIP-932)

Share groups maintain per-partition delivery state (`PersisterStateBatch` with offset ranges and delivery counts)
tracked by the `ShareCoordinatorShard`.

**When a partition enters DRAINING:**

- Share group members can still acknowledge messages (delivery completions)
- New message delivery stops (no new produce)
- Share coordinator continues tracking state for in-flight deliveries

**When a partition is REMOVED:**

**Gap identified:** The existing `ShareCoordinatorService.handleTopicsDeletion()` only cleans up state when
an *entire topic* is deleted (via `TopicsDelta.deletedTopicIds()`). It does NOT handle individual partition
removal within a still-existing topic.

**Required implementation:**

```java
// In ShareCoordinatorService — new method:
private void handlePartitionsDeleted(Uuid topicId, Set<Integer> removedPartitionIds) {
    runtime.scheduleWriteAllOperation(
        "on-partitions-deleted",
        coordinator -> coordinator.maybeCleanupShareStateForPartitions(topicId, removedPartitionIds)
    );
}

// In ShareCoordinatorShard — new method:
public CoordinatorResult<Void, CoordinatorRecord> maybeCleanupShareStateForPartitions(
        Uuid topicId, Set<Integer> removedPartitionIds) {
    Set<SharePartitionKey> eligibleKeys = new HashSet<>();
    shareStateMap.forEach((key, __) -> {
        if (key.topicId().equals(topicId) && removedPartitionIds.contains(key.partition())) {
            eligibleKeys.add(key);
        }
    });
    return new CoordinatorResult<>(eligibleKeys.stream()
        .map(key -> ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(
            key.groupId(), key.topicId(), key.partition()))
        .toList());
}
```

**Trigger:** In `ShareCoordinatorService.onNewMetadataImage()`, after checking `deletedTopicIds`, also
check `TopicsDelta` for topics with removed partitions (signaled by `RemovePartitionRecord` replay
removing entries from `TopicImage.partitions()`).

**In-flight delivery behavior:**
- Share group members with acquired (in-flight) records from the removed partition will receive
  `UNKNOWN_TOPIC_OR_PARTITION` on their next `ShareAcknowledge` call
- The share coordinator writes tombstones for the partition's state entries in `__share_group_state`
- Unacknowledged deliveries are effectively abandoned (no retry possible — partition is gone)
- This is acceptable: the data is being deleted anyway

**Same pattern as `OffsetMetadataManager.onPartitionsDeleted()` for consumer offsets.**

---

### Interaction with Kafka Streams

During draining, source partitions remain readable and tasks keep processing. After removal,
both classic and Streams protocols derive fewer source tasks and revoke ownership for removed
tasks. Previously assigned active, standby and warmup ownership is accepted while reconciliation
revokes it; arbitrary invalid tasks are still rejected.

Existing changelog topics may have more partitions than the current source-derived task count.
Both client and coordinator validation accept that surplus only for changelog topics. Every
active task must still have a changelog partition. The spare changelog data is not assigned a
source task and is not automatically deleted.

Repartition topic counts remain exact, including a topic also used as a changelog. Co-partitioned
join inputs must match; operators must coordinate changes to all affected inputs or stop and
reset/migrate the application. The broker does not recognize application-prefixed Streams
internal names through Topic.isInternal(), so operators must protect application-managed
changelog and repartition topics from direct partition removal.

---

### Interaction with Kafka Connect / MirrorMaker 2

#### Sink Connectors

- Sink connectors assigned to draining partitions continue consuming until removal
- On removal: connector rebalance triggered, tasks reassigned to remaining partitions
- No special handling needed

#### Source Connectors

- Source connectors write to topics. If the target topic has draining partitions, the connector's produce calls
  will be routed away from draining partitions (normal producer behavior)
- No special handling needed

#### MirrorMaker 2

- MM2's `MirrorSourceConnector.refreshTopicPartitions()` already detects "deleted partitions" by comparing current
  source partitions against known partitions
- MM2 does NOT propagate partition deletion to the target cluster — it only increases partition count, never decreases
- If source topic shrinks, MM2 will detect deleted source partitions and stop replicating them, but target topic
  keeps its original partition count
- **Operators must separately delete partitions on the target topic** if they want symmetric shrinking

---

### Interaction with Remote/Tiered Storage

If the topic has `remote.log.storage.enable=true`:

**During DRAINING:**

- Remote log manager continues operating normally
- Tier uploads continue for any new data (but no new data arrives since produce is rejected)
- Remote fetches continue to be served

**On REMOVAL:**

- `RemoteLogManager.stopPartitions()` called with `deleteRemoteLog=true`
- This schedules async deletion of all remote log segments for the partition
- `RemoteLogMetadataManager.onStopPartitions()` cleans up remote metadata
- This reuses the exact same code path as topic deletion

**Important:** Remote segment deletion is async and may take time. The partition is considered removed from the
metadata perspective immediately; remote cleanup happens in the background.

#### Diskless Topics (KIP-1500)

Diskless topics store data exclusively in tiered storage with no local log segments on brokers. At the time
of writing, the Diskless Topics design is still under discussion.

**Impact on DeletePartitions:**

- `LogManager.asyncDelete()` operates on local log directories. For diskless partitions, there is no local
  directory to rename or delete.
- Remote segment deletion via `RemoteLogManager.stopPartitions()` would still apply.
- The metadata removal (`RemovePartitionRecord`) works identically regardless of storage location.

**Design decision:** Diskless topics are **not explicitly supported** in the initial implementation. If a
partition has no local log (fully diskless), `asyncDelete()` will be a no-op (no directory to rename).
Remote cleanup proceeds normally. A future KIP for Diskless Topics will address any additional cleanup
requirements.

---

### Interaction with Partition-Level Metrics

When a partition is removed:

1. **BrokerTopicMetrics**: `GaugeWrapper.removeKey(partitionNum)` removes the partition's contribution to
   topic-level aggregate metrics
2. **DelayedProduce metrics**: `removePartitionMetrics(topicPartition)` removes expiration rate meter
3. **Replica-level metrics**: cleaned up when `Partition` object is closed
4. **Remote storage metrics**: `RemoteCopyLagBytes`/`RemoteCopyLagSegments` gauges removed for this partition

No draining-specific metrics are introduced in the MVP. Observability of draining state is through
`DescribeTopicPartitions` response.

---

### Interaction with Internal Topics

The following topics are protected from `DeletePartitions`:

| Topic                 | Reason                                                                                          |
|-----------------------|-------------------------------------------------------------------------------------------------|
| `__consumer_offsets`  | Removing partitions would lose committed offsets for consumer groups hashed to those partitions |
| `__transaction_state` | Removing partitions would orphan active transactions                                            |
| `__share_group_state` | Removing partitions would lose share group delivery state                                       |
| `__cluster_metadata`  | KRaft metadata topic; catastrophic to modify                                                    |

**Detection:** `Topic.isInternal(topicName)` check in validation step. Returns `INVALID_REQUEST` (42) with message
"Cannot delete partitions from internal topic '{name}'."

---

### Interaction with ACLs

- `DeletePartitions` requires `ALTER` permission on the topic (same as `CreatePartitions`)
- ACLs are topic-level only (no partition-level ACL granularity exists in Kafka)
- No ACL changes needed
- The `ALTER` operation is appropriate because we are altering the topic's partition configuration

---

### Interaction with Quotas

#### Controller Mutation Quota

`DeletePartitions` consumes controller mutation quota proportional to `deleteCount`:

```java
context.applyPartitionChangeQuota(deleteCount);
```

If the mutation quota is exhausted, the controller returns `THROTTLING_QUOTA_EXCEEDED` (89) with a `ThrottleTimeMs`
value indicating when to retry.

#### Produce/Fetch Quotas

No impact. Produce quotas are per-client, not per-partition. When partitions are removed, the client's traffic
redistributes to remaining partitions under the same quota.

---

### CLI Support

File: `tools/src/main/java/org/apache/kafka/tools/TopicCommand.java`

Add `--delete-partitions <count>` option to `kafka-topics.sh --alter`:

```
kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic my-topic \
    --delete-partitions 3
```

This removes the last 3 partitions from `my-topic`, placing them in draining state.

**Validation in CLI:**

- `--delete-partitions` and `--partitions` (increase) are mutually exclusive
- `--delete-partitions` requires a positive integer

---

## Edge Cases and Invariants

### Partition Numbering After Delete + CreatePartitions

Partition IDs form a contiguous retained prefix after completed removal. Removed IDs are never
reused within the same topic UUID. The controller returns INVALID_PARTITIONS for an expansion
that would reuse one; a new topic is required for a new incarnation. The historical allocation
high watermark survives snapshots and controller failover.

### CreatePartitions During Draining

Expansion targets the active count. It first reopens existing draining IDs within the target
prefix by writing PartitionDrainingRecord with DeadlineMs=-1. IDs above the target remain
draining with their original deadlines. New PartitionRecords are written only above the
physically retained count, provided no deleted ID would be reused.

For retained IDs 0-4 with IDs 3-4 draining: target4 reopens3, target5 reopens3-4, target6 also
creates5. Reopened partitions retain their original data and registration. Manual assignments
apply only to newly created partitions, not reopened ones. Quotas count both resumed and newly
created partitions. Validation-only and rejected requests do not cancel deadlines. Brokers
clear the write fence on resume; snapshots store only the remaining positive deadlines.

### DeletePartitions During Existing Drain

A normal or conflicting drain request returns `PARTITION_OPERATION_IN_PROGRESS`. An explicit force request for
the same target upgrades existing draining partitions to `Force=true` and preserves their original deadlines.
It does not shorten the minimum interval or extend it by starting a second countdown.

### Topic Deletion During Draining

**Allowed.** `DeleteTopics` takes priority and is a superset of partition deletion.

When `RemoveTopicRecord` is replayed:

- All draining state for that topic is implicitly discarded
- The deferred removal event becomes a no-op (topic no longer exists when it fires)
- Implementation: deferred event checks if topic still exists before writing `RemovePartitionRecord`

### Controller Failover During Draining

The new controller restores deadlines and per-partition force flags from replay or snapshots and clears all
cached readiness reports. For an expired normal drain it waits for fresh applied-metadata confirmation from the
current leader with matching epochs. An expired forced drain is eligible without that confirmation. Future
deadlines keep their original earliest removal time.

### Broker Restart During Draining

- Broker replays metadata on startup
- Sees `PartitionDrainingRecord` → marks local partition as draining
- Resumes serving fetch requests for the partition
- Rejects produce requests
- If `RemovePartitionRecord` was written while broker was down → catches up and deletes local log

### Split-Brain / Stale Controller

Not possible in KRaft. Only the active controller (Raft leader) can write to the metadata log. A stale controller
cannot write `PartitionDrainingRecord` or `RemovePartitionRecord`.

### deleteCount Equals currentPartitionCount - 1 (Only 1 Partition Remains)

**Allowed.** A topic with 1 partition is valid. All keys hash to partition 0.

### partition.drain.timeout.ms = 0

Zero makes removal eligible without a consumer-drain wait. A normal drain still waits for its current leader to
apply the write fence and report transaction safety. Zero does not imply same-batch or immediate removal.
Explicit force bypasses safety after that zero minimum interval, while still following metadata commit and
broker cleanup asynchronously.

### Concurrent Requests for Different Topics

**Allowed.** Each topic's draining state is independent. Concurrent `DeletePartitions` for different topics
proceed in parallel.

### Network Partition Between Controller and Brokers

- If a broker cannot communicate with the controller, it won't receive the metadata update about draining
- The broker continues serving produce requests to the partition until it catches up
- Eventually (when network heals), broker gets the update and starts rejecting produces
- The controller rejects safety reports that do not cover the applied drain metadata, so normal removal waits
- Explicit force can bypass that safety requirement only after the original deadline

### What If Producer Ignores NOT_LEADER_OR_FOLLOWER?

A misbehaving producer that doesn't refresh metadata will keep getting `NOT_LEADER_OR_FOLLOWER` on every attempt.
It cannot write to a draining partition. The broker-side enforcement is authoritative regardless of client behavior.

### Replica on JBOD Disk Failure During Draining

```
Partition 8: replicas=[B0*, B1, B2], directories=[d0, d1, d2], state=DRAINING
B1's disk d1 fails (JBOD, not full broker failure).

  1. B1 reports log dir failure via BrokerHeartbeat
  2. Controller removes B1 from ISR for partition 8
  3. Partition 8 continues on [B0*, B2] — ISR=[B0, B2]
  4. Consumer unaffected (leader B0 alive)
  5. Draining continues normally
```

If `d0` (leader's disk) fails:

- Same as "leader crashes" scenario — controller elects from remaining ISR
- If B0 has other disks and partition is on the failed disk, it's functionally equivalent to B0 losing
  that partition replica

### Retention Policy During Draining

**Problem:** If `retention.ms` is short (e.g., 5 minutes) and `partition.drain.timeout.ms` is long (e.g., 1 hour),
retention will delete data from the draining partition BEFORE consumers can drain it.

```
Example:
  Topic config: retention.ms=300000 (5 min), partition.drain.timeout.ms=3600000 (1 hour)
  Partition 8 enters DRAINING at T0. Last message written at T0.
  T0 + 5min: retention.ms expires → LogManager deletes oldest segments
  T0 + 10min: consumer starts draining → data already gone!
```

**Design decision: Retention continues to run during draining.**

Rationale:
- Pausing retention would require partition-level retention override — complex to implement
- If retention deletes data before consumers drain, the consumers simply reach end-of-log faster
- This is an operator responsibility: set `partition.drain.timeout.ms` shorter than or comparable to
  `retention.ms`, OR increase retention before initiating deletion
- Consistent behavior: `retention.ms` is always enforced regardless of partition state

**Documentation warning:** The AdminClient Javadoc and CLI output must warn:
> "Ensure partition.drain.timeout.ms does not exceed retention.ms for time-based retention topics,
> otherwise data may be deleted by retention before consumers can drain it."

### Log Compaction and Data Loss Semantics

For topics with `cleanup.policy=compact`:

**Unique key loss:** If partition 8 holds the ONLY remaining copy of key "X" (after compaction removed
older versions on other partitions, or the key was only ever written to partition 8), deleting partition 8
permanently loses key "X" with no recovery path.

This is **fundamentally identical** to deleting a whole topic — both are destructive operations that lose
data. The difference is that partition deletion is more surgical, affecting only keys routed to the
deleted partitions.

**Operator responsibility:**
- Before deleting partitions from a compacted topic, operators must understand that any key whose
  ONLY remaining record is in a deleted partition will be permanently lost
- If the topic uses a key-based partitioner (`hash(key) % partitionCount`), operators can determine
  exactly which keys are affected: those where `hash(key) % oldCount` maps to a deleted partition
- For topics where all keys must be preserved, operators should re-produce affected keys to remaining
  partitions before initiating deletion

**No system-level protection:** The system does NOT scan partition data to warn about unique keys.
This is analogous to how `DeleteTopics` does not warn about data loss — it is always an operator decision.

### Log Cleaner and Draining Partitions

For log-compacted partitions in DRAINING state:

- Log cleaner **continues to run** — compaction keeps the log small
- Since no new data arrives, cleaner work eventually stops naturally (nothing new to compact)
- On removal: `LogManager.asyncDelete()` calls `cleaner.abortCleaning(topicPartition)` to stop any
  in-progress compaction before renaming the directory

### Fetch Session Cache and Draining Partitions

Consumers using incremental fetch (fetch sessions) have a server-side session cache tracking which partitions
they are fetching.

During DRAINING:

- Fetch session continues normally — consumer's fetched partitions include the draining partition
- Server continues serving data from the draining partition

On REMOVAL:

- Next incremental fetch from client will reference a partition that no longer exists
- Server returns `UNKNOWN_TOPIC_OR_PARTITION` for that partition in the fetch response
- Client removes partition from fetch session and triggers metadata refresh
- Consistent with existing behavior when a topic is deleted mid-fetch-session

**Important:** The broker does NOT proactively invalidate fetch sessions when partitions are removed. The
mechanism is entirely passive — errors propagate on the next client request. If the client dies without
cleanup, the stale session entry lingers until evicted by cache pressure (minimum 120s idle). This is
acceptable: no correctness impact, just memory.

### Consumer Group Static Membership (KIP-345) and Partition Removal

Static members (`group.instance.id` set) normally hold their partitions through the `session.timeout.ms`
window even after stopping heartbeats — this prevents unnecessary rebalances during rolling restarts.

**Partition removal triggers immediate rebalance regardless of static membership:**

| Trigger | Behavior for Static Members |
|---------|---------------------------|
| Member failed/left (no heartbeat) | Session timeout respected — partitions held |
| Metadata changed (partition removed) | **Immediate rebalance** — session timeout irrelevant |

**Classic protocol path:** Consumer leader detects partition count change via `MetadataSnapshot` comparison →
sends JoinGroup → forces rebalance on coordinator.

**New protocol (KIP-848) path:** `GroupMetadataManager.onMetadataUpdate()` calls
`group.requestMetadataRefresh()` → resets metadata deadline to 0 → next heartbeat from ANY member triggers
group epoch bump → new assignment computed immediately.

**Conclusion:** No special handling needed for static membership. Partition removal is a metadata change,
not a member failure, so it bypasses the session timeout grace period.

### TOCTOU Safety: Concurrent Operations on Controller

A potential concern: can a partition reassignment START between the time `DeletePartitions` checks
`isReassigning()` and the time it writes `PartitionDrainingRecord`?

**Answer: No. Not possible.**

The KRaft controller is explicitly single-threaded (all operations run on `KafkaEventQueue`'s single
event handler thread via `ControllerWriteEvent`). The validation check and record generation happen in
a single `generateRecordsAndResult()` invocation. No interleaving is possible.

This also means:
- `DeletePartitions` cannot race with `AlterPartitionReassignments` (serialized)
- `DeletePartitions` cannot race with `CreatePartitions` (serialized)
- `DeletePartitions` cannot race with `DeleteTopics` (serialized)
- Controller failover serializes via Raft leader election (only one active controller)

### Delayed ACK (acks=all) In-Flight When Draining Starts

```
Timeline:
  T0: Producer sends Produce(partition=8, acks=all)
  T1: Leader appends locally, waiting for ISR followers to replicate
  T2: PartitionDrainingRecord arrives at broker (metadata update)
  T3: Followers replicate → ISR ack achieved

  Result: The produce request SUCCEEDS (T3 > T2 but it was already in-flight)
```

**Design decision:** In-flight produce requests that were already appended to the leader's log before
the draining state arrives are NOT retroactively rejected.

Rationale:

- The data is already in the leader's log; rejecting the client response doesn't remove the data
- Simplifies implementation — no need to scan pending delayed operations when draining starts
- After this in-flight request completes, subsequent produce requests will be rejected

**BUT:** New produce requests that arrive AFTER the broker learns about draining ARE rejected, even
if the leader hasn't processed the metadata update atomically:

```java
// Check order in appendRecords:
// 1. Is partition draining? → reject
// 2. Append to log
// 3. Wait for ISR ack (DelayedProduce)
```

### AlterConfigs on partition.drain.timeout.ms During Draining

**Allowed, but no effect on in-progress drain.**

The config value is read only at the time `DeletePartitionsRequest` is processed. The deadline is computed
as an absolute timestamp and stored in `PartitionDrainingRecord`. Changing the config after draining starts
only affects future `DeletePartitions` requests.

If operators need to extend a drain: not supported in MVP (requires Cancel + Re-issue, which is future work).

### Multiple Broker Failures During Removal (Cleanup Reliability)

```
RemovePartitionRecord written. Brokers B0, B1, B2 hold replicas.
B1 and B2 are offline during removal.

  B0: receives record → cleans up immediately
  B1: offline → will clean up on recovery
  B2: offline → will clean up on recovery

What if B1 recovers but crashes again before cleanup completes?
  → On next recovery, same logic applies: detect stale dir, async delete
  → Cleanup is idempotent — partial rename (to .delete suffix) is re-detected
  → LogManager startup scans for ".delete" suffixed directories and re-queues them
```

The cleanup process is crash-safe because:

1. `asyncDelete()` renames first (atomic on most filesystems), then queues for background deletion
2. On startup, `LogManager.loadLogs()` scans for `.delete`-suffixed directories and re-queues them
3. Even if a broker crashes mid-deletion, the next startup will finish the job

---

## Metrics

### New JMX Metrics

| Name | Type | MBean | Description |
|------|------|-------|-------------|
| `DrainingPartitionCount` | Gauge | `kafka.controller:type=KafkaController,name=DrainingPartitionCount` | Number of partitions currently in DRAINING state across all topics. Only reported by the active controller. |
| `PendingDrainRemovals` | Gauge | `kafka.controller:type=KafkaController,name=PendingDrainRemovals` | Number of draining removals still pending deadline or transaction safety. |
| `DrainCompletedTotal` | Meter | `kafka.controller:type=KafkaController,name=DrainCompletedTotal` | Cumulative count of partitions that transitioned from DRAINING to REMOVED. |
| `DeletePartitionsRequestsPerSec` | Meter | `kafka.network:type=RequestMetrics,name=RequestsPerSec,request=DeletePartitions` | Rate of DeletePartitions requests received by the broker. |
| `DeletePartitionsTotalTimeMs` | Histogram | `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=DeletePartitions` | Total time to process DeletePartitions requests. |
| `DeletePartitionsErrorsPerSec` | Meter | `kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=DeletePartitions,error={errorName}` | Rate of errors returned by DeletePartitions requests, broken down by error code. |

### Existing Metrics Behavior During Draining

| Metric | During DRAINING | After REMOVAL |
|--------|----------------|---------------|
| `kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=X` | Drops to 0 for draining partitions (no writes) | Partition contribution removed |
| `kafka.server:type=BrokerTopicMetrics,name=BytesOutPerSec,topic=X` | Consumer reads continue; may remain non-zero | Partition contribution removed |
| `kafka.server:type=ReplicaManager,name=PartitionCount` | Unchanged (partition still exists) | Decremented |
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | May include draining partitions if ISR < replicas | Partition removed from count |
| `kafka.server:type=FetcherLagMetrics,name=ConsumerLag,topic=X,partition=N` | Continues reporting consumer lag | Metric deregistered |

---

## Performance Considerations

### Controller Impact

- **PartitionDrainingRecord writes**: One record per targeted partition, including its deadline and force flag; force upgrades also preserve that deadline.
- **RemovePartitionRecord writes**: One record per partition removed. For a topic with 100 partitions deleting 50, this is 50 records in a batch — similar to a 50-partition topic deletion. Batch amortizes Raft commit overhead.
- **Readiness state**: Cached leader confirmations are proportional to draining partitions and are cleared on controller failover. The existing controller periodic task checks removal eligibility.
- **Failover recovery**: On controller activation, iterating all topics with draining partitions is O(drainingTopics). In practice, very few topics drain simultaneously, so this adds negligible startup latency.

### Broker Impact

- **Produce rejection overhead**: A single boolean check (`partition.isDraining()`) on each produce request for draining partitions. The hot path (non-draining) adds one field read — effectively zero cost.
- **Metadata and readiness propagation**: Draining/removal records use the existing metadata path. Leader transaction-safety status is included in the existing broker heartbeat; normal removal waits for its application confirmation.
- **Log deletion**: `asyncDelete()` renames the directory (O(1) filesystem operation) then queues background deletion. Background thread deletes segment files sequentially. Deleting a 100 GB partition log takes proportional time but does not block serving.
- **Remote storage deletion**: Async, fire-and-forget with retries. Does not block broker serving or replication.
- **Checkpoint file rewrites**: One atomic file write per log directory to update offset checkpoint files. Existing code path, same as topic deletion.

### Client Impact

- **Producer**: One metadata refresh RTT (~2-10ms in same DC) per draining event. After refresh, routing stabilizes. No throughput impact on non-draining partitions.
- **Consumer**: Zero impact during draining (fetch continues normally). On removal, one rebalance (~seconds depending on group size). Standard rebalance cost.
- **AdminClient**: `deletePartitions()` is a single controller round-trip. Latency comparable to `createPartitions()`.

### Scalability Limits

| Dimension | Practical Limit | Bottleneck |
|-----------|----------------|------------|
| Concurrent draining topics | ~10,000 | Controller state size and periodic readiness checks |
| Partitions per single DeletePartitions call | ~10,000 | Metadata batch size (default 5MB); each PartitionDrainingRecord is ~50 bytes |
| Drain timeout range | 0 to ~292 years (int64 ms) | Wall-clock overflow not a concern in practice |
| Removal batch size | Limited by controller batch size | Same constraint as topic deletion |

---

## Command-Line Interface

### Usage Examples

**Delete the last 3 partitions from a topic (enters draining state):**

```bash
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders \
    --delete-partitions 3
Initiated partition deletion for topic 'orders': partitions [7, 8, 9] entering DRAINING state.
Drain deadline: 2026-07-15T15:00:00Z (partition.drain.timeout.ms=3600000)
```

**Check draining status via describe:**

```bash
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic orders
Topic: orders   TopicId: abc123   PartitionCount: 10   ReplicationFactor: 3
  Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
  ...
  Partition: 7   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1   Draining: true   Deadline: 2026-07-15T15:00:00Z
  Partition: 8   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2   Draining: true   Deadline: 2026-07-15T15:00:00Z
  Partition: 9   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3   Draining: true   Deadline: 2026-07-15T15:00:00Z
```

Set the minimum interval to zero to make removal eligible without a consumer-drain wait. Normal deletion
still requires fresh transaction-safety confirmation:

```bash
bin/kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name orders \
    --alter --add-config partition.drain.timeout.ms=0
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders --delete-partitions 3
```

To explicitly bypass transaction safety after the deadline, pass `--force`. This also upgrades an existing drain
for the same target while preserving its original deadline:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders --delete-partitions 3 --force
# Equivalent target-count form for a ten-partition topic:
bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders --partitions 7 --force
```

`--force` is valid only for an alteration that decreases partition counts. With a topic pattern, every matched
topic must pass that validation before any mutation is submitted. The command reports initiation, including for
zero-timeout or forced deletion; it does not promise that removal is complete. Describe continues to show the
draining flag and original deadline. Controller logs explain transaction-safety blockers.

**Error: draining already in progress:**

```bash
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders \
    --delete-partitions 2
Error while executing topic command: A partition operation is already in progress for topic 'orders'.
```

**Error: invalid count:**

```bash
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders \
    --delete-partitions 10
Error while executing topic command: The requested delete partition count is invalid.
Cannot delete all partitions; at least one partition must remain.
```

### CLI Options Reference

| Option | Description | Mutually Exclusive With |
|--------|-------------|------------------------|
| `--delete-partitions <count>` | Number of partitions to remove from the tail | `--partitions` |
| `--partitions <count>` | Target total count; a lower count initiates partition deletion | `--delete-partitions` |
| `--force` | Explicit transaction-safety override for a decrease; validates all matched topics before mutation | Increases or non-alter operations |

---

## Compatibility, Deprecation, and Migration Plan

### Compatibility Matrix

| Component | Version Requirement | Notes |
|-----------|-------------------|-------|
| Controller | IBP_4_4_IV2+ | Must support PartitionDrainingRecord, RemovePartitionRecord |
| Brokers | IBP_4_4_IV2+ | Must support draining state in metadata image |
| Admin Client (caller) | 4.4+ | Must support DeletePartitions API key 94 |
| Producers | Any version | No code changes. v0-v13 metadata: draining partitions excluded. v14: IsDraining field. |
| Consumers (group) | Any version | No code changes. Standard rebalance handles removal. |
| Consumers (manual assign) | Any version | Application must detect UNKNOWN_TOPIC_OR_PARTITION and update assignment. |
| Kafka Streams | Updated client/coordinator | Surplus changelog accepted; repartition and join constraints remain. |
| Kafka Connect | Any version | Connector rebalance handles removal automatically. |
| MirrorMaker 2 | Any version | Detects partition deletion on source. Does NOT propagate to target. |

### Wire Protocol Compatibility

| Client version                         | Behavior                                                                                                               |
|----------------------------------------|------------------------------------------------------------------------------------------------------------------------|
| Old client, old API version            | Cannot call DeletePartitions (unknown API key → `UnsupportedVersionException`)                                         |
| Old producer (MetadataResponse v0-v13) | Draining partitions excluded from metadata. Producer sees reduced count. No code changes needed.                       |
| Old consumer                           | Continues consuming from draining partitions. On removal, metadata refresh triggers rebalance. No code changes needed. |
| New client (v14 metadata)              | Sees `IsDraining=true` field. Can provide observability.                                                               |

### Cluster Upgrade Path

```mermaid
flowchart LR
    A[Rolling upgrade all nodes\nto version with IBP_4_4_IV2] --> B[kafka-features.sh upgrade\n--feature metadata.version=33]
    B --> C[DeletePartitions API\nbecomes available]
    C --> D[Operators can now call\nkafka-topics.sh --delete-partitions]
```

1. Rolling upgrade all brokers/controllers to version supporting IBP_4_4_IV2
2. Set `MetadataVersion` to IBP_4_4_IV2 via `kafka-features.sh`
3. DeletePartitions API becomes available

### Rollback

If issues are found after enabling:

- Partitions already REMOVED cannot be recovered (data is deleted)
- Reopen retained draining partitions with CreatePartitions before removal. Downgrading across IBP_4_4_IV2
  is rejected because metadata changed; older controllers must not discard draining state or historical ID allocation.
- **Recommendation:** Test thoroughly with non-critical topics first

### Release Phases

| Phase | MetadataVersion | Stability | Enablement |
|-------|----------------|-----------|------------|
| Early Access | IBP_4_4_IV2 (unstable) | `unstable.feature.versions.enable=true` required | Explicit opt-in. Not for production. |
| General Availability | Future IBP (stable) | Stable, no flags needed | Enabled by default at target MetadataVersion. |

---

## Files Modified (Complete List)

### New Files

| File                                                                                              | Description            |
|---------------------------------------------------------------------------------------------------|------------------------|
| `clients/src/main/resources/common/message/DeletePartitionsRequest.json`                          | API schema             |
| `clients/src/main/resources/common/message/DeletePartitionsResponse.json`                         | API schema             |
| `metadata/src/main/resources/common/metadata/PartitionDrainingRecord.json`                        | Metadata record schema |
| `metadata/src/main/resources/common/metadata/RemovePartitionRecord.json`                          | Metadata record schema |
| `clients/src/main/java/org/apache/kafka/common/errors/PartitionOperationInProgressException.java` | Exception              |
| `clients/src/main/java/org/apache/kafka/common/errors/InvalidDeletePartitionCountException.java`  | Exception              |
| `clients/src/main/java/org/apache/kafka/clients/admin/DeletePartitionsOptions.java`               | AdminClient options    |
| `clients/src/main/java/org/apache/kafka/clients/admin/DeletePartitionsResult.java`                | AdminClient result     |

### Modified Files

| File                                                                                            | Change                                                                     |
|-------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `clients/src/main/resources/common/message/MetadataRequest.json`                                | validVersions "0-13" → "0-14"                                              |
| `clients/src/main/resources/common/message/MetadataResponse.json`                               | validVersions "0-13" → "0-14"; add `IsDraining` field to partition         |
| `clients/src/main/resources/common/message/DescribeTopicPartitionsRequest.json`                 | validVersions "0" → "0-1"                                                  |
| `clients/src/main/resources/common/message/DescribeTopicPartitionsResponse.json`                | validVersions "0" → "0-1"; add `IsDraining`, `DrainDeadlineMs`             |
| `clients/src/main/java/org/apache/kafka/common/protocol/Errors.java`                            | Add codes 136, 137                                                         |
| `clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java`                         | Add `partition.drain.timeout.ms`                                           |
| `clients/src/main/java/org/apache/kafka/clients/admin/Admin.java`                               | Add `deletePartitions()` methods                                           |
| `clients/src/main/java/org/apache/kafka/clients/admin/KafkaAdminClient.java`                    | Implement `deletePartitions()`                                             |
| `server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java`               | Add `IBP_4_4_IV2`, add `isDeletePartitionsSupported()`                     |
| `metadata/src/main/java/org/apache/kafka/metadata/PartitionRegistration.java`                   | Add `draining` boolean field + builder method                              |
| `metadata/src/main/java/org/apache/kafka/image/TopicImage.java`                                 | Add `hasDrainingPartitions()`, `activePartitionCount()` helpers            |
| `metadata/src/main/java/org/apache/kafka/image/TopicDelta.java`                                 | Add replay for `PartitionDrainingRecord`, `RemovePartitionRecord`          |
| `metadata/src/main/java/org/apache/kafka/image/MetadataDelta.java`                              | Add replay dispatch for new records                                        |
| `metadata/src/main/java/org/apache/kafka/controller/Controller.java`                            | Add `deletePartitions()` interface method                                  |
| `metadata/src/main/java/org/apache/kafka/controller/QuorumController.java`                      | Wire `deletePartitions()`, add deadline tick check                         |
| `metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java`             | Implement deletion safety gate, heartbeat readiness and force upgrades; reopen retained IDs on increase |
| `core/src/main/scala/kafka/server/ControllerApis.scala`                                         | Add `handleDeletePartitions()` request handler                             |
| `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/OffsetMetadataManager.java` | Add `onPartitionsDeleted()` for offset tombstones                          |
| `tools/src/main/java/org/apache/kafka/tools/TopicCommand.java`                                  | Add `--delete-partitions` CLI option                                       |

---

## Testing Strategy

### Unit Tests

| Component                                       | Test Cases                                                                                                 |
|-------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| `ReplicationControlManager`                     | All validation paths (invalid count, internal topic, unauthorized, reassignment conflict, duplicate drain) |
| `ReplicationControlManager`                     | State machine: ACTIVE→DRAINING on PartitionDrainingRecord write                                            |
| `ReplicationControlManager`                     | Removal check requires deadline plus fresh leader readiness, or explicit force                                     |
| `ReplicationControlManager`                     | CreatePartitions reopens retained draining IDs; removed IDs cannot be reused                                                                   |
| `ReplicationControlManager`                     | AlterPartitionReassignments blocked for draining partitions                                                |
| `ReplicationControlManager`                     | Conflicting drain blocked; same-target force upgrade preserves deadline                                                |
| `TopicDelta`                                    | Replay `PartitionDrainingRecord` → marks partitions as draining                                            |
| `TopicDelta`                                    | Replay `RemovePartitionRecord` → removes partition from map                                                |
| `MetadataDelta`                                 | Dispatch to TopicsDelta for both new record types                                                          |
| `PartitionRegistration`                         | Builder sets draining=true; equals/hashCode includes draining                                              |
| `TopicImage`                                    | `hasDrainingPartitions()` returns true/false correctly                                                     |
| `TopicImage`                                    | `activePartitionCount()` excludes draining partitions                                                      |
| `MetadataResponse` serialization                | v14 includes IsDraining field; v0-v13 excludes draining partitions                                         |
| `DescribeTopicPartitionsResponse` serialization | v1 includes IsDraining and DrainDeadlineMs                                                                 |
| `Errors`                                        | New error codes serialize/deserialize correctly                                                            |
| `TopicConfig`                                   | New config validates (type=long, atLeast(0))                                                               |
| Broker heartbeat readiness                     | Safe/blocked status, applied drain metadata and current epochs; stale, omitted or fenced reports rejected |
| Controller failover                             | Clear cached readiness while preserving deadlines and force; normal removal waits for fresh report |
| Force policy                                    | No early removal; same-target upgrade visits only retained draining IDs and keeps deadlines; resume/remove clear force; snapshot round-trip |
| Contiguous tail removal                         | Highest blocked ID prevents lower removal; descending eligible suffix removes; other topics proceed |
| Admin and CLI force                             | Default false; serialize per-topic flag; reject increases/non-alter and mixed matched topics before mutation |
| `DeletePartitionsRequest/Response`              | Serde round-trip                                                                                           |

### Integration Tests

| Scenario                             | Verification                                                                                                                                   |
|--------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| Happy path: full lifecycle           | DeletePartitions → minimum interval → fresh leader safety confirmation → RemovePartitionRecord → log cleanup verified                                                       |
| Producer during draining             | Produce to draining partition → NOT_LEADER_OR_FOLLOWER → metadata refresh → produce to active partition succeeds                               |
| Consumer during draining             | Consumer fetches from draining partition → data received → partition removed → rebalance → consumer assigned remaining partitions              |
| Transactional producer               | Open txn writing to partition → partition enters draining → data produce rejected → abort marker allowed through → transaction aborted cleanly |
| CreatePartitions during drain | Reopens retained draining IDs, preserving data; creates new IDs only beyond retained count |
| AlterPartitionReassignments blocked  | Reassign draining partition → PARTITION_OPERATION_IN_PROGRESS                                                                                  |
| DeletePartitions during reassignment | Partition being reassigned → DeletePartitions returns PARTITION_OPERATION_IN_PROGRESS                                                          |
| Topic deletion during drain          | DeleteTopics → drain cancelled implicitly → topic removed                                                                                      |
| Multiple topics concurrently         | DeletePartitions on 3 different topics simultaneously → all succeed independently                                                              |
| Consumer group offset cleanup        | After partition removed → verify offsets tombstoned in __consumer_offsets                                                                      |
| Partition reuse after delete | Deleted ID reuse rejected, including after snapshot restore |
| CLI: --delete-partitions             | kafka-topics.sh --alter --delete-partitions 2 → succeeds                                                                                       |
| partition.drain.timeout.ms=0         | No minimum wait; fresh safety confirmation still required unless explicitly forced                                                                                                             |
| MetadataVersion check                | DeletePartitions with version < IBP_4_4_IV2 → UnsupportedVersionException                                                                      |
| Internal topic protection            | DeletePartitions on __consumer_offsets → INVALID_REQUEST                                                                                       |
| Mutation quota                       | Large deleteCount with low quota → THROTTLING_QUOTA_EXCEEDED                                                                                   |

### Edge Case / Failure Tests

| Scenario                                  | Verification                                                                                                                    |
|-------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| Controller failover during drain          | Restores deadlines/force, clears readiness; normal removal waits for fresh confirmation                             |
| Controller failover after deadline passed | Clears cached readiness; normal drain waits for fresh confirmation; force remains persisted                                                        |
| Broker restart during drain               | Broker replays metadata → resumes serving fetch for draining partition → rejects produce                                        |
| Broker restart after removal              | Broker catches up → deletes local log segments for removed partition                                                            |
| deleteCount = partitionCount - 1          | Only partition 0 remains. Topic still functional.                                                                               |
| deleteCount = partitionCount              | Rejected: INVALID_DELETE_PARTITION_COUNT (must keep at least 1)                                                                 |
| deleteCount = 0                           | Rejected: INVALID_DELETE_PARTITION_COUNT                                                                                        |
| deleteCount negative                      | Rejected: INVALID_DELETE_PARTITION_COUNT                                                                                        |
| Remote storage enabled                    | After removal → RemoteLogManager.stopPartitions() called → remote segments scheduled for deletion                               |
| Compacted topic                           | DeletePartitions on compacted topic → allowed (same behavior as delete topic). Operator responsibility to understand data loss. |

### Replica Lifecycle Tests

| Scenario                                            | Verification                                                                                               |
|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| Leader crash during draining                        | New leader elected from ISR; draining preserved; old safety confirmation invalidated |
| All replicas crash during draining                  | Deadline stays fixed; normal removal waits for leader safety; force permits removal and later stale-dir cleanup             |
| Follower crash + recovery before deadline           | Follower rejoins ISR → no impact on drain                                                                  |
| Follower crash + recovery after removal             | Stale log dir detected → async deleted                                                                     |
| Controlled shutdown of leader during drain          | Leadership migrated to follower → consumer reconnects → drain unaffected                                   |
| Txn abort marker with min.isr unsatisfied | Actual WriteTxnMarkers returns ISR error; retries until recovery; normal removal waits, explicit force may override removal safety |
| Txn commit marker during drain                      | Allowed through → transaction terminally committed → data eventually lost with partition                   |
| In-flight acks=all produce when drain starts        | Already-appended produce completes successfully → subsequent produces rejected                             |
| Unclean leader election during drain                | Follows topic config; if enabled, out-of-ISR follower becomes leader; consumer may see data loss           |
| JBOD disk failure on leader during drain            | Same as leader crash → new leader elected                                                                  |
| Metadata snapshot taken during drain                | Snapshot contains draining state → new broker loading snapshot sees correct state                          |
| Metadata snapshot taken after removal               | Partition absent from snapshot → recovering broker detects stale dir → cleanup                             |
| Fetch session with draining partition removed       | Incremental fetch returns UNKNOWN_TOPIC_OR_PARTITION → client refreshes metadata                           |
| Log cleaner running on compacted draining partition | Cleaner continues until removal → abortCleaning() called during asyncDelete                                |
| Offline broker during RemovePartitionRecord         | Online brokers clean up immediately → offline broker cleans up on recovery (idempotent)                    |
| Broker crashes mid-asyncDelete (dir renamed)        | On restart, LogManager detects .delete dir → re-queues for background deletion                             |

---

## Rejected Alternatives

### Alternative 1: Extend CreatePartitions to Support Decrease

Allow `CreatePartitions` with a target count lower than the current count.

**Rejected because:**

- Violates the API's semantic contract (name says "create")
- Introduces a two-phase draining mechanism into an API designed for synchronous partition creation
- Confusing UX: users expect `CreatePartitions` to be instant, not deferred

### Alternative 2: Config-Driven Deletion (Set target.partition.count)

Set a topic config like `target.partition.count=7` and let the controller shrink to match.

**Rejected because:**

- Configs should be declarative state, not imperative triggers with side effects
- Hard to observe progress (when did deletion start? what's the deadline?)
- No clear error reporting path for invalid configurations
- Violates the principle that config changes are reversible (deleting partitions is not)

### Alternative 3: Allow Deleting Arbitrary (Non-Tail) Partitions

Allow specifying exact partition IDs to delete (e.g., delete partition 3 and 7).

**Rejected for MVP because:**

- Creates partition ID gaps: [0,1,2,4,5,6,8,9]
- Breaks `hash(key) % partitionCount` — requires fundamentally changing the partitioner
- Requires all producers to support "partition list" routing instead of "partition count" routing
- Much larger client-side change; can be added in a future KIP if needed

### Alternative 4: No Drain Period (Immediate Deletion)

Delete partitions immediately without a draining phase.

**Rejected as default because:**

- Data loss for consumers that haven't caught up
- Existing in-flight transactions may fail uncleanly
- Zero minimum wait is supported via `partition.drain.timeout.ms=0`; bypassing transaction safety additionally requires explicit force

### Alternative 5: Drain Until Lag = 0 (Smart Completion)

Instead of a fixed timeout, monitor consumer group lag and delete when all consumers have caught up.

**Rejected because:**

- Not all consumers are registered consumer groups (manual assignment, Streams internal consumers)
- Determining "all consumers" is impossible — there may be consumers unknown to the broker
- A stuck consumer would block deletion indefinitely
- A minimum interval plus transaction safety protects normal deletion; explicit force gives operators control
- Can be added as a future enhancement without breaking the current design

---

## Future Work (Not in This KIP)

1. **Cancel Drain** — A `CancelPartitionDrainingRecord` to revert partitions from DRAINING back to ACTIVE
2. **Arbitrary Partition Deletion** — Allow deleting non-tail partitions with partition-list-based routing
3. **Smart Drain Completion** — Option to complete drain early when all known consumer groups have zero lag
4. **Drain Progress API** — Expose per-partition lag across all consumer groups in DescribeTopicPartitions
5. **MirrorMaker Integration** — MM2 option to propagate partition deletion to target cluster
6. **Offset Cleanup Policy** — Configurable retention for orphaned offsets after partition deletion

---

## Out of Scope (MVP)

- Deleting arbitrary (non-tail) partitions
- A separate cancellation API (CreatePartitions can reopen retained draining IDs)
- Early completion when lag reaches 0
- Automatic MirrorMaker target shrinking
- Kafka Streams automatic state store cleanup for deleted partitions (handled by existing rebalance)
