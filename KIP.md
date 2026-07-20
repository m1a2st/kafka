# KIP-XXX: Delete Partitions

## Status

- **Current state:** Draft
- **Discussion thread:** (link to dev mailing list thread)
- **JIRA:** KAFKA-XXXXX
- **Pull request:** (link to PR)

## Motivation

### The Problem

Kafka supports increasing the partition count of a topic (`CreatePartitions` API) but provides no mechanism to decrease
it. This asymmetry has been a known limitation since 2012
([KAFKA-347](https://issues.apache.org/jira/browse/KAFKA-347)) and was explicitly requested as a feature in 2014
([KAFKA-1231](https://issues.apache.org/jira/browse/KAFKA-1231), resolved "Won't Fix" in 2020). The StackOverflow
question "[How to decrease number partitions Kafka topic?](https://stackoverflow.com/questions/45497878/how-to-decrease-number-partitions-kafka-topic)"
has accumulated 37 votes and over 50,000 views, indicating sustained real-world demand.

The only workaround today is to delete and recreate the topic, which causes:

- Complete data loss of all unconsumed messages across all partitions (not just the ones being removed)
- Consumer group offset reset across all consumer groups
- Hard failures in downstream systems (Kafka Streams, Connect, MirrorMaker)
- Loss of ACLs, configs, and quotas that must be manually re-applied
- Operational disruption requiring coordinated downtime

### Why Kafka Has Never Supported This

The commonly cited reason — "reducing partitions causes data loss" — is imprecise. The true technical difficulty is
rooted in Kafka's ordering model:

**Kafka guarantees message ordering within each partition, but does not guarantee ordering across partitions.**

If data in a removed partition were to be redistributed to the remaining partitions, the ordering guarantee of the
target partitions would be violated. There is no way to merge messages from partition N into partition M while
preserving the total order of both. This is fundamental — not an implementation limitation, but a semantic
impossibility given Kafka's ordering contract.

However, this analysis only applies to a design that attempts to **redistribute data** from removed partitions into
other partitions. It does not apply to a design that simply **removes partitions and lets their data expire under
the existing retention policy**.

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

| Property                | Expansion (3→5) | Removal (5→3)                                   |
|-------------------------|-----------------|-------------------------------------------------|
| Existing partition data | Untouched       | Untouched                                       |
| Ordering guarantee      | Preserved       | Preserved                                       |
| Key affinity            | Breaks          | Breaks                                          |
| Data loss               | None            | Draining partitions expire per retention policy |

Both operations share the same key-affinity disruption that producers already handle today (via metadata refresh and
re-routing). The only additional cost of removal is that historical data in the draining partitions will eventually
be lost after the retention period expires. This is an explicit, operator-controlled trade-off — not an ordering
violation.

### Use Cases

1. **Capacity right-sizing** — A topic was provisioned with 256 partitions for anticipated load that never
   materialized. Each idle partition still costs ~1 MB heap, file handles, and ISR heartbeat overhead per broker.
2. **Over-partition recovery** — An operator accidentally set partition count too high. Today the only fix is to
   delete the entire topic and recreate it, disrupting all producers and consumers.
3. **Cost reduction in cloud environments** — Partition count directly affects billing in managed Kafka offerings.
   Reducing unused partitions saves cost without service interruption.
4. **Post-peak scale-down** — A topic was scaled up for a seasonal traffic peak and should be scaled back down
   afterward, mirroring how brokers and consumer instances are scaled down.
5. **Multi-tenant platform management** — Platform operators need to reclaim resources from tenants whose usage has
   decreased, without forcing a destructive topic recreation.

## Proposed Changes

Partition deletion is a two-phase process managed entirely by the KRaft controller:

1. **Draining** — Partitions stop accepting writes but continue serving reads for a configurable duration
   (`partition.drain.timeout.ms`), allowing consumers to drain remaining data.
2. **Removal** — After the drain deadline expires, partitions are permanently removed from metadata and
   their physical storage is cleaned up on all brokers.

### Partition State Machine

```
ACTIVE ──[DeletePartitionsRequest]──→ DRAINING ──[drain deadline expires]──→ REMOVED
  │                                      │
  └──[DeleteTopics]──→ REMOVED           └──[DeleteTopics]──→ REMOVED
```

State is derived from metadata replay:

- No `PartitionDrainingRecord` for this partition → ACTIVE
- `PartitionDrainingRecord` written, no `RemovePartitionRecord` → DRAINING
- `RemovePartitionRecord` written → REMOVED (partition no longer exists in TopicImage)

### Metadata Records

#### PartitionDrainingRecord (Record ID 29)

Written by the controller when `DeletePartitionsRequest` is accepted. Marks tail partitions as draining and records the absolute wall-clock deadline for removal.

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
      "name": "PartitionIds",
      "type": "[]int32",
      "versions": "0+",
      "about": "The partition IDs entering the draining state. These are always the tail partitions: [currentCount - deleteCount, ..., currentCount - 1]."
    },
    {
      "name": "DrainDeadlineMs",
      "type": "int64",
      "versions": "0+",
      "about": "The absolute wall-clock timestamp (ms since epoch) after which the controller will write RemovePartitionRecords for these partitions."
    }
  ]
}
```

#### RemovePartitionRecord (Record ID 30)

Written by the controller when the drain deadline expires. Permanently removes the partition from all metadata images.

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

### Controller Behavior

#### Processing DeletePartitionsRequest

The controller validates the request and initiates the two-phase deletion:

```
For each topic in request:
  1. Feature gate: if metadataVersion < IBP_4_4_IV2 → UNSUPPORTED_VERSION
  2. Resolve topic name → TopicControlInfo
     - Not found → UNKNOWN_TOPIC_OR_PARTITION (3)
  3. Authorization: require ALTER on topic
     - Denied → TOPIC_AUTHORIZATION_FAILED (29)
  4. Internal topic check: if Topic.isInternal(name) → INVALID_REQUEST (42)
  5. Validate deleteCount:
     - deleteCount <= 0 → INVALID_DELETE_PARTITION_COUNT (137)
     - deleteCount >= currentPartitionCount → INVALID_DELETE_PARTITION_COUNT (137)
  6. Check no existing draining operation:
     - Any partition in this topic already DRAINING → PARTITION_OPERATION_IN_PROGRESS (136)
  7. Check no active reassignment on targeted partitions:
     - For each targetPartitionId: if partitionRegistration.isReassigning()
       → PARTITION_OPERATION_IN_PROGRESS (136)
  8. Apply controller mutation quota:
     - If throttled → THROTTLING_QUOTA_EXCEEDED (89)
  9. Read topic config: partition.drain.timeout.ms (default 3600000)
  10. Compute absolute deadline: controller.time.milliseconds() + drainTimeoutMs
  11. Write PartitionDrainingRecord { topicId, targetPartitionIds, drainDeadlineMs }
  12. Schedule deferred removal event at drainDeadlineMs
  13. Return success
```

#### Deferred Removal (Drain Deadline Expiry)

The KRaft controller uses a `ScheduledExecutorService` for wall-clock scheduling. The callback does NOT write metadata directly — it only enqueues a `RemovePartitionsEvent` onto the controller event queue, maintaining the single-writer guarantee.

```java
// In QuorumController:
private final ScheduledExecutorService drainScheduler =
    Executors.newSingleThreadScheduledExecutor(
        new ThreadFactory("controller-drain-scheduler"));

private final Map<Uuid, ScheduledFuture<?>> pendingDrainRemovals = new ConcurrentHashMap<>();
```

When the event fires:
1. Verify topic still exists (may have been deleted while waiting)
2. Verify partitions are still in DRAINING state
3. For each partitionId: write `RemovePartitionRecord { topicId, partitionId }`
4. Remove from `pendingDrainRemovals`

#### Controller Failover Recovery

On new controller activation (after metadata log replay):
1. All `PartitionDrainingRecord` entries are replayed → draining state restored in TopicImage
2. Controller checks all draining sets against current wall-clock time
3. If deadline already passed → immediately schedule `RemovePartitionRecord` writes
4. If deadline in future → re-schedule with remaining delay

#### Topic Deletion During Draining

When `RemoveTopicRecord` is replayed, the deferred removal event is cancelled:
```
ScheduledFuture<?> future = pendingDrainRemovals.remove(topicId);
if (future != null) future.cancel(false);
```

### Broker Behavior

#### During DRAINING

| Mechanism | Behavior | Rationale |
|-----------|----------|-----------|
| Produce requests (data) | Rejected with `NOT_LEADER_OR_FOLLOWER` | Producers refresh metadata and reroute |
| Produce requests (txn COMMIT/ABORT markers) | Allowed with acks=1 (leader-only) | Transactions must resolve cleanly |
| Fetch requests | Served normally | Consumers drain remaining data |
| Follower replication | Continues normally | Leader failover requires caught-up followers |
| ISR management | Unchanged | Standard ISR expand/shrink behavior |
| Leader election | Allowed | Consumers need available leader |
| ListOffsets / OffsetFetch / OffsetCommit | Served normally | Consumers may still commit progress |

#### On REMOVAL (RemovePartitionRecord received)

When a broker receives `RemovePartitionRecord` via metadata update:

1. **Stop fetchers** (if follower): `ReplicaFetcherManager.removeFetcherForPartitions({partition})`
2. **Complete delayed operations**: Any `DelayedProduce` → error; any `DelayedFetch` → empty response
3. **Stop partition**: `Partition.delete()` — clear replica map, reset state, notify listeners, remove metrics
4. **Remove from serving map**: `allPartitions.remove(topicPartition)`
5. **Async log deletion**: `LogManager.asyncDelete()` — rename dir to `.delete` suffix, queue for background cleanup
6. **Update checkpoint files**: recovery-point-offset, log-start-offset, replication-offset (HW) rewritten excluding this partition
7. **Remote storage cleanup** (if `remote.log.storage.enable=true`): `RemoteLogManager.stopPartitions(deleteRemoteLog=true)` — async segment deletion

#### Offline Broker Recovery

- **Case 1** (RemovePartitionRecord still in log): Broker replays log → processes removal → standard cleanup
- **Case 2** (compacted into snapshot): Partition absent from snapshot → stale dir detection → renamed to `.stray` suffix → background cleanup

### Producer Behavior

Producers require **zero code changes**. The existing retry logic handles draining transparently:

1. Producer sends to draining partition → `NOT_LEADER_OR_FOLLOWER`
2. `NOT_LEADER_OR_FOLLOWER` extends `RetriableException` → producer calls `metadata.requestUpdate()`
3. Metadata refresh returns:
   - **v0–v13**: Draining partitions excluded from partition list; producer sees reduced count
   - **v14**: Draining partitions included with `IsDraining=true`; producer's `availablePartitionsForTopic()` filters them
4. Producer re-routes via `hash(key) % newActiveCount`
5. Convergence time: ~1-2 RTTs for active producers (error-driven)

The sticky partitioner adapts naturally: when the current sticky partition becomes draining/unavailable, `nextPartition()` picks from active partitions.

### Consumer Behavior

#### During DRAINING

- Consumer group assignment **still includes** draining partitions
- Consumers fetch and commit offsets normally — draining the remaining data
- No rebalance triggered by entering DRAINING (partition still exists)

#### On REMOVAL

- Fetch returns `UNKNOWN_TOPIC_OR_PARTITION` → metadata refresh → consumer group rebalance
- Assignment updated to exclude removed partitions
- **Manual-assign consumers** must detect the error and update their assignment at the application level

### Consumer Offset Cleanup

When partitions are removed, committed offsets in `__consumer_offsets` become orphaned. The group coordinator's `OffsetMetadataManager.onPartitionsDeleted(topicId, removedPartitionIds)` generates tombstone records for these offsets, which are eventually compacted away. If tombstone generation fails, orphaned offsets expire naturally via `offsets.retention.minutes` (default 7 days).

### Transaction Handling

- In-flight transactions that include a draining partition: data produce is rejected, but COMMIT/ABORT markers are allowed through with relaxed acks (leader-only) so the transaction resolves cleanly.
- `AddPartitionsToTxn` for draining partitions: not possible — producer's metadata excludes them after refresh.
- `WriteTxnMarkers` to removed partitions: broker returns `UNKNOWN_TOPIC_OR_PARTITION`; transaction coordinator retries until metadata propagates, then drops the partition from the pending set. No stuck transactions.

### Features Integration

#### Idempotent Producers

`ProducerStateManager` is per-partition. When the partition is removed and its log deleted, producer state (`.snapshot` files) are deleted as part of `LogManager.asyncDelete()`. No cross-partition impact. If the same partition ID is later reused via `CreatePartitions`, it starts with fresh state — the `logCreationOrDeletionLock` serializes rename-then-create.

#### Share Groups (KIP-932)

When a partition is removed, `ShareCoordinatorService` detects the removal via `TopicsDelta` and writes tombstone records for the partition's delivery state entries in `__share_group_state`. Unacknowledged deliveries are abandoned (data is being deleted anyway).

#### Kafka Streams

Metadata update triggers consumer group rebalance → `StreamsPartitionAssignor` recalculates task assignments with fewer partitions → `TaskManager.handleRevocation()` commits state and closes tasks for removed partitions. Internal changelog/repartition topics retain their original partition count; operators must reset the application if partition count must match.

#### Kafka Connect / MirrorMaker 2

- **Sink connectors**: connector rebalance handles removal automatically
- **Source connectors**: producer routing handles draining automatically
- **MirrorMaker 2**: detects partition deletion on source; does NOT propagate to target. Operators must separately delete partitions on target.

#### Remote/Tiered Storage

During DRAINING, remote log manager operates normally (no new uploads since no new data). On REMOVAL, `RemoteLogManager.stopPartitions(deleteRemoteLog=true)` schedules async deletion of remote segments. Remote cleanup failure does not block local cleanup.

#### Log Compaction

Log cleaner continues during DRAINING (keeps log small). On removal, `cleaner.abortCleaning(topicPartition)` stops any in-progress compaction before directory rename.

### Interactions With Other Systems

| System | Behavior |
|--------|----------|
| CreatePartitions | Blocked during draining (`PARTITION_OPERATION_IN_PROGRESS`) |
| AlterPartitionReassignments | Blocked for draining partitions |
| DeletePartitions during reassignment | Rejected (`PARTITION_OPERATION_IN_PROGRESS`) |
| DeleteTopics | Allowed; supersedes draining (cancels scheduled removal) |
| ElectLeaders | Allowed for draining partitions |
| Internal topics | Protected; returns `INVALID_REQUEST` |
| Retention policy | Continues running during draining (not paused) |
| Fetch sessions | Incremental fetch returns `UNKNOWN_TOPIC_OR_PARTITION` on removal; client removes partition |
| Static membership (KIP-345) | Partition removal triggers immediate rebalance (metadata change, not member failure) |

### Failure Handling

| Layer | Failure | Client Impact | Recovery |
|-------|---------|:---:|----------|
| Controller RemovePartitionRecord write | Fenced mid-write | None | Automatic — new controller re-schedules |
| Broker log rename (asyncDelete step 1) | Disk error | None — partition already gone from metadata | Retry on broker restart (stale dir detection) |
| Broker log file deletion (step 2) | I/O error | None | Background thread retries every 60s; survives restarts |
| Remote storage deletion | Network/throttling | None | Background cleanup or manual; segments inaccessible |
| Offset tombstone generation | Coordinator unavailable | None | Natural expiry (7 days) + compaction |

**Design principle:** Partition removal from metadata is the authoritative action. All local/remote cleanup is best-effort and eventually consistent. No cleanup failure can cause the partition to "come back."

### Partition ID Reuse

After removal, partition IDs form a contiguous range `[0, count-1]`. If partitions are later added via `CreatePartitions`, new partitions receive IDs starting from the new count. New partitions start with fresh empty logs; orphaned offsets and state from old partitions are cleaned up.

## Public Interfaces

### New API: DeletePartitions (API Key 94)

A new RPC that removes partitions from the tail of a topic. The request is forwarded to the active controller.

#### DeletePartitionsRequest (v0)

```json
{
  "apiKey": 94,
  "type": "request",
  "listeners": ["broker", "controller"],
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
          "name": "DeleteCount",
          "type": "int32",
          "versions": "0+",
          "about": "The number of partitions to remove from the tail. For example, if a topic has 10 partitions and DeleteCount is 3, partitions 7, 8, 9 will be marked for deletion."
        }
      ]
    },
    {
      "name": "TimeoutMs",
      "type": "int32",
      "versions": "0+",
      "about": "The time in ms to wait for the controller to process the request. This is NOT the drain timeout; the drain timeout is configured per-topic via partition.drain.timeout.ms."
    }
  ]
}
```

#### DeletePartitionsResponse (v0)

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

### Modified API: MetadataResponse (v13 → v14)

New field under `MetadataResponsePartition`:

```json
{
  "name": "IsDraining",
  "type": "bool",
  "versions": "14+",
  "default": "false",
  "ignorable": true,
  "about": "True if this partition is in the draining state and will be removed after the drain deadline. Draining partitions do not accept produce requests."
}
```

Backward compatibility:

| MetadataResponse version | Draining partition handling |
|:---:|---|
| v0–v13 | Draining partitions **excluded** from partition list. Old producers see reduced partition count and rehash immediately. |
| v14 | Draining partitions **included** with `IsDraining=true`. New producers explicitly skip them for observability. |

`MetadataRequest` validVersions bumped from `"0-13"` to `"0-14"`. No new request fields.

### Modified API: DescribeTopicPartitionsResponse (v0 → v1)

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
  "about": "The absolute timestamp in ms when this partition will be forcibly removed, or -1 if not draining."
}
```

`DescribeTopicPartitionsRequest` validVersions bumped from `"0"` to `"0-1"`. No new request fields.

### Admin Client

New methods in `Admin.java`:

```java
/**
 * Delete partitions from the tail of the specified topics.
 *
 * <p>Partitions enter a "draining" state where they stop accepting writes but continue
 * serving reads. After the topic's configured partition.drain.timeout.ms expires,
 * the partitions are permanently removed.
 *
 * <p>This operation requires ALTER permission on the topic.
 *
 * @param deletions A map from topic name to the number of partitions to delete from the tail.
 * @return The DeletePartitionsResult.
 */
default DeletePartitionsResult deletePartitions(Map<String, DeletePartitionsCount> deletions) {
    return deletePartitions(deletions, new DeletePartitionsOptions());
}

/**
 * Delete partitions from the tail of the specified topics.
 *
 * @param deletions A map from topic name to the number of partitions to delete from the tail.
 * @param options   Additional options for the request.
 * @return The DeletePartitionsResult.
 */
DeletePartitionsResult deletePartitions(
        Map<String, DeletePartitionsCount> deletions,
        DeletePartitionsOptions options
);
```

#### DeletePartitionsCount

```java
package org.apache.kafka.clients.admin;

/**
 * Specifies the number of partitions to delete from the tail of a topic.
 */
public class DeletePartitionsCount {
    private final int deleteCount;

    /**
     * @param deleteCount The number of partitions to remove from the tail. Must be positive and
     *                    less than the current partition count (at least one partition must remain).
     */
    public DeletePartitionsCount(int deleteCount) {
        this.deleteCount = deleteCount;
    }

    public int deleteCount() {
        return deleteCount;
    }
}
```

#### DeletePartitionsOptions

```java
package org.apache.kafka.clients.admin;

/**
 * Options for {@link Admin#deletePartitions(Map, DeletePartitionsOptions)}.
 */
public class DeletePartitionsOptions extends AbstractOptions<DeletePartitionsOptions> {
}
```

#### DeletePartitionsResult

```java
package org.apache.kafka.clients.admin;

import org.apache.kafka.common.KafkaFuture;
import java.util.Map;

/**
 * Result of {@link Admin#deletePartitions(Map, DeletePartitionsOptions)}.
 *
 * <p>The futures complete when the controller has accepted the deletion request and written the
 * PartitionDrainingRecord. Completion does NOT mean the partitions have been removed — they
 * remain in draining state until the configured timeout expires.
 */
public class DeletePartitionsResult {
    private final Map<String, KafkaFuture<Void>> futures;

    DeletePartitionsResult(Map<String, KafkaFuture<Void>> futures) {
        this.futures = futures;
    }

    /**
     * @return A map from topic name to a future that completes when draining has been initiated.
     */
    public Map<String, KafkaFuture<Void>> values() {
        return futures;
    }

    /**
     * @return A future that completes when all topics have entered draining state.
     */
    public KafkaFuture<Void> all() {
        return KafkaFuture.allOf(futures.values().toArray(new KafkaFuture[0]));
    }
}
```

### Command-Line Interface

`kafka-topics.sh` gains a `--delete-partitions` option under `--alter`:

```
kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic my-topic \
    --delete-partitions 3
```

Delete the last 3 partitions (enters draining state):

```
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders \
    --delete-partitions 3
Initiated partition deletion for topic 'orders': partitions [7, 8, 9] entering DRAINING state.
Drain deadline: 2026-07-15T15:00:00Z (partition.drain.timeout.ms=3600000)
```

Check draining status via describe:

```
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --describe --topic orders
Topic: orders   TopicId: abc123   PartitionCount: 10   ReplicationFactor: 3
  Partition: 0   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
  ...
  Partition: 7   Leader: 2   Replicas: 2,3,1   Isr: 2,3,1   Draining: true   Deadline: 2026-07-15T15:00:00Z
  Partition: 8   Leader: 3   Replicas: 3,1,2   Isr: 3,1,2   Draining: true   Deadline: 2026-07-15T15:00:00Z
  Partition: 9   Leader: 1   Replicas: 1,2,3   Isr: 1,2,3   Draining: true   Deadline: 2026-07-15T15:00:00Z
```

Set drain timeout to zero for immediate removal:

```
$ bin/kafka-configs.sh --bootstrap-server localhost:9092 \
    --entity-type topics --entity-name orders \
    --alter --add-config partition.drain.timeout.ms=0

$ bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --alter --topic orders \
    --delete-partitions 3
Initiated partition deletion for topic 'orders': partitions [7, 8, 9] removed immediately (drain timeout = 0).
```

Error examples:

```
$ bin/kafka-topics.sh --alter --topic orders --delete-partitions 2
Error: A partition operation is already in progress for topic 'orders'.

$ bin/kafka-topics.sh --alter --topic orders --delete-partitions 10
Error: The requested delete partition count is invalid. Cannot delete all partitions; at least one must remain.
```

`--delete-partitions` and `--partitions` (increase) are mutually exclusive.

### Configuration

#### Topic Configuration

| Config | Type | Default | Valid Values | Description |
|--------|------|---------|--------------|-------------|
| `partition.drain.timeout.ms` | long | 3600000 (1 hour) | >= 0 | Time to wait after marking tail partitions as draining before forcibly removing them. During this window, consumers can read but producers cannot write. A value of 0 means immediate removal. Config is only read at the time `DeletePartitionsRequest` is processed; changing it after draining starts has no effect on the existing deadline. |

### MetadataVersion

```java
// Add support for partition deletion (DeletePartitions API, PartitionDrainingRecord, RemovePartitionRecord).
IBP_4_4_IV2(33, "4.4", "IV2", true);
```

Feature gate method:

```java
public boolean isDeletePartitionsSupported() {
    return this.isAtLeast(IBP_4_4_IV2);
}
```

The `true` flag indicates metadata format changed (new record types).

### Error Codes

| Code | Name | Retriable | Returned By | Condition |
|:----:|------|:---------:|-------------|-----------|
| 3 | `UNKNOWN_TOPIC_OR_PARTITION` | Yes | `DeletePartitions`, `Produce`, `Fetch` | Topic does not exist, or partition removed |
| 6 | `NOT_LEADER_OR_FOLLOWER` | Yes | `Produce` | Produce request targets a draining partition |
| 29 | `TOPIC_AUTHORIZATION_FAILED` | No | `DeletePartitions` | Caller lacks ALTER permission on topic |
| 35 | `UNSUPPORTED_VERSION` | No | `DeletePartitions` | MetadataVersion < IBP_4_4_IV2 |
| 42 | `INVALID_REQUEST` | No | `DeletePartitions` | Topic is internal (`__consumer_offsets`, `__transaction_state`, etc.) |
| 89 | `THROTTLING_QUOTA_EXCEEDED` | Yes | `DeletePartitions` | Controller mutation quota exhausted |
| 136 | `PARTITION_OPERATION_IN_PROGRESS` | No | `DeletePartitions`, `CreatePartitions`, `AlterPartitionReassignments` | Topic already has draining partitions, OR partition reassignment in progress for targeted partitions |
| 137 | `INVALID_DELETE_PARTITION_COUNT` | No | `DeletePartitions` | deleteCount <= 0, OR deleteCount >= currentPartitionCount |

New exception classes:

```java
package org.apache.kafka.common.errors;

public class PartitionOperationInProgressException extends ApiException {
    public PartitionOperationInProgressException(String message) {
        super(message);
    }
}

public class InvalidDeletePartitionCountException extends ApiException {
    public InvalidDeletePartitionCountException(String message) {
        super(message);
    }
}
```

### Metrics

#### New JMX Metrics

| Name | Type | MBean | Description |
|------|------|-------|-------------|
| `DrainingPartitionCount` | Gauge | `kafka.controller:type=KafkaController,name=DrainingPartitionCount` | Number of partitions currently in DRAINING state across all topics. Only reported by the active controller. |
| `PendingDrainRemovals` | Gauge | `kafka.controller:type=KafkaController,name=PendingDrainRemovals` | Number of scheduled deferred removal events pending in the DrainScheduler. |
| `DrainCompletedTotal` | Meter | `kafka.controller:type=KafkaController,name=DrainCompletedTotal` | Cumulative count of partitions that transitioned from DRAINING to REMOVED. |
| `DeletePartitionsRequestsPerSec` | Meter | `kafka.network:type=RequestMetrics,name=RequestsPerSec,request=DeletePartitions` | Rate of DeletePartitions requests received. |
| `DeletePartitionsTotalTimeMs` | Histogram | `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=DeletePartitions` | Total time to process DeletePartitions requests. |
| `DeletePartitionsErrorsPerSec` | Meter | `kafka.network:type=RequestMetrics,name=ErrorsPerSec,request=DeletePartitions,error={errorName}` | Rate of errors returned by DeletePartitions, broken down by error code. |

#### Existing Metrics Behavior

| Metric | During DRAINING | After REMOVAL |
|--------|----------------|---------------|
| `kafka.server:...MessagesInPerSec,topic=X` | Drops to 0 (no writes) | Partition contribution removed |
| `kafka.server:...BytesOutPerSec,topic=X` | Consumer reads continue | Partition contribution removed |
| `kafka.server:...PartitionCount` | Unchanged (partition still exists) | Decremented |
| `kafka.server:...UnderReplicatedPartitions` | May include draining if ISR < replicas | Removed from count |
| `kafka.server:...ConsumerLag,topic=X,partition=N` | Continues reporting | Metric deregistered |

## Compatibility, Deprecation, and Migration Plan

### Compatibility Matrix

| Component | Version Requirement | Notes |
|-----------|-------------------|-------|
| Controller | IBP_4_4_IV2+ | Must support PartitionDrainingRecord, RemovePartitionRecord |
| Brokers | IBP_4_4_IV2+ | Must support draining state in metadata image |
| Admin Client (caller) | 4.4+ | Must support DeletePartitions API key 94 |
| Producers | Any version | No code changes. v0-v13: draining partitions excluded. v14: IsDraining field. |
| Consumers (group) | Any version | No code changes. Standard rebalance handles removal. |
| Consumers (manual assign) | Any version | Application must handle `UNKNOWN_TOPIC_OR_PARTITION` and update assignment. |
| Kafka Streams | Any version | Rebalance handles task reassignment automatically. |
| Kafka Connect | Any version | Connector rebalance handles removal automatically. |
| MirrorMaker 2 | Any version | Detects deletion on source; does NOT propagate to target. |

### Wire Protocol Compatibility

| Client version | Behavior |
|---|---|
| Old client, old API version | Cannot call DeletePartitions (unknown API key → `UnsupportedVersionException`) |
| Old producer (MetadataResponse v0-v13) | Draining partitions excluded from metadata. Producer sees reduced count. |
| Old consumer | Continues consuming from draining partitions. On removal, rebalance triggered. |
| New client (v14 metadata) | Sees `IsDraining=true`. Can log warning for observability. |

### Cluster Upgrade Path

1. Rolling upgrade all brokers/controllers to version supporting IBP_4_4_IV2
2. Set MetadataVersion to IBP_4_4_IV2 via `kafka-features.sh --bootstrap-server :9092 upgrade --feature metadata.version=33`
3. DeletePartitions API becomes available

### Rollback

- Partitions already REMOVED cannot be recovered (data is deleted)
- Partitions in DRAINING: on MetadataVersion downgrade, the new controller ignores unknown record types (standard KRaft forward-compatibility behavior). The draining state becomes inert.
- **Recommendation:** Test with non-critical topics first.

### Release Phases

| Phase | MetadataVersion | Stability | Enablement |
|-------|----------------|-----------|------------|
| Early Access | IBP_4_4_IV2 (unstable) | `unstable.feature.versions.enable=true` required | Explicit opt-in. Not for production. |
| General Availability | Future IBP (stable) | Stable, no flags needed | Enabled by default at target MetadataVersion. |

## Test Plan

### Unit Tests

- All controller validation paths (invalid count, internal topic, unauthorized, reassignment conflict, duplicate drain)
- State machine transitions (ACTIVE → DRAINING → REMOVED)
- Metadata record replay (PartitionDrainingRecord, RemovePartitionRecord)
- MetadataResponse v14 serialization with IsDraining field
- DescribeTopicPartitionsResponse v1 with IsDraining and DrainDeadlineMs
- New error code serde
- Topic config validation (`partition.drain.timeout.ms`)
- DeletePartitionsRequest/Response serde round-trip
- `PartitionRegistration` builder with `draining=true`
- `TopicImage.hasDrainingPartitions()` / `activePartitionCount()`

### Integration Tests

- Full lifecycle: delete → drain period → removal → log cleanup verified
- Producer convergence: produce to draining partition → error → metadata refresh → reroute succeeds
- Consumer draining: fetch from draining partition → data received → removal → rebalance
- Transaction handling: open txn → draining → abort marker allowed → transaction resolved
- Blocking: CreatePartitions/AlterPartitionReassignments blocked during drain
- Topic deletion during drain: supersedes and cancels drain
- Controller failover during drain: new controller resumes countdown
- Controller failover after deadline passed: immediate removal
- Broker restart during/after drain: correct state restored
- Multiple replicas (RF=3): all brokers clean up log dirs on removal
- Partition reuse: delete → CreatePartitions → fresh empty partitions
- drain timeout = 0: immediate removal
- Internal topic protection
- CLI integration (`--delete-partitions`)
- Mutation quota: large deleteCount with low quota → THROTTLING_QUOTA_EXCEEDED
- MetadataVersion check: version < IBP_4_4_IV2 → UNSUPPORTED_VERSION
- Consumer group offset cleanup: offsets tombstoned after removal
- Share group state cleanup: delivery state tombstoned after removal
- Multiple topics concurrently: independent drain operations proceed in parallel

### Failure Tests

- Controller failover mid-drain and after deadline expiry
- Broker offline during removal → recovery and cleanup
- Leader crash during draining → new leader elected, drain continues
- All replicas offline → deadline fires → removal on recovery
- Follower crash + recovery before/after deadline
- Controlled shutdown of leader during drain
- JBOD disk failure during drain
- In-flight acks=all produce when drain starts (already-appended succeeds)
- Txn abort marker with min.isr unsatisfied (acks=1 bypass)
- WriteTxnMarkers retry after partition removal (converges via metadata propagation)
- Metadata snapshot taken during drain / after removal
- Fetch session with draining partition removed
- Remote storage deletion failure (does not block local cleanup)

## Rejected Alternatives

### 1. Extend CreatePartitions to Support Decrease

Allow `CreatePartitions` with a target count lower than current.

**Rejected**: Violates API semantic contract ("create" implies adding). Introduces deferred behavior into a
synchronous API. Confusing UX.

### 2. Config-Driven Deletion (target.partition.count)

Set a topic config and let the controller converge.

**Rejected**: Configs should be declarative state, not imperative triggers. Hard to observe progress. No
clear error path. Violates principle that config changes are reversible.

### 3. Redistribute Data to Remaining Partitions

Move messages from deleted partitions into surviving partitions.

**Rejected**: Fundamentally violates Kafka's within-partition ordering guarantee. Merging messages from
partition N into partition M breaks the total order of M. This is not an implementation difficulty — it
is a semantic impossibility given Kafka's ordering contract.

### 4. Allow Deleting Arbitrary (Non-Tail) Partitions

Specify exact partition IDs to delete (e.g., delete partition 3 and 7).

**Rejected for MVP**: Creates partition ID gaps, breaks `hash(key) % count` routing, requires
fundamentally changing the partitioner contract for all producers. Can be added in a future KIP.

### 5. No Drain Period (Immediate Deletion Only)

Delete partitions immediately without a draining phase.

**Rejected as default**: Causes data loss for consumers that haven't caught up and prevents clean
transaction resolution. However, immediate deletion IS supported via `partition.drain.timeout.ms=0`.

### 6. Drain Until Consumer Lag = 0

Monitor consumer group lag and delete when all consumers have caught up.

**Rejected**: Not all consumers are registered groups (manual assign, Streams internal). Determining
"all consumers" is impossible. A stuck consumer would block deletion indefinitely. Fixed timeout is
predictable. Can be added as future enhancement.
