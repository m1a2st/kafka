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
| Data loss               | None            | Removed partition data is deleted after the drain |

Both operations share the same key-affinity disruption that producers already handle today (via metadata refresh and
re-routing). The only additional cost of removal is that historical data in the draining partitions will eventually
be lost after removal; normal retention also continues during draining. This is an explicit, operator-controlled trade-off — not an ordering
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
2. **Removal** — Once the minimum drain interval has elapsed and a fresh leader confirmation establishes
   transaction safety, partitions are removed from metadata and their physical storage is cleaned up. An explicit
   force override bypasses transaction safety after the same interval.

### Partition State Machine

```
ACTIVE ──[DeletePartitionsRequest]──→ DRAINING ──[deadline reached and safe, or forced]──→ REMOVED
  │                                      │
  └──[DeleteTopics]──→ REMOVED           └──[DeleteTopics]──→ REMOVED
```

CreatePartitions can also transition DRAINING back to ACTIVE before physical removal.

State is derived from metadata replay:

- No `PartitionDrainingRecord` for this partition → ACTIVE
- Latest `PartitionDrainingRecord` has a non-negative deadline, no `RemovePartitionRecord` → DRAINING
- Latest `PartitionDrainingRecord` has `DeadlineMs=-1` → ACTIVE again, retaining the same log and offsets
- `RemovePartitionRecord` written → REMOVED (partition no longer exists in TopicImage)

### Metadata Records

`TopicRecord` gains the tagged int32 field `PartitionIdHighWatermark` (default 0). Replay infers it from partition creation records; snapshots preserve it explicitly when the highest IDs have been removed. This fences reuse of removed IDs after failover.

#### PartitionDrainingRecord (Record ID 29)

Written per partition when `DeletePartitionsRequest` is accepted. A non-negative `DeadlineMs` starts draining; `DeadlineMs=-1` cancels draining when `CreatePartitions` reopens a partition before removal. The cancellation is replayed and snapshotted as active state. The tagged `Force` flag defaults to `false`,
survives replay and snapshots, and is cleared by cancellation or removal.

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

#### RemovePartitionRecord (Record ID 30)

Written by the controller once the earliest removal time is reached and either transaction safety is confirmed
or an explicit force override applies. Permanently removes the partition from all metadata images.

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

### Broker Behavior

#### During DRAINING

| Mechanism | Behavior | Rationale |
|-----------|----------|-----------|
| Produce requests (data) | Rejected with `NOT_LEADER_OR_FOLLOWER` | Producers refresh metadata and reroute |
| Produce requests (txn COMMIT/ABORT markers) | Allowed with acks=all and normal min ISR | Preserve transaction marker durability |
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

Applications do not need to change their producer calls. After a metadata refresh, newly partitioned records use
only active partitions:

1. Producer sends to draining partition → `NOT_LEADER_OR_FOLLOWER`
2. `NOT_LEADER_OR_FOLLOWER` extends `RetriableException` → producer calls `metadata.requestUpdate()`
3. Metadata refresh returns:
   - **v0–v13**: Draining partitions excluded from partition list; producer sees reduced count
   - **v14**: Draining partitions included with `IsDraining=true`; `ProducerMetadata` excludes them from the producer's cached partition metadata
4. Subsequent sends without an explicit partition use `hash(key) % newActiveCount` or the sticky partitioner.

Batches already assigned to a draining partition keep their original partition during retries. They are not
repartitioned and may fail after retries or `delivery.timeout.ms` are exhausted. Explicitly targeting a draining
partition also fails. A metadata refresh only changes routing for records whose partition has not yet been selected.

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

When partitions are removed, the group coordinator writes tombstones for committed and pending transactional offsets. The durable partition ID high watermark identifies previously allocated IDs missing from the current metadata image. Shard loading and metadata updates reconcile these deleted IDs, and scheduled cleanup retries failed writes. Offsets remain until tombstones replay successfully. Retention alone is insufficient: offsets for actively subscribed groups may never expire.

### Transaction Handling

- In-flight transactions that include a draining partition: data produce is rejected, but COMMIT/ABORT markers bypass the draining write block. Markers retain `acks=all` and `min.insync.replicas`; an unavailable ISR blocks normal removal until transaction completion is safe. An explicit force request can
  override removal safety after the original deadline.
- After a metadata refresh, newly selected producer routing excludes draining partitions. Stale or explicitly addressed transaction requests can still reference them; broker data writes remain rejected while draining.
- `WriteTxnMarkers` to removed partitions: broker returns `UNKNOWN_TOPIC_OR_PARTITION`; transaction coordinator retries until metadata propagates, then drops the partition from the pending set. The metadata retry path converges; force can discard unresolved transactional data.

### Features Integration

#### Idempotent Producers

`ProducerStateManager` is per-partition. When the partition is removed and its log deleted, producer state (`.snapshot` files) are deleted as part of `LogManager.asyncDelete()`. No cross-partition impact. Physically removed IDs cannot be reused within the same topic UUID. `TopicRecord.PartitionIdHighWatermark` preserves the highest allocated ID plus one across snapshots; `CreatePartitions` returns `INVALID_PARTITIONS` if it would reuse a deleted ID. A fresh local log alone cannot fence delayed Produce requests or transaction markers. Reopening before removal retains the existing log and producer state.

#### Share Groups (KIP-932)

When a partition is removed, `ShareCoordinatorService` detects the removal via `TopicsDelta` and writes tombstone records for the partition's delivery state entries in `__share_group_state`. Unacknowledged deliveries are abandoned (data is being deleted anyway).

#### Kafka Streams

Both Streams protocols derive tasks from the remaining source partitions and revoke removed tasks. A heartbeat may report ownership that was previously assigned to that member while revocation is pending. Existing changelog topics may retain extra partitions; they must still cover every active task. Repartition topics keep exact partition counts, and co-partitioned join inputs must still match. Operators must coordinate join input changes and must not shrink application-managed internal topics.

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
| CreatePartitions | Reopens retained draining IDs up to the target, then adds new IDs if no removed ID would be reused |
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
| Offset tombstone generation | Coordinator unavailable | Stale offsets await cleanup | Reconcile durable removed IDs on load/metadata update and retry tombstone writes |

**Design principle:** Partition removal from metadata is the authoritative action. All local/remote cleanup is best-effort and eventually consistent. No cleanup failure can cause the partition to "come back."

### Partition ID Reuse

Removal proceeds from the highest retained ID downward and stops at a blocked partition, preserving a
contiguous retained prefix. Physically removed IDs cannot be allocated again under the same topic UUID.
`TopicRecord.PartitionIdHighWatermark` preserves allocation history across snapshots; `CreatePartitions` rejects
an increase that would cross a removed ID with `INVALID_PARTITIONS`. Before removal, it may reopen retained
draining IDs, keeping their original logs, producer state and offsets. New IDs can be created only when no
removed ID would be reused; a new topic UUID is required to recreate an already removed partition.

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
  "about": "True if this partition is draining and eligible for removal after its deadline and transaction safety checks. Draining partitions do not accept produce requests."
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
  "about": "The earliest removal timestamp in ms, or -1 if not draining. Transaction safety may delay removal."
}
```

`DescribeTopicPartitionsRequest` validVersions bumped from `"0"` to `"0-1"`. No new request fields.

### Broker Heartbeat Readiness

`BrokerHeartbeatRequest` adds `DrainingPartitions`, a flexible tagged array (tag 2, versions 0+, empty by default)
of `PartitionDrainStatus`. The fields are `TopicId`, `PartitionId`, `LeaderEpoch`, `PartitionEpoch`,
`MetadataOffset` (default -1), `Ready` (default false), and `BlockedReason` (default empty string). Each heartbeat
reports the draining partitions currently led by that broker. Missing, stale or blocked reports cannot authorize
normal removal. This adds no Describe response fields: `IsDraining` and `DrainDeadlineMs` remain the public
partition-status fields, while controller logs expose blockers.

### Admin Client

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

Error examples:

```
$ bin/kafka-topics.sh --alter --topic orders --delete-partitions 2
Error: A partition operation is already in progress for topic 'orders'.

$ bin/kafka-topics.sh --alter --topic orders --delete-partitions 10
Error: The requested delete partition count is invalid. Cannot delete all partitions; at least one must remain.
```

`--delete-partitions` specifies a number to remove; `--partitions` specifies the target total and can also
request a decrease. These options are mutually exclusive.

### Configuration

#### Topic Configuration

| Config | Type | Default | Valid Values | Description |
|--------|------|---------|--------------|-------------|
| `partition.drain.timeout.ms` | long | 3600000 (1 hour) | >= 0 | Minimum time after marking tail partitions as draining before removal is eligible. Consumers can read while producers cannot write. A value of 0 removes the minimum wait but still requires transaction safety unless force is explicit. Config is only read at the time `DeletePartitionsRequest` is processed; changing it after draining starts has no effect on the existing deadline. |

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
| 37 | `INVALID_PARTITIONS` | No | `CreatePartitions` | Target does not increase the active count, or would reuse a removed ID |
| 35 | `UNSUPPORTED_VERSION` | No | `DeletePartitions` | MetadataVersion < IBP_4_4_IV2 |
| 42 | `INVALID_REQUEST` | No | `DeletePartitions` | Topic is internal (`__consumer_offsets`, `__transaction_state`, etc.) |
| 89 | `THROTTLING_QUOTA_EXCEEDED` | Yes | `DeletePartitions` | Controller mutation quota exhausted |
| 136 | `PARTITION_OPERATION_IN_PROGRESS` | No | `DeletePartitions`, `AlterPartitionReassignments` | Conflicting existing drain or targeted reassignment; force may upgrade the same target |
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
| `PendingDrainRemovals` | Gauge | `kafka.controller:type=KafkaController,name=PendingDrainRemovals` | Number of draining removals still pending deadline or transaction safety. |
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
| Kafka Streams | Updated clients/coordinator | Surplus changelog partitions accepted; repartition and join input counts remain constrained. |
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
- Reopen retained draining partitions with CreatePartitions before removal. Downgrading across IBP_4_4_IV2 is rejected because metadata changed; older controllers must not discard draining state or partition ID allocation history.
- **Recommendation:** Test with non-critical topics first.

### Release Phases

| Phase | MetadataVersion | Stability | Enablement |
|-------|----------------|-----------|------------|
| Early Access | IBP_4_4_IV2 (unstable) | `unstable.feature.versions.enable=true` required | Explicit opt-in. Not for production. |
| General Availability | Future IBP (stable) | Stable, no flags needed | Enabled by default at target MetadataVersion. |

## Test Plan

### Unit Tests

- All controller validation paths (invalid count, internal topic, unauthorized, reassignment conflict, duplicate drain)
- State transitions with expired deadlines blocked by ongoing or unreplicated transaction completion
- Fresh current-leader reports covering applied drain metadata; stale epochs, fencing, omitted/blocked reports and failover invalidate readiness
- Force before/after deadline, same-target upgrade preserving deadlines, and snapshot/replay retention or clearing
- Admin and CLI force validation for all matched topics before mutation
- Tail removal stops at a blocked highest ID; safe lower IDs remain retained while other topics progress
- Force upgrades after partial removal touch only actual retained draining IDs and preserve deadlines
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
- Expansion during drain: partial/full reopen retains logs; expansion beyond retained count creates only new IDs
- Blocking: AlterPartitionReassignments blocked for draining partitions
- Topic deletion during drain: supersedes and cancels drain
- Controller failover during drain: new controller resumes countdown
- Controller failover after deadline passed: normal drain waits for fresh safety confirmation; force persists
- Broker restart during/after drain: correct state restored
- Multiple replicas (RF=3): all brokers clean up log dirs on removal
- Partition reuse fencing: delete → CreatePartitions rejects reused IDs, including after snapshot restore
- drain timeout = 0: no minimum wait, transaction safety still required
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
- All replicas offline → normal drain remains blocked; force permits removal after deadline
- Follower crash + recovery before/after deadline
- Controlled shutdown of leader during drain
- JBOD disk failure during drain
- In-flight acks=all produce when drain starts (already-appended succeeds)
- Actual WriteTxnMarkers path retains acks=all; min ISR failure retries until recovery; normal removal waits, explicit force may override removal safety
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
transaction resolution. However, zero minimum wait is supported via `partition.drain.timeout.ms=0`; bypassing transaction safety additionally requires explicit force.

### 6. Drain Until Consumer Lag = 0

Monitor consumer group lag and delete when all consumers have caught up.

**Rejected**: Not all consumers are registered groups (manual assign, Streams internal). Determining
"all consumers" is impossible. A stuck consumer would block deletion indefinitely. A fixed minimum interval with explicit force is
predictable. Can be added as future enhancement.
