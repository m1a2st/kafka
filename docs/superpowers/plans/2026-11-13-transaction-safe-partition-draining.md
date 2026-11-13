# Transaction-safe partition draining implementation plan

> **For agentic workers:** Execute the assigned broker, controller, metadata, and client tasks with regression tests before production changes.

**Goal:** Prevent normal partition deletion while transactions or their replicated markers remain outstanding.

**Architecture:** Leaders report readiness in broker heartbeats after installing an append fence and sampling the log under the partition write lock. Controllers require a current leader report tied to the applied draining metadata, or an explicit persisted force override, before removing an expired draining partition.

**Tech Stack:** Kafka Java/Scala, generated JSON protocols, Gradle/JUnit.

**Spec:** `docs/superpowers/specs/2026-07-15-delete-partitions-design.md` (transaction protection amendment below).

## Constraints and design

- Keep all existing patch fixes and dirty work. No commit, push, or live deployment is requested.
- Clients and Streams remain Java 11 compatible; server components require Java 17.
- The drain deadline is the earliest removal time. Default expiry keeps the partition draining until safe.
- Safety requires a local leader, installed append fence, no first unstable offset, HW equal to LEO, and effective min ISR. Coordinator transaction markers retain normal `acks=-1` semantics.
- Readiness is sampled under the partition write lock, excluding concurrent appends. Normal append checks run under the partition read lock.
- Reports carry topic UUID, partition ID, leader/partition epochs, applied metadata offset, readiness and blocker. A controller validates broker epoch, session and current leadership again before removal.
- Controller replay stores the draining record offset. Cancel/re-drain and leader changes invalidate prior readiness. Controller activation/deactivation clears the soft report cache.
- `DeletePartitionsOptions.force(boolean)` and CLI `--force` explicitly bypass transaction readiness, preserving the configured drain interval. The override persists in records/snapshots and can upgrade an existing blocked drain without extending its deadline.
- Expansion before removal cancels draining and force state, preserving the original log.

## Tasks

### 1. Protocol, client and CLI

Files: `clients/.../BrokerHeartbeatRequest.json`, `DeletePartitionsRequest.json`, `DescribeTopicPartitionsResponse.json`, `DeletePartitionsOptions.java`, `KafkaAdminClient.java`, `tools/.../TopicCommand.java` and corresponding tests.

- [x] Generate optional heartbeat drain statuses and per-topic force fields using Gradle generators.
- [x] Observe failures for force option default/forwarding, protocol round trips and CLI scope validation.
- [x] Implement explicit force plumbing. Reject force for mixed increase/decrease or unrelated CLI actions before Admin mutation.
- [x] Run focused clients and tools regressions.

### 2. Durable force metadata

Files: `PartitionDrainingRecord.json`, `TopicImage.java`, `TopicDelta.java`, `TopicsDelta.java`, `TopicsImageTest.java`.

- [x] Observe snapshot/delta force propagation regressions fail.
- [x] Add immutable `TopicImage.forcedDrainingPartitions()` membership with compatible constructors.
- [x] Replay and snapshot force; cancel/remove/normal re-drain clears the corresponding override.
- [x] Run image and delta regressions.

### 3. Broker readiness and append fencing

Files: `Partition.scala`, `ReplicaManager.scala`, `BrokerServer.scala`, `BrokerLifecycleManager.java` and their tests.

- [x] Observe failures for absent readiness reports and heartbeat forwarding.
- [x] Install/clear a partition-owned append fence under its write lock before leadership can serve writes.
- [x] Implement `ReplicaManager.partitionDrainStatuses()` with metadata offsets captured only after applying replica metadata.
- [x] Verify open transactions, COMMIT/ABORT markers awaiting replication, insufficient ISR, stale epochs and delayed appends.
- [x] Wire reports through lifecycle heartbeat supplier and run broker regressions.

### 4. Controller removal gate

Files: `ReplicationControlManager.java`, `QuorumController.java`, `PartitionDrainSafetyTest.java`, `DeletePartitionsTest.java`.

- [x] Observe default expired drains are incorrectly deleted; assert no report/blocked/stale reports retain the partition.
- [x] Track drain record offsets in timeline state, force membership in timeline state, and readiness in a soft cache.
- [x] Process full broker reports, clearing omitted entries; require matching broker/leader/partition epochs, applied offset and a valid unfenced session.
- [x] Clear readiness on failover and cancellation; require fresh confirmation after leader changes.
- [x] Implement force upgrades preserving deadlines and atomic request validation.
- [x] Verify ready drains delete after deadline, default unsafe drains remain, and explicit force bypasses readiness.

### 5. Review, documentation and verification

- [x] Update KIP and design from hard deadlines to safe default removal, describe force and heartbeat fencing.
- [x] Review transaction marker locking, replay rollback, snapshot provenance, heartbeat omission, and cancellation across all components.
- [x] Run focused unit/component suites for changed behavior, then affected surrounding suites and scoped formatting/checkstyle.
- [x] Record exact observed results and report remaining test limits without claiming live-cluster coverage.

## Verification evidence

- 2180 selected tests across 44 classes: zero failures, errors or skips.
- Central RED runs confirmed missing transaction readiness, delayed append acceptance, snapshot force loss, force API/CLI absence, unsafe partial-tail removal, missing-ID force targeting and incomplete-prefix acceptance.
- Independent controller and broker reviews completed; identified sparse/tail issues were fixed and regression tested.
- Checkstyle, required SpotBugs tasks, scoped Spotless and diff whitespace checks passed.
- Evidence: `/private/tmp/kafka-transaction-safety-verification-20261113/RESULTS.md`; no live-cluster system test and no commit.
