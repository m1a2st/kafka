# Delete Partitions Review Remediation Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development to implement the independent scopes below. Root owns orchestration, central Gradle execution, and independent reviews. Preserve the existing worktree and its uncommitted review tests.

**Goal:** Fix all twelve findings in the 2026-11-25 review, starting with the five P1 issues, then make the related regression and integration suites pass.

**Architecture:** Keep partition deletion as a fenced ACTIVE → DRAINING → REMOVED transition. Update assignments against the current partition set, make removal terminal for replica reconciliation, stage controller mutations atomically, and preserve all retained IDs in legacy Metadata responses. After the P1 validation gate, reconcile snapshot omissions and durable share state, retain producer metadata epochs independently of writable routing, and distinguish permanent remote deletion from replica reassignment.

**Tech Stack:** Java 11-compatible clients/Streams, Java 17 server components, Scala 2.13, Gradle 9.4.1, JUnit and KRaft test clusters.

**Spec:** `docs/reviews/2026-11-25-delete-partition-test-review.md`; existing lifecycle semantics in `KIP.md`. The compatibility correction below replaces the contradictory legacy-client promises in the draft KIP.

## Global Constraints

- Work in `/Users/al02475442/Desktop/java_project/kafka-delete-partition-review`, branch `delete-partition-test`, starting at `8cf29f25c6`.
- Preserve the original checkout on `KAFKA-21501` and all existing uncommitted tests/review artifacts.
- Preserve Java 11+ for clients, generator, and streams; Java 17+ for other modules.
- Do not hand-edit generated RPC/message classes.
- Do not weaken the 79 existing review cases or disable failures.
- Root runs Gradle centrally; workers do not run concurrent builds against the shared worktree.
- Complete P1 implementation and its validation before starting P2 production changes.
- The remediation phase left concrete changes uncommitted in the worktree. In a subsequent request, the user authorized organizing these fixes and tests into six local commits on `delete-partition-test`; push, merge, and workspace deletion remain outside scope.

## Compatibility Contract

Legacy Metadata v0–v13 must include every retained readable partition. Version 14 additionally carries `IsDraining`; updated producers use that flag to avoid draining targets. Legacy producers cannot transparently reroute without upgrading because their Metadata requests do not identify whether the caller is a producer or consumer. Legacy consumer discovery during draining is preserved. Physical-removal reconciliation must also account for assignor-specific version limitations; verify classic Sticky/CooperativeSticky rather than promising support without evidence.

## Baseline Evidence

The preceding review verified 1,894 unique cases: 1,865 passed, 29 failed. The 79 new cases contain 26 reproduced failures for the twelve findings. Three older integration failures are two producer-metadata wait timeouts and one stale partition-ID reuse expectation. Saved RED evidence: `/private/tmp/kafka-delete-partition-review-20261125/{broad-final,targeted-final,RESULTS.json}`. HEAD and production sources were unchanged before this remediation.

### Task 1: P1 consumer and share assignment reconciliation

**Owner:** coordinator worker.

**Files:** `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/assignor/UniformHomogeneousAssignmentBuilder.java`, `UniformHeterogeneousAssignmentBuilder.java`, the Simple assignor builders and their tests.

**Consumes:** current partition counts from `SubscribedTopicDescriber`, old immutable assignments, and member subscriptions.

**Produces:** valid target assignments containing only current IDs, preserving assignment/revocation semantics for retained IDs.

- [x] Verify the saved RED cases for both Uniform variants and real share assignment.
- [x] Exclude no-longer-existing partitions before indexing arrays, consuming quotas, retaining owners, or redistributing revoked partitions.
- [x] Cover homogeneous/heterogeneous subscriptions and applicable Simple assignor variants.
- [x] Centrally run `ConsumerAssignmentPartitionDeletionReviewTest`, the assignment case in `SharePartitionDeletionReviewTest`, and surrounding assignor suites.
- [x] Obtain independent P1 review and address important findings.

### Task 2: P1 terminal replica removal and atomic controller mutations

**Owner:** metadata worker.

**Files:** `metadata/src/main/java/org/apache/kafka/image/TopicDelta.java`, `metadata/src/main/java/org/apache/kafka/controller/ReplicationControlManager.java`, and relevant controller/image tests.

**Consumes:** `RemovePartitionRecord`, subsequent ELR clearing, a bounded shared controller result list.

**Produces:** no leader/follower/directory work for removed IDs; a rejected topic contributes no partial drain records.

- [x] Verify saved RED ELR and oversized deletion cases, including the real broker write-after-removal failure.
- [x] Skip removed IDs during ELR reconciliation and local replica work generation.
- [x] Generate each topic's drain records separately and append through atomic bounded-list capacity checking.
- [x] Verify a rejected large topic does not undo already accepted topics or leave partial records.
- [x] Centrally run the targeted Image/Controller review methods, broker ELR cases, and related existing controller/image suites.
- [x] Obtain independent P1 review.

### Task 3: P1 legacy discovery compatibility

**Owner:** clients worker.

**Files:** `metadata/src/main/java/org/apache/kafka/metadata/KRaftMetadataCache.java`, `metadata/src/test/java/org/apache/kafka/metadata/MetadataCacheTest.java`, `clients/src/main/java/org/apache/kafka/clients/consumer/internals/AbstractStickyAssignor.java` if the additional shrink regression confirms the same assumption, corresponding client tests, `KIP.md`, and matching design-spec claims.

**Consumes:** Metadata API request version and the retained/draining metadata image.

**Produces:** full retained metadata for legacy requests, draining flags for v14, and a coherent documented producer/consumer compatibility contract.

- [x] Keep all retained IDs in v0–v13 responses; preserve v14 flags and existing pagination/description semantics.
- [x] Keep the existing legacy readability review expectations intact; update contradictory older expectations.
- [x] Verify classic Sticky/CooperativeSticky physical shrink with real assignor cases; repair current clients if required and document older-client limits.
- [x] Update KIP/design compatibility statements, including producer upgrade requirements.
- [x] Centrally run MetadataCache legacy/new version cases and applicable classic assignor suites.
- [x] Obtain independent P1 review.

### Task 4: P1 validation gate

**Owner:** root.

- [x] Run all targeted P1 repros centrally and appropriate surrounding unit suites.
- [x] Run real `PartitionDeletionReviewTest.consumerProtocolGroupReconcilesAfterPhysicalRemoval` and verify successful reconciliation.
- [x] Prepare a working-diff review package covering Tasks 1–3 and their test evidence.
- [x] Resolve important review findings before releasing P2 production work.
- [x] Record the completed P1 gate and exact results in the progress ledger.

### Task 5: P2 producer metadata and adaptive routing

**Owner:** clients worker, after Task 4.

**Files:** `clients/src/main/java/org/apache/kafka/clients/Metadata.java`, `clients/src/main/java/org/apache/kafka/clients/producer/internals/ProducerMetadata.java`, `RecordAccumulator.java`, `BuiltInPartitioner.java`, and their tests as required by the final design.

**Consumes:** full/partial responses with reliable leader epochs, topic UUID changes, draining flags, and old accumulator queues.

**Produces:** stale responses cannot evict newer active state or resurrect known drains; active queue coverage is complete before enabling adaptive statistics.

- [x] Preserve epoch/incarnation truth before filtering the producer's writable view.
- [x] Verify full/partial updates, leader hints, cancellation, and topic recreation.
- [x] Base adaptive warmup on current active IDs instead of stale queue count, preserving queued data and pending completions.
- [x] Centrally run producer metadata/routing/accumulator review cases and existing related suites.
- [x] Obtain an independent review.

### Task 6: P2 snapshot reconciliation, timeout arithmetic, and remote cleanup

**Owner:** metadata/broker worker, after Task 4.

**Files:** `metadata/src/main/java/org/apache/kafka/image/TopicsDelta.java`, `TopicDelta.java`, `ReplicationControlManager.java`, `core/src/main/scala/kafka/server/ReplicaManager.scala`, and corresponding tests.

**Consumes:** old and completed snapshot partition sets, timeout configuration, permanent partition removal versus replica reassignment.

**Produces:** snapshot omissions stop old replicas and trigger idempotent cleanup of positively removed remote IDs, deadline arithmetic cannot become cancellation, and explicit removals use their captured leader when one exists, with cleanup by any previously hosted replica when the captured leader is `NO_LEADER`.

- [x] Reconcile missing IDs for retained topic UUIDs during snapshot completion without losing snapshot drain/force/history state.
- [x] Saturate positive deadline overflow at `Long.MAX_VALUE`, preserving cancellation's negative sentinel.
- [x] Distinguish permanent removals from reassignment. Explicit removals use the latest captured leader when nonnegative; positively identified snapshot omissions and explicit leaderless force removals let previously hosted replicas perform idempotent remote cleanup. Retain valid explicit-owner, explicit-follower, and reassignment controls.
- [x] Cover coalesced metadata changes and snapshot loading in tests.
- [x] Centrally run snapshot, overflow, remote-cleanup repros and existing image/broker/controller suites.
- [x] Obtain an independent review.

### Task 7: P2 durable share reconciliation

**Owner:** coordinator worker, after Task 4.

**Files:** group coordinator initialization metadata handling and share coordinator service/shard cleanup plus related tests.

**Consumes:** removed partition IDs, current topic UUID/count/high watermark, persisted share initialization and delivery keys, shard load/update callbacks.

**Produces:** no repeated epoch bumps for a settled shrink, tombstones for removed share state, and recovery cleanup when metadata events were missed.

- [x] Reconcile initialized/pending share partition sets with current valid IDs through durable records.
- [x] Clean share delivery state for individual removals and on coordinator load, preserving UUID fencing and retained keys.
- [x] Ensure dead keys cannot keep receiving cold snapshots or unnecessarily hold log pruning.
- [x] Centrally run share review cases and existing group/share service/shard suites.
- [x] Obtain an independent review.

### Task 8: Align older integration tests and run complete related verification

**Owner:** assigned integration worker after its production scope is ready; root executes tests.

**Files:** `clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/DeletePartitionsTest.java` and related integration assertions; the current review tests remain enabled.

- [x] Bound producer metadata waits separately from delivery timeout in the rejection helper.
- [x] Give actual cluster methods explicit timeouts covering drain scheduling.
- [x] Replace obsolete successful reuse assumptions with ID-reuse rejection plus surviving-log/data and deletion checks.
- [x] Run all review cases, relevant existing suites across the affected modules, real deletion lifecycle tests, and producer/consumer integrations.
- [x] Run scoped Spotless, affected Checkstyle, and tracked/untracked whitespace checks.
- [x] Save exact JUnit XML, logs, and per-finding evidence under `/private/tmp/kafka-delete-partition-fixes-20261125/`.

### Task 9: Final review and report

**Owner:** root plus independent reviewer.

- [x] Review the complete remediation diff against all twelve findings and the updated compatibility contract.
- [x] Resolve important issues and rerun the covering tests for each amended implementation.
- [x] Update the historical review with remediation status and write a concise final verification artifact linking raw evidence.
- [x] Confirm all code changes remain reviewable in this worktree and the original checkout remains untouched.

## Central Test Commands

Targeted P1 selectors are recorded in the progress ledger as each worker finishes. Full review regression baseline:

```bash
./gradlew \
  :metadata:test --tests '*PartitionDeletionReviewTest' \
  :clients:test --tests '*DeletionReviewTest' \
  :core:test --tests 'kafka.server.ReplicaManagerTest.testDeletionReview*' \
  :group-coordinator:test --tests '*PartitionDeletionReviewTest' \
  :share-coordinator:test --tests '*PartitionDeletionReviewTest' \
  :streams:test --tests '*PartitionDeletionReviewTest' \
  :tools:test --tests '*DeletionReviewTest' \
  :clients:clients-integration-tests:test --tests org.apache.kafka.clients.admin.PartitionDeletionReviewTest \
  --continue --max-workers=4 -PmaxParallelForks=1 -Dorg.gradle.parallel=false
```

Broaden with existing assignor, metadata/image/controller, producer, group/share service/shard, Streams internal-topic, CLI, and deletion integration classes actually touched by the fixes. Verification must finish with zero unexplained failures.

## Follow-up: Local Commit Organization

The user subsequently requested several local commits on `delete-partition-test`, without pushing. Group changes with their corresponding regressions: group reconciliation; controller/image/replica cleanup; client compatibility and routing; durable share cleanup; cross-component integration/API coverage; review and verification documentation. Preserve the tested code and test bytes, add the required AI trailer, and verify the final commit tree against the validated snapshot.
