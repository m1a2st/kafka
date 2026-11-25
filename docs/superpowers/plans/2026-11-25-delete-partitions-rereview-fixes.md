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

# Delete Partitions Rereview Fixes Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Root owns central Gradle execution and independent reviews. Preserve existing review tests and working-tree changes.

**Goal:** Fix all four confirmed rereview findings, beginning with heartbeat liveness; complete the user's partition-deletion tool inventory and retain concrete regression evidence.

**Architecture:** Keep heartbeat sampling independent of slow replica cleanup while preserving installed-fence, metadata-epoch and transaction-safety checks. Enforce mutation quotas from DeletePartitions v0. Implement the KIP drain metrics with active-controller semantics and restore the test teardown lifecycle. Inventory tools against retained/active/deleted partition states without changing unrelated behavior.

**Tech Stack:** Java 11-compatible clients/Streams, Java 17 server, Scala 2.13, Gradle 9.4.1, JUnit/KRaft.

**Spec:** KIP.md and docs/reviews/2026-11-25-delete-partition-test-full-rereview.md. The user explicitly authorized fixing these existing behaviors after receiving the concrete report and recommendations.

## Global Constraints

- Use existing linked worktree /Users/al02475442/Desktop/java_project/kafka-delete-partition-review, branch delete-partition-test at 6b95ada57d.
- Preserve the original kafka-m1 checkout and all five dirty review test files plus the report.
- Root runs Gradle centrally, sources are frozen during builds; workers never build or commit.
- Keep all three failing repros and three passing physical-removal integration cases enabled. Do not weaken assertions to make results green.
- No generated-source edits, pushes, merges, or worktree deletion. The original fix phase also excluded commits; a subsequent user request authorizes six local commits on delete-partition-test.
- Implement and centrally verify P1 before releasing P2 production edits. Independent analysis/test preparation may proceed in parallel.

### Task 1: P1 heartbeat liveness

**Files:** core/src/main/scala/kafka/server/ReplicaManager.scala and core/src/test/scala/unit/kafka/server/ReplicaManagerTest.scala; Partition.scala or BrokerLifecycleManager only if necessary and coordinated.

**Consumes/produces:** installed metadata/fence/replica state -> fresh transaction-safety status without waiting on global cleanup I/O. Never cache stale Ready booleans or claim an unapplied metadata offset.

- [x] Inspect saved RED testReviewDrainHeartbeatSamplingDoesNotWaitForRemoteDeletion and actual monitor-owner evidence under /private/tmp/kafka-delete-partition-rereview-20261125/targeted-results/.
- [x] Use a coherent published applied-metadata view and current per-partition safety checks to remove the global I/O lock dependency.
- [x] Preserve tests for fence installation, paused/incomplete application, stale applied metadata, cancel/reopen, leaders/followers, and transactional readiness. Add meaningful concurrency controls if the implementation needs them.
- [x] Freeze sources; root runs all ReplicaManagerTest, BrokerLifecycleManagerTest and draining txn-marker cases.
- [x] Obtain independent concurrency/spec review and resolve important findings before P2 production release.

### Task 2: P2 strict quota and teardown

**Files:** core/src/main/scala/kafka/server/ControllerApis.scala and core/src/test/scala/unit/kafka/server/ControllerApisTest.scala.

**Consumes/produces:** real DeletePartitions v0 header + exhausted quota -> THROTTLING_QUOTA_EXCEEDED, without accepted mutation. Test cleanup runs exactly through the intended teardown.

- [x] Preserve RED testReviewDeletePartitionsEnforcesExhaustedRealMutationQuota, which returned NONE0 instead of THROTTLING89 with a real exhausted sensor.
- [x] After Task1 gate, change the API quota strict-version threshold from3 to0.
- [x] Move AfterEach from testDeletePartitionsRequest back to tearDown; preserve request assertions and verify fixture resources close.
- [x] Root runs full ControllerApisTest and ControllerMutationQuotaManager/Strict/Permissive quota suites, plus client quota retry regressions as needed.
- [x] Obtain independent review.

### Task 3: P2 metrics contract

**Files:** metadata controller metrics classes/publisher/tests, core ControllerServer constructor wiring if required, and KIP metric semantics only where clarification is needed.

**Consumes/produces:** committed metadata images/deltas + current controller leadership -> DrainingPartitionCount and PendingDrainRemovals gauges and DrainCompletedTotal meter matching KIP.

- [x] Preserve exact registry RED assertion for the two missing MBeans; prepare lifecycle tests before production edits.
- [x] After Task1 gate, implement the missing metrics, active-controller gating, snapshot initialization, cancellation/removal/topic deletion and leadership transitions.
- [x] Keep existing unrelated controller metrics behavior intact. Completed meter is process-local observed committed removals while active; snapshots initialize gauges rather than fabricating historical completion counts. Document precise restart/failover/coalescing semantics if necessary.
- [x] Root confirms new tests RED before implementing the extra lifecycle paths, then GREEN and full metric/image/controller regressions.
- [x] Obtain independent metrics/spec review.

### Task 4: Tool inventory

**Files:** add docs/reviews/2026-11-25-delete-partition-tools-inventory.md; test-only tool regressions if a concrete problem needs proof.

- [x] Enumerate bin entry points and delegated classes across tools, shell, Streams reset, producer/consumer/performance/transaction tools, group tools and offline metadata/log tools.
- [x] For each affected tool trace retained vsactive counts, ID-as-index/range assumptions, stale plans, explicit partitions, draining flags and deleted offsets/state.
- [x] Classify already adapted, compatible without code changes, confirmed needs update, or tests/documentation needed. Do not label a search hit as a bug without a reachable scenario.
- [x] Root centrally reproduces and fixes confirmed removed-partition tool defects with focused tests; preserve existing green suites and do not introduce broad speculative updates.
- [x] Record ownership, code references, impact and proposed regression cases in an actionable matrix.

### Task 5: Final verification/report

- [x] Run the three original failing repros and all related suites to zero unexplained failures, including three new physical-removal integration cases.
- [x] Run scoped formatting/checkstyle and tracked/untracked whitespace checks; inspect changes for scope and Java compatibility.
- [x] Independent final review; resolve important findings and rerun covering tests.
- [x] Preserve historical review evidence, add remediation status and a concise verification report with exact commands/XML/counts.
- [x] Confirm original checkout untouched, no push/commit during the original fix phase, and leave concrete changes reviewable in this worktree.

### Follow-up: authorized local commit organization

The user subsequently requested several local commits containing this round's fixes, tests, KIP, and tool inventory, with no push. Organize the 20 files into six commits: heartbeat readiness; quota and teardown; controller metrics; Streams reset validation; physical-removal integration coverage; KIP, review reports, and tool inventory. Keep each fix with its tests and the metrics API changes with their callers.

Only this plan and the remediation report's submission-status text change during organization. Source, test, and KIP bytes remain identical to the verified 3,223-case final snapshot. Verify exact file coverage, commit trailers, a clean worktree, unchanged upstream tracking ref, and the untouched original checkout. No additional Gradle execution or per-intermediate-commit build claim is part of this organization step.
