# DeletePartitions Admin and tools follow-up

**Goal:** Expose existing draining state through Admin descriptions and make CLI operations use the correct active/retained counts; fix concrete deletion request validation and quota retry gaps.

**Design:** Add backward-compatible TopicPartitionInfo draining/deadline accessors and preserve wire metadata through all Admin describe paths. CLI target counts refer to active partitions; reopening sends CreatePartitions and explicit force upgrades send DeletePartitions. Existing protocol fields suffice. Quota retries follow CreatePartitions conventions and never resubmit successful topics.

## Constraints

- Preserve the nine existing commits and their contents; new fixes remain reviewable separately.
- Java 11 compatibility in clients; no generated source edits.
- A deadline is the earliest removal time, not a promised completion time.
- Unknown deadlines remain optional. Old constructors and protocol versions default to active/unknown.
- Relative force requests with an existing drain are ambiguous after partial removal; require an absolute target before mutating anything.
- Preserve ordinary non-draining CLI behavior and replica-assignment conventions.

## Work

- [x] Reproduce dropped Admin draining/deadline metadata and verify old constructor compatibility.
- [x] Expose TopicPartitionInfo.isDraining() and OptionalLong drainDeadlineMs(), including equality and descriptions.
- [x] Preserve state through DescribeTopicPartitions and Metadata fallback/by-ID paths; add meaningful request-path tests.
- [x] Reproduce CLI reopening/count/force/describe/manual-assignment gaps; fix using active and retained counts.
- [x] Reject null/empty Admin topic names per future without harming valid requests.
- [x] Preserve quota throttle duration; retry only throttled topics under the original deadline, with an explicit opt-out used by CLI.
- [x] Update public API/KIP/design documentation and independently review the combined changes.
- [x] Reproduce and fix reassignment verification of stale plans after partition deletion, using actual partition IDs.
- [x] Run focused regressions and affected formatting/checkstyle checks; record exact results and limits.

## Review and scope

Independent review found and verified the fix for excess reopening assignments. A subsequent review found
no outstanding correctness blockers in the Admin metadata/options, CLI planning, public API documentation,
or reassignment lookup changes. Other offset/group/producer performance tools enumerate actual IDs or inherit
the existing producer handling; no additional required change was found there.

Public API documentation also clarifies CreatePartitions/NewPartitions active-count targets, preserved logs
and assignments on reopening, the physically-new assignment count, and the prohibition on removed-ID reuse.

## Final verification

- 310 clients unit/component cases, 103 tools unit cases, and 11 selected local-broker integration cases passed.
- 37 new regression cases were added; all 424 selected cases passed without failures, errors, or skips.
- Clients/tools Spotless and Checkstyle, main SpotBugs, clients Javadoc, and final diff checks passed.
- The full repository/system-test suites were not run. Broker integration cases cover existing CLI flows;
  the new draining decision matrix is covered by focused unit/component regressions.
- Exact logs and copied JUnit XML: `/private/tmp/kafka-admin-tools-verification-20261113/RESULTS.md`.
- Verification baseline: `6e08a3bdeb`; the nine earlier commits remain intact. The follow-up is split into
  Admin descriptions, Admin deletion protection, CLI behavior, reassignment verification, and documentation commits.
