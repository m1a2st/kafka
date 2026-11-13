# DeletePartitions review fixes implementation plan

> **For agentic workers:** Execute each independent area with test-driven development, then review and verify centrally.

**Goal:** Complete all outstanding patch review fixes, including expansion that reopens partitions before physical deletion.

**Architecture:** Persist cancellation as PartitionDrainingRecord deadline -1; brokers resume the retained log. Persist the highest allocated partition ID plus one in TopicRecord snapshots and reject creation that would reuse removed IDs. Allow excess changelog partitions while keeping repartition and copartition requirements. Reconcile old legitimate Streams ownership and converge offset cleanup from durable deletion metadata. Keep transaction marker durability unchanged.

**Tech Stack:** Java, Scala, Gradle.

**Spec:** ../specs/2026-07-15-delete-partitions-design.md

## Global constraints

- Preserve existing uncommitted fixes; do not commit or publish.
- Keep Java 11 compatibility in clients and streams.
- Edit metadata JSON, regenerate sources with Gradle, never edit generated files.
- Run Gradle centrally, use targeted module tests and formatting checks.

## Tasks

- [x] Controller/image: reproduce partial/full/beyond-current reopen, stale deadline, invalid assignment atomicity, reused ID fencing after snapshot, and delta remove/recreate failures.
- [x] Controller/image: implement cancellation, durable watermark, local replica transitions, accurate metrics and snapshots.
- [x] Streams: reproduce shrink with retained changelog in both protocols; allow excess changelog only; preserve repartition and join invariants.
- [x] Group coordinator: reproduce legitimate owned tasks after shrink; allow only ownership already recorded for that member, then revoke through reconciliation.
- [x] Offsets: reproduce missed deletion on loading and pending transactional entries; reconcile durable deleted IDs at load and metadata updates with retries.
- [x] Broker/transactions: reproduce resume with same log, restart/leadership drain propagation; test actual WriteTxnMarkers path with acks=-1 and min ISR.
- [x] Documentation: align cancellation, post-removal reuse restriction, Streams constraints, offset retries and transaction durability.
- [x] Review final diff and run targeted tests plus required code quality checks; record verified coverage and limits.

**Verification:** 1,987 selected tests across 36 classes passed (zero failures/errors/skips). Changed-module Checkstyle, Spotbugs main dependencies and Spotless checks passed. No commits created.
