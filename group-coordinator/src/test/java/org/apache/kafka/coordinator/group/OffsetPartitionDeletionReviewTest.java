/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.OffsetMetadataManagerTest.OffsetMetadataManagerTestContext;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OffsetPartitionDeletionReviewTest {
    private static final String GROUP = "deletion-review-group";
    private static final String TOPIC = "deletion-review-topic";
    private static final Uuid TOPIC_ID = new Uuid(7L, 11L);

    @Test
    void deletingOnePartitionPreservesTheRestOfAnOpenTransaction() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(GROUP, TOPIC, 2, 20L, 0);
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        context.commitOffset(42L, GROUP, TOPIC, 0, 100L, 0, 0L);
        context.commitOffset(43L, GROUP, TOPIC, 2, 300L, 0, 0L);
        context.commitOffset(43L, GROUP, "other-topic", 2, 400L, 0, 0L);

        context.offsetMetadataManager.onPartitionsDeleted(TOPIC, Set.of(2)).forEach(context::replay);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(42L, GROUP, TOPIC, 2));
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(43L, GROUP, TOPIC, 2));
        assertTrue(context.offsetMetadataManager.hasPendingTransactionalOffsets(GROUP, TOPIC, 0));
        context.replayEndTransactionMarker(42L, TransactionResult.COMMIT);
        context.replayEndTransactionMarker(43L, TransactionResult.COMMIT);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertEquals(100L, context.offsetMetadataManager.offset(GROUP, TOPIC, 0).committedOffset);
        assertEquals(400L, context.offsetMetadataManager.offset(GROUP, "other-topic", 2).committedOffset);
        assertFalse(context.offsetMetadataManager.hasPendingTransactionalOffsets(GROUP, TOPIC, 0));
    }

    @ParameterizedTest
    @EnumSource(TransactionResult.class)
    void transactionCompletionBeforeTheSweepDoesNotHideDeletedOffsets(TransactionResult result) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(GROUP, TOPIC, 2, 20L, 0);
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        context.commitOffset(42L, GROUP, TOPIC, 0, 100L, 0, 0L);
        context.offsetMetadataManager.onMetadataUpdate(removedImage());

        context.replayEndTransactionMarker(42L, result);
        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        assertEquals(List.of(tombstone(GROUP)), records);
        records.forEach(context::replay);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertEquals(result == TransactionResult.COMMIT, context.hasOffset(GROUP, TOPIC, 0));
        assertTrue(context.offsetMetadataManager.cleanupDeletedPartitions().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(TransactionResult.class)
    void aDelayedTransactionalAppendIsRemovedBeforeItsMarkerCanResurrectIt(TransactionResult result) {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.offsetMetadataManager.onMetadataUpdate(removedImage());
        assertTrue(context.offsetMetadataManager.cleanupDeletedPartitions().isEmpty());

        // A request already validated on the broker can reach the coordinator after the deletion event.
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        context.commitOffset(42L, GROUP, TOPIC, 0, 100L, 0, 0L);
        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        assertEquals(List.of(tombstone(GROUP)), records);
        records.forEach(context::replay);
        context.replayEndTransactionMarker(42L, result);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertEquals(result == TransactionResult.COMMIT, context.hasOffset(GROUP, TOPIC, 0));
    }

    @Test
    void drainingAndReopeningKeepCommittedAndPendingOffsets() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(GROUP, TOPIC, 2, 20L, 0);
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        MetadataImage original = new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 3).build();
        MetadataDelta draining = new MetadataDelta.Builder().setImage(original).build();
        draining.replay(new PartitionDrainingRecord().setTopicId(TOPIC_ID).setPartitionId(2).setDeadlineMs(1000L));
        MetadataImage drainingImage = draining.apply(MetadataProvenance.EMPTY);
        context.offsetMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataImage(drainingImage));

        assertTrue(context.offsetMetadataManager.cleanupDeletedPartitions().isEmpty());
        MetadataDelta reopening = new MetadataDelta.Builder().setImage(drainingImage).build();
        reopening.replay(new PartitionDrainingRecord().setTopicId(TOPIC_ID).setPartitionId(2).setDeadlineMs(-1L));
        context.offsetMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataImage(reopening.apply(MetadataProvenance.EMPTY)));

        assertTrue(context.offsetMetadataManager.cleanupDeletedPartitions().isEmpty());
        assertEquals(20L, context.offsetMetadataManager.offset(GROUP, TOPIC, 2).committedOffset);
        context.replayEndTransactionMarker(42L, TransactionResult.COMMIT);
        assertEquals(200L, context.offsetMetadataManager.offset(GROUP, TOPIC, 2).committedOffset);
    }

    @Test
    void recoveryCleansCurrentAndLegacyOffsetsWithoutTouchingAnotherTopicIncarnation() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(RecordBatch.NO_PRODUCER_ID, "current", TOPIC_ID, TOPIC, 2, 20L, 0, 0L);
        context.commitOffset("legacy", TOPIC, 2, 20L, 0);
        context.commitOffset(RecordBatch.NO_PRODUCER_ID, "previous", new Uuid(8L, 12L), TOPIC, 2, 90L, 0, 0L);
        context.commitOffset(GROUP, TOPIC, 0, 10L, 0);
        context.commitOffset(GROUP, TOPIC, 9, 90L, 0);
        context.offsetMetadataManager.onMetadataUpdate(removedImage());

        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        assertEquals(Set.of(tombstone("current"), tombstone("legacy")), new HashSet<>(records));
        assertEquals(2, records.size());
        records.forEach(context::replay);

        assertFalse(context.hasOffset("current", TOPIC, 2));
        assertFalse(context.hasOffset("legacy", TOPIC, 2));
        assertEquals(90L, context.offsetMetadataManager.offset("previous", TOPIC, 2).committedOffset);
        assertTrue(context.hasOffset(GROUP, TOPIC, 0));
        assertTrue(context.hasOffset(GROUP, TOPIC, 9));
    }

    @Test
    void deletingPendingOnlyOffsetsUnblocksGroupDeletion() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        context.commitOffset(43L, GROUP, TOPIC, 2, 300L, 0, 0L);
        context.offsetMetadataManager.onMetadataUpdate(removedImage());
        assertFalse(context.groupHasNoOffsets(GROUP));

        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        assertEquals(List.of(tombstone(GROUP)), records);
        // Generating a record is insufficient: a failed append must leave the transaction protected.
        assertFalse(context.groupHasNoOffsets(GROUP));
        records.forEach(context::replay);

        assertTrue(context.groupHasNoOffsets(GROUP));
        context.replayEndTransactionMarker(42L, TransactionResult.COMMIT);
        context.replayEndTransactionMarker(43L, TransactionResult.ABORT);
        assertTrue(context.groupHasNoOffsets(GROUP));
        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
    }

    @Test
    void failedAppendRollbackRestoresPendingTransactionsForTheRetry() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(GROUP, TOPIC, 2, 20L, 0);
        context.commitOffset(42L, GROUP, TOPIC, 2, 200L, 0, 0L);
        context.commitOffset(42L, GROUP, TOPIC, 0, 100L, 0, 0L);
        context.offsetMetadataManager.onMetadataUpdate(removedImage());
        long rollbackOffset = context.lastWrittenOffset;
        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        records.forEach(context::replay);
        assertFalse(context.hasOffset(GROUP, TOPIC, 2));

        context.snapshotRegistry.revertToSnapshot(rollbackOffset);

        assertEquals(20L, context.offsetMetadataManager.offset(GROUP, TOPIC, 2).committedOffset);
        assertNotNull(context.offsetMetadataManager.pendingTransactionalOffset(42L, GROUP, TOPIC, 2));
        assertTrue(context.offsetMetadataManager.hasPendingTransactionalOffsets(GROUP, TOPIC, 2));
        assertEquals(records, context.offsetMetadataManager.cleanupDeletedPartitions());
        context.offsetMetadataManager.cleanupDeletedPartitions().forEach(context::replay);
        context.replayEndTransactionMarker(42L, TransactionResult.COMMIT);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertEquals(100L, context.offsetMetadataManager.offset(GROUP, TOPIC, 0).committedOffset);
    }

    @Test
    void aDelayedRegularCommitIsReconciledWithoutWaitingForOffsetRetention() {
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset(GROUP, TOPIC, 2, 20L, 0);
        context.offsetMetadataManager.onPartitionsDeleted(TOPIC, Set.of(2)).forEach(context::replay);
        context.offsetMetadataManager.onMetadataUpdate(removedImage());

        context.commitOffset(GROUP, TOPIC, 2, 30L, 0);
        context.commitOffset(GROUP, TOPIC, 1, 10L, 0);
        List<CoordinatorRecord> records = context.offsetMetadataManager.cleanupDeletedPartitions();
        assertEquals(List.of(tombstone(GROUP)), records);
        records.forEach(context::replay);

        assertFalse(context.hasOffset(GROUP, TOPIC, 2));
        assertEquals(10L, context.offsetMetadataManager.offset(GROUP, TOPIC, 1).committedOffset);
    }

    private static CoordinatorMetadataImage removedImage() {
        MetadataImage original = new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 3).build();
        MetadataDelta delta = new MetadataDelta.Builder().setImage(original).build();
        delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));
        return new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
    }

    private static CoordinatorRecord tombstone(String groupId) {
        return GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, TOPIC, 2);
    }
}
