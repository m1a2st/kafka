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
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class ShareInitializationPartitionCleanupTest {
    private static final String GROUP = "share-initialization-cleanup";
    private static final String TOPIC = "source";
    private static final String OTHER = "other-source";
    private static final Uuid TOPIC_ID = new Uuid(71L, 73L);
    private static final Uuid OTHER_ID = new Uuid(79L, 83L);

    @Test
    void removalIsPersistedForBothInitializationSetsOnlyAfterReplay() {
        GroupMetadataManagerTestContext context = context();
        update(context, removedDelta());

        List<CoordinatorRecord> records = context.groupMetadataManager.cleanupDeletedShareGroupPartitions();

        assertEquals(List.of(stateRecord(Set.of(1), Set.of(0))), records);
        assertEquals(Map.of(TOPIC_ID, Set.of(0, 2)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
        assertEquals(records, context.groupMetadataManager.cleanupDeletedShareGroupPartitions());
        records.forEach(context::replay);
        assertEquals(Map.of(TOPIC_ID, Set.of(0)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
        var state = context.groupMetadataManager.shareGroupStatePartitionMetadata().get(GROUP);
        assertEquals(Set.of(1), state.initializingTopics().get(TOPIC_ID).partitions());
        assertEquals(Set.of(OTHER_ID), state.deletingTopics());
        assertTrue(context.groupMetadataManager.cleanupDeletedShareGroupPartitions().isEmpty());
    }

    @Test
    void rollbackLeavesInitializationCleanupEligibleForRetry() {
        GroupMetadataManagerTestContext context = context();
        update(context, removedDelta());
        List<CoordinatorRecord> records = context.groupMetadataManager.cleanupDeletedShareGroupPartitions();
        long rollbackOffset = context.lastWrittenOffset;
        records.forEach(context::replay);

        context.snapshotRegistry.revertToSnapshot(rollbackOffset);

        assertEquals(Map.of(TOPIC_ID, Set.of(0, 2)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
        assertEquals(records, context.groupMetadataManager.cleanupDeletedShareGroupPartitions());
        records.forEach(context::replay);
        assertEquals(Map.of(TOPIC_ID, Set.of(0)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void lateInitializationSuccessCannotRestoreRemovedIdsOrAnotherTopicIncarnation(boolean cleanupFirst) {
        GroupMetadataManagerTestContext context = context();
        update(context, removedDelta());
        if (cleanupFirst) {
            context.groupMetadataManager.cleanupDeletedShareGroupPartitions().forEach(context::replay);
        }

        context.groupMetadataManager.initializeShareGroupState(GROUP,
            Map.of(TOPIC_ID, Set.of(1, 2), new Uuid(89L, 97L), Set.of(0)))
            .records().forEach(context::replay);

        assertEquals(Map.of(TOPIC_ID, Set.of(0, 1)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
        assertTrue(context.groupMetadataManager.shareGroupStatePartitionMetadata().get(GROUP).initializingTopics().isEmpty());
        assertTrue(context.groupMetadataManager.initializeShareGroupState(GROUP, Map.of(TOPIC_ID, Set.of(2)))
            .records().isEmpty());
    }

    @Test
    void emptyMetadataAndAnUnknownUuidAreNotDeletionEvidence() {
        GroupMetadataManagerTestContext context = context();
        context.groupMetadataManager.onMetadataUpdate(CoordinatorMetadataImage.EMPTY.emptyDelta(), CoordinatorMetadataImage.EMPTY);
        assertTrue(context.groupMetadataManager.cleanupDeletedShareGroupPartitions().isEmpty());
        assertTrue(context.groupMetadataManager.initializeShareGroupState(GROUP, Map.of(TOPIC_ID, Set.of(1))).records().isEmpty());
        CoordinatorMetadataImage recreated = new MetadataImageBuilder().addTopic(new Uuid(101L, 103L), TOPIC, 1)
            .buildCoordinatorMetadataImage();
        context.groupMetadataManager.onMetadataUpdate(recreated.emptyDelta(), recreated);

        assertTrue(context.groupMetadataManager.cleanupDeletedShareGroupPartitions().isEmpty());
        assertEquals(Map.of(TOPIC_ID, Set.of(0, 2)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
    }

    @Test
    void retainedDrainingAndReopenedIdsCanStillCompleteInitialization() {
        GroupMetadataManagerTestContext context = context();
        MetadataDelta draining = new MetadataDelta.Builder().setImage(original()).build();
        draining.replay(new PartitionDrainingRecord().setTopicId(TOPIC_ID).setPartitionId(2).setDeadlineMs(1000L));
        update(context, draining);
        assertTrue(context.groupMetadataManager.cleanupDeletedShareGroupPartitions().isEmpty());
        context.groupMetadataManager.initializeShareGroupState(GROUP, Map.of(TOPIC_ID, Set.of(2)))
            .records().forEach(context::replay);
        MetadataDelta reopened = new MetadataDelta.Builder().setImage(draining.apply(MetadataProvenance.EMPTY)).build();
        reopened.replay(new PartitionDrainingRecord().setTopicId(TOPIC_ID).setPartitionId(2).setDeadlineMs(-1L));
        update(context, reopened);

        assertTrue(context.groupMetadataManager.cleanupDeletedShareGroupPartitions().isEmpty());
        assertEquals(Map.of(TOPIC_ID, Set.of(0, 2)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
    }

    @Test
    void aLoadWithEmptyMetadataReconcilesAfterTheFirstUsableImage() {
        GroupMetadataManagerTestContext context = context();
        GroupCoordinatorShard shard = new GroupCoordinatorShard(new LogContext(), context.groupMetadataManager,
            mock(OffsetMetadataManager.class), context.time, context.timer,
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, 24),
            mock(GroupCoordinatorMetrics.class), context.metrics);
        shard.onLoaded(CoordinatorMetadataImage.EMPTY);
        context.sleep(0L);
        assertEquals(Map.of(TOPIC_ID, Set.of(0, 2)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
        MetadataDelta delta = removedDelta();

        shard.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
            new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY)));
        context.sleep(0L);

        assertEquals(Map.of(TOPIC_ID, Set.of(0)), context.groupMetadataManager.initializedShareGroupPartitions(GROUP));
    }

    private static GroupMetadataManagerTestContext context() {
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(new KRaftCoordinatorMetadataImage(original()))
            .withShareGroup(new ShareGroupBuilder(GROUP, 10)).build();
        context.replay(stateRecord(Set.of(1, 2), Set.of(0, 2)));
        return context;
    }

    private static CoordinatorRecord stateRecord(Set<Integer> initializing, Set<Integer> initialized) {
        return GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(GROUP,
            Map.of(TOPIC_ID, new InitMapValue(TOPIC, initializing, 0L)),
            Map.of(TOPIC_ID, new InitMapValue(TOPIC, initialized, 0L)), Map.of(OTHER_ID, OTHER));
    }

    private static MetadataImage original() {
        return new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 3).addTopic(OTHER_ID, OTHER, 1).build();
    }

    private static MetadataDelta removedDelta() {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(original()).build();
        delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));
        return delta;
    }

    private static void update(GroupMetadataManagerTestContext context, MetadataDelta delta) {
        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
            new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY)));
    }
}
