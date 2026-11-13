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
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.OffsetMetadataManagerTest.OffsetMetadataManagerTestContext;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class OffsetMetadataManagerPartitionDeletionTest {
    @Test
    public void testLoadedShardCleansOffsetsForMissedPartitionRemoval() {
        Uuid topicId = Uuid.randomUuid();
        MetadataImage originalImage = new MetadataImageBuilder().addTopic(topicId, "foo", 3).build();
        MetadataDelta delta = new MetadataDelta.Builder().setImage(originalImage).build();
        delta.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        CoordinatorMetadataImage image = new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY));
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset("group", "foo", 0, 10L, 0);
        context.commitOffset("group", "foo", 2, 20L, 0);
        context.commitOffset("group", "foo", 7, 70L, 0);
        context.commitOffset("group", "absent-topic", 2, 30L, 0);
        context.commitOffset(42L, "group", "foo", 2, 200L, 0, context.time.milliseconds());
        context.commitOffset(43L, "pending-group", "foo", 2, 300L, 0, context.time.milliseconds());
        context.groupMetadataManager.getOrMaybeCreateClassicGroup("group", true)
            .setSubscribedTopics(Optional.of(Set.of("foo")));
        GroupCoordinatorShard shard = new GroupCoordinatorShard(
            new LogContext(), context.groupMetadataManager, context.offsetMetadataManager, context.time, context.timer,
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, 24),
            mock(GroupCoordinatorMetrics.class), context.metrics);

        // No deletion delta was delivered to this shard while it was loading.
        shard.onLoaded(image);
        List<CoordinatorRecord> records = context.sleep(0).stream()
            .flatMap(timeout -> timeout.result().records().stream()).toList();
        assertEquals(Set.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group", "foo", 2),
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("pending-group", "foo", 2)
        ), new HashSet<>(records));
        assertEquals(2, records.size());
        assertFalse(context.hasOffset("group", "foo", 2));
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(42L, "group", "foo", 2));
        assertNull(context.offsetMetadataManager.pendingTransactionalOffset(43L, "pending-group", "foo", 2));
        context.replayEndTransactionMarker(42L, TransactionResult.COMMIT);
        context.replayEndTransactionMarker(43L, TransactionResult.COMMIT);
        assertFalse(context.hasOffset("group", "foo", 2));
        assertFalse(context.hasOffset("pending-group", "foo", 2));
        assertTrue(context.hasOffset("group", "foo", 0));
        assertTrue(context.hasOffset("group", "foo", 7));
        assertTrue(context.hasOffset("group", "absent-topic", 2));
    }

    @Test
    public void testPartitionRemovalCleanupRetriesWithoutRemovingOffsetsBeforeReplay() {
        Uuid topicId = Uuid.randomUuid();
        MetadataImage originalImage = new MetadataImageBuilder().addTopic(topicId, "foo", 3).build();
        MetadataDelta delta = new MetadataDelta.Builder().setImage(originalImage).build();
        delta.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        OffsetMetadataManagerTestContext context = new OffsetMetadataManagerTestContext.Builder().build();
        context.commitOffset("group", "foo", 0, 10L, 0);
        context.commitOffset("group", "foo", 2, 20L, 0);
        context.groupMetadataManager.getOrMaybeCreateClassicGroup("group", true)
            .setSubscribedTopics(Optional.of(Set.of("foo")));
        GroupCoordinatorShard shard = new GroupCoordinatorShard(
            new LogContext(), context.groupMetadataManager, context.offsetMetadataManager, context.time, context.timer,
            GroupCoordinatorConfigTest.createGroupCoordinatorConfig(4096, 60000L, 24),
            mock(GroupCoordinatorMetrics.class), context.metrics);
        shard.onLoaded(new KRaftCoordinatorMetadataImage(originalImage));
        context.sleep(0);
        shard.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
            new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY)));

        List<CoordinatorRecord> expected = List.of(
            GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord("group", "foo", 2));
        // A failed append does not replay its tombstones. The next sweep must generate them again.
        assertEquals(expected, shard.cleanupGroupMetadata().records());
        assertTrue(context.hasOffset("group", "foo", 2));
        assertEquals(expected, shard.cleanupGroupMetadata().records());
        expected.forEach(context::replay);
        assertEquals(List.of(), shard.cleanupGroupMetadata().records());
        assertTrue(context.hasOffset("group", "foo", 0));
    }
}
