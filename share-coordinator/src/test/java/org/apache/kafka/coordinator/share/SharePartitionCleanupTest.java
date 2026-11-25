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
package org.apache.kafka.coordinator.share;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetrics;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetricsShard;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class SharePartitionCleanupTest {
    private static final String GROUP = "share-partition-cleanup";
    private static final String TOPIC = "source";
    private static final Uuid TOPIC_ID = new Uuid(107L, 109L);
    private static final SharePartitionKey REMOVED = SharePartitionKey.getInstance(GROUP, TOPIC_ID, 2);
    private static final SharePartitionKey RETAINED = SharePartitionKey.getInstance(GROUP, TOPIC_ID, 0);

    @Test
    void deletionDoesNotChangeStateOrPruningUntilItsTombstoneReplays() {
        Fixture context = new Fixture();
        context.seed();
        context.update(removedDelta());

        List<CoordinatorRecord> records = context.shard.cleanupDeletedPartitions().records();

        assertEquals(List.of(tombstone()), records);
        assertNotNull(context.shard.getShareStateMapValue(REMOVED));
        assertEquals(Optional.of(1L), context.shard.lastRedundantOffset().response());
        assertEquals(records, context.shard.cleanupDeletedPartitions().records());
        context.replay(3L, records.get(0));
        context.replay(100L, snapshot(TOPIC_ID, 0, 0L));
        assertNull(context.shard.getShareStateMapValue(REMOVED));
        assertNotNull(context.shard.getShareStateMapValue(RETAINED));
        assertEquals(Optional.of(100L), context.shard.lastRedundantOffset().response());
        assertTrue(context.shard.cleanupDeletedPartitions().records().isEmpty());
    }

    @Test
    void emptyImageDuringLoadDefersCleanupUntilMetadataArrives() {
        Fixture context = new Fixture();
        context.seed();
        context.shard.onLoaded(CoordinatorMetadataImage.EMPTY);
        assertTrue(context.timer.poll().isEmpty());
        assertNotNull(context.shard.getShareStateMapValue(REMOVED));

        context.update(removedDelta());
        context.replayTimeouts(3L);

        assertNull(context.shard.getShareStateMapValue(REMOVED));
        assertNotNull(context.shard.getShareStateMapValue(RETAINED));
    }

    @Test
    void drainingUnknownAndNeverAllocatedIdsAreNotDeletionEvidence() {
        Fixture context = new Fixture();
        context.seed();
        context.replay(3L, snapshot(TOPIC_ID, 9, 0L));
        Uuid unknownId = new Uuid(113L, 127L);
        context.replay(4L, snapshot(unknownId, 2, 0L));
        MetadataDelta draining = new MetadataDelta.Builder().setImage(original()).build();
        draining.replay(new PartitionDrainingRecord().setTopicId(TOPIC_ID).setPartitionId(2).setDeadlineMs(1000L));
        context.update(draining);
        assertTrue(context.shard.cleanupDeletedPartitions().records().isEmpty());
        context.shard.onLoaded(CoordinatorMetadataImage.EMPTY);
        assertTrue(context.shard.cleanupDeletedPartitions().records().isEmpty());
        CoordinatorMetadataImage recreated = new MetadataImageBuilder().addTopic(new Uuid(131L, 137L), TOPIC, 1)
            .buildCoordinatorMetadataImage();
        context.shard.onMetadataUpdate(recreated.emptyDelta(), recreated);

        assertTrue(context.shard.cleanupDeletedPartitions().records().isEmpty());
        assertNotNull(context.shard.getShareStateMapValue(REMOVED));
    }

    @Test
    void allColdPartitionsStillProduceCleanupTombstones() {
        Fixture context = new Fixture();
        context.replay(1L, snapshot(TOPIC_ID, 2, 1L));
        context.replay(2L, snapshot(TOPIC_ID, 0, 1L));
        context.update(removedDelta());

        assertEquals(List.of(tombstone()), context.shard.snapshotColdPartitions().records());
        assertNotNull(context.shard.getShareStateMapValue(REMOVED));
    }

    @Test
    void failedAppendRollbackRestoresTheStateAndRetryFence() {
        Fixture context = new Fixture();
        context.seed();
        context.update(removedDelta());
        context.registry.idempotentCreateSnapshot(3L);
        context.replay(3L, context.shard.cleanupDeletedPartitions().records().get(0));
        assertNull(context.shard.getShareStateMapValue(REMOVED));

        context.registry.revertToSnapshot(3L);

        assertNotNull(context.shard.getShareStateMapValue(REMOVED));
        assertEquals(Optional.of(1L), context.shard.lastRedundantOffset().response());
        assertEquals(List.of(tombstone()), context.shard.snapshotColdPartitions().records().stream()
            .filter(record -> record.value() == null).toList());
        context.replay(4L, context.shard.cleanupDeletedPartitions().records().get(0));
        assertNull(context.shard.getShareStateMapValue(REMOVED));
    }

    @Test
    void unloadingCancelsCleanupAndReloadingSchedulesItAgain() {
        Fixture context = new Fixture();
        context.seed();
        CoordinatorMetadataImage image = new KRaftCoordinatorMetadataImage(removedDelta().apply(MetadataProvenance.EMPTY));
        context.shard.onLoaded(image);
        context.shard.onUnloaded();
        assertTrue(context.timer.poll().isEmpty());
        assertNotNull(context.shard.getShareStateMapValue(REMOVED));

        context.shard.onLoaded(image);
        context.replayTimeouts(3L);

        assertNull(context.shard.getShareStateMapValue(REMOVED));
        assertNotNull(context.shard.getShareStateMapValue(RETAINED));
    }

    private static CoordinatorRecord tombstone() {
        return ShareCoordinatorRecordHelpers.newShareStateTombstoneRecord(GROUP, TOPIC_ID, 2);
    }

    private static CoordinatorRecord snapshot(Uuid topicId, int partition, long writeTimestamp) {
        return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(GROUP, topicId, partition,
            new ShareGroupOffset.Builder().setSnapshotEpoch(0).setStateEpoch(0).setLeaderEpoch(1)
                .setStartOffset(5L).setStateBatches(List.of()).setCreateTimestamp(0L).setWriteTimestamp(writeTimestamp).build());
    }

    private static MetadataImage original() {
        return new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 3).build();
    }

    private static MetadataDelta removedDelta() {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(original()).build();
        delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));
        return delta;
    }

    private static final class Fixture {
        private final MockTime time = new MockTime();
        private final SnapshotRegistry registry = new SnapshotRegistry(new LogContext());
        private final MockCoordinatorTimer<CoordinatorRecord> timer = new MockCoordinatorTimer<>(time);
        private final ShareCoordinatorShard shard = new ShareCoordinatorShard(new LogContext(), ShareCoordinatorTestConfig.testConfig(),
            mock(CoordinatorMetrics.class), mock(CoordinatorMetricsShard.class), registry, time, timer);

        private void seed() {
            replay(1L, snapshot(TOPIC_ID, 2, 0L));
            replay(2L, snapshot(TOPIC_ID, 0, 0L));
        }

        private void replay(long offset, CoordinatorRecord record) {
            registry.idempotentCreateSnapshot(offset);
            shard.replay(offset, -1L, (short) -1, record);
        }

        private void update(MetadataDelta delta) {
            shard.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
                new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY)));
        }

        private void replayTimeouts(long firstOffset) {
            long offset = firstOffset;
            for (var timeout : timer.poll()) {
                for (CoordinatorRecord record : timeout.result().records()) {
                    replay(offset++, record);
                }
            }
        }
    }
}
