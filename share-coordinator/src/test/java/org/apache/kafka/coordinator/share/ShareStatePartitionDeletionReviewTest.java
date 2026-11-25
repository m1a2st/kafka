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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.share.generated.ShareSnapshotKey;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.timeline.SnapshotRegistry;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class ShareStatePartitionDeletionReviewTest {
    private static final String GROUP = "share-state-deletion-review-group";
    private static final String TOPIC = "share-state-deletion-review-topic";
    private static final Uuid TOPIC_ID = new Uuid(17L, 19L);
    private static final SharePartitionKey REMOVED_KEY = SharePartitionKey.getInstance(GROUP, TOPIC_ID, 2);
    private static final SharePartitionKey RETAINED_KEY = SharePartitionKey.getInstance(GROUP, TOPIC_ID, 0);

    @Test
    void metadataRemovalMustReleaseShareStateAndItsLogPruningFence() {
        try (Harness context = new Harness()) {
            context.replayAt(1L, snapshot(2));
            context.replayAt(2L, snapshot(0));
            assertEquals(Optional.of(1L), context.shard.lastRedundantOffset().response());

            context.service.onMetadataUpdate(context.removalDelta, context.removedImage);
            // Subsequent work on a retained partition must no longer be held back by the removed key.
            context.replayAt(100L, snapshot(0));

            assertEquals(Optional.of(100L), context.shard.lastRedundantOffset().response(),
                "Deleted share delivery state must not pin pruning of __share_group_state");
            assertNull(context.shard.getShareStateMapValue(REMOVED_KEY));
            assertNotNull(context.shard.getShareStateMapValue(RETAINED_KEY));
        }
    }

    @Test
    void reloadedCoordinatorMustNotWriteFreshSnapshotsForPermanentlyRemovedPartitions() {
        try (Harness context = new Harness()) {
            context.replayAt(1L, snapshot(2));
            context.replayAt(2L, snapshot(0));
            context.shard.onUnloaded();
            context.shard.onLoaded(new KRaftCoordinatorMetadataImage(context.removedImage));
            context.time.sleep(10001L);

            List<CoordinatorRecord> periodicRecords = context.shard.snapshotColdPartitions().records();

            assertFalse(periodicRecords.stream().anyMatch(record -> record.key() instanceof ShareSnapshotKey key
                && key.topicId().equals(TOPIC_ID) && key.partition() == 2 && record.value() != null),
                "A periodic job must not keep refreshing state for a partition that cannot ever be recreated");
        }
    }

    private static CoordinatorRecord snapshot(int partition) {
        return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(GROUP, TOPIC_ID, partition,
            new ShareGroupOffset.Builder().setSnapshotEpoch(0).setStateEpoch(0).setLeaderEpoch(1)
                .setStartOffset(5L).setStateBatches(List.of()).setCreateTimestamp(0L).setWriteTimestamp(0L).build());
    }

    /** Runs service callbacks against a real shard while replacing only asynchronous event dispatch. */
    private static final class Harness implements AutoCloseable {
        private final MockTime time = new MockTime();
        private final ShareCoordinatorMetrics metrics = new ShareCoordinatorMetrics();
        private final ShareCoordinatorShard shard;
        private final ShareCoordinatorService service;
        private final MetadataDelta removalDelta;
        private final MetadataImage removedImage;
        private long nextOffset = 3L;

        @SuppressWarnings("unchecked")
        private Harness() {
            LogContext logContext = new LogContext();
            ShareCoordinatorConfig config = ShareCoordinatorTestConfig.testConfig();
            SnapshotRegistry registry = new SnapshotRegistry(logContext);
            shard = new ShareCoordinatorShard(logContext, config, metrics,
                metrics.newMetricsShard(registry, new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0)), registry, time,
                new MockCoordinatorTimer<>(time));
            CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime = mock(CoordinatorRuntime.class);
            doAnswer(invocation -> {
                CoordinatorMetadataDelta delta = invocation.getArgument(0);
                CoordinatorMetadataImage image = invocation.getArgument(1);
                shard.onMetadataUpdate(delta, image);
                return null;
            }).when(runtime).onMetadataUpdate(any(), any());
            doAnswer(invocation -> {
                CoordinatorRuntime.CoordinatorWriteOperation<ShareCoordinatorShard, Object, CoordinatorRecord> operation =
                    invocation.getArgument(1);
                var result = operation.generateRecordsAndResult(shard);
                result.records().forEach(record -> replayAt(nextOffset++, record));
                return List.of(CompletableFuture.completedFuture(result.response()));
            }).when(runtime).scheduleWriteAllOperation(anyString(), any());
            service = new ShareCoordinatorService(logContext, config, runtime, metrics, time,
                new MockTimer(time), mock(PartitionWriter.class));
            service.startup(() -> 1);
            MetadataImage original = new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 3).build();
            shard.onLoaded(new KRaftCoordinatorMetadataImage(original));
            removalDelta = new MetadataDelta.Builder().setImage(original).build();
            removalDelta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));
            removedImage = removalDelta.apply(MetadataProvenance.EMPTY);
        }

        private void replayAt(long offset, CoordinatorRecord record) {
            shard.replay(offset, -1L, (short) -1, record);
        }

        @Override
        public void close() {
            service.shutdown();
        }
    }
}
