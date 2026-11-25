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
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEvent;
import org.apache.kafka.coordinator.common.runtime.CoordinatorEventProcessor;
import org.apache.kafka.coordinator.common.runtime.CoordinatorLoader;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntime;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRuntimeMetrics;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.PartitionWriter;
import org.apache.kafka.coordinator.share.metrics.ShareCoordinatorMetrics;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("ClassDataAbstractionCoupling")
class ShareColdSnapshotBatchingTest {
    private static final TopicPartition STATE_PARTITION = new TopicPartition(Topic.SHARE_GROUP_STATE_TOPIC_NAME, 0);
    private static final Uuid TOPIC_ID = new Uuid(139L, 149L);
    private static final int MAX_BATCH_BYTES = 512;
    private static final int REMOVED_KEYS = 12;
    private static final String RETAINED_GROUP = "retained-" + "r".repeat(120);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void coldCleanupSplitsWritesAtThePartitionLimit(boolean includeRetainedSnapshot) throws Exception {
        MockTimer timer = new MockTimer();
        QueuedEvents events = new QueuedEvents();
        List<CoordinatorRecord> persisted = new ArrayList<>();
        List<Integer> batchSizes = new ArrayList<>();
        ShareCoordinatorRecordSerde serde = new ShareCoordinatorRecordSerde();
        PartitionWriter writer = mock(PartitionWriter.class, RETURNS_DEEP_STUBS);
        when(writer.config(STATE_PARTITION).maxMessageSize()).thenReturn(MAX_BATCH_BYTES);
        AtomicReference<PartitionWriter.Listener> listener = new AtomicReference<>();
        doAnswer(invocation -> {
            listener.set(invocation.getArgument(1));
            return null;
        }).when(writer).registerListener(eq(STATE_PARTITION), any());
        when(writer.append(eq(STATE_PARTITION), any(), any(MemoryRecords.class), anyShort())).thenAnswer(invocation -> {
            MemoryRecords batch = invocation.getArgument(2);
            if (batch.sizeInBytes() > MAX_BATCH_BYTES) {
                throw new RecordTooLargeException("Batch exceeds the configured partition limit");
            }
            batchSizes.add(batch.sizeInBytes());
            batch.records().forEach(record -> persisted.add(serde.deserialize(record.key(), record.value())));
            listener.get().onHighWatermarkUpdated(STATE_PARTITION, persisted.size());
            return (long) persisted.size();
        });
        @SuppressWarnings("unchecked")
        CoordinatorLoader<CoordinatorRecord> loader = mock(CoordinatorLoader.class);
        when(loader.load(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        MetadataImage original = new MetadataImageBuilder().addTopic(TOPIC_ID, "source", 3).build();

        try (ShareCoordinatorMetrics metrics = new ShareCoordinatorMetrics();
             CoordinatorRuntime<ShareCoordinatorShard, CoordinatorRecord> runtime =
                 new CoordinatorRuntime.Builder<ShareCoordinatorShard, CoordinatorRecord>()
                     .withTime(timer.time()).withTimer(timer).withWriteTimeout(Duration.ofSeconds(5))
                     .withEventProcessor(events).withPartitionWriter(writer).withLoader(loader)
                     .withCoordinatorShardBuilderSupplier(() -> new ShareCoordinatorShard.Builder(ShareCoordinatorTestConfig.testConfig()))
                     .withCoordinatorRuntimeMetrics(mock(CoordinatorRuntimeMetrics.class)).withCoordinatorMetrics(metrics)
                     .withSerializer(serde).withAppendLingerMs(OptionalInt.of(0))
                     .withExecutorService(Executors.newSingleThreadExecutor()).withCachedBufferMaxBytesSupplier(() -> 4096)
                     .build()) {
            runtime.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(new MetadataDelta.Builder().setImage(original).build()),
                new KRaftCoordinatorMetadataImage(original));
            runtime.scheduleLoadOperation(STATE_PARTITION, 0);
            events.drain();

            List<CoordinatorRecord> seed = new ArrayList<>();
            for (int i = 0; i < REMOVED_KEYS; i++) {
                seed.add(snapshot(removedGroup(i), 2, 1L));
            }
            seed.add(snapshot(RETAINED_GROUP, 0, includeRetainedSnapshot ? 0L : 1L));
            var seeded = runtime.scheduleWriteOperation("seed", STATE_PARTITION,
                shard -> new CoordinatorResult<Void, CoordinatorRecord>(seed, false));
            events.drain();
            seeded.get(5, TimeUnit.SECONDS);
            timer.advanceClock(10001L);
            events.drain();
            int persistedBefore = persisted.size();
            int batchesBefore = batchSizes.size();
            MetadataDelta delta = new MetadataDelta.Builder().setImage(original).build();
            delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));

            var cleaned = runtime.scheduleWriteOperation("snapshot-cold-partitions", STATE_PARTITION, shard -> {
                shard.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
                    new KRaftCoordinatorMetadataImage(delta.apply(MetadataProvenance.EMPTY)));
                return shard.snapshotColdPartitions();
            });
            events.drain();
            cleaned.get(5, TimeUnit.SECONDS);

            List<CoordinatorRecord> cleanupWrites = persisted.subList(persistedBefore, persisted.size());
            assertEquals(REMOVED_KEYS, cleanupWrites.stream().filter(record -> record.value() == null).count());
            assertEquals(includeRetainedSnapshot ? 1 : 0,
                cleanupWrites.stream().filter(record -> record.value() != null).count());
            assertTrue(batchSizes.size() - batchesBefore > 1, "The cleanup must reach storage as several bounded batches");
            assertTrue(batchSizes.stream().allMatch(size -> size <= MAX_BATCH_BYTES));

            var state = runtime.scheduleReadOperation("check-cleanup", STATE_PARTITION, (shard, offset) -> {
                long remaining = 0;
                for (int i = 0; i < REMOVED_KEYS; i++) {
                    if (shard.getShareStateMapValue(SharePartitionKey.getInstance(removedGroup(i), TOPIC_ID, 2)) != null) {
                        remaining++;
                    }
                }
                ShareGroupOffset retained = shard.getShareStateMapValue(SharePartitionKey.getInstance(RETAINED_GROUP, TOPIC_ID, 0));
                return new long[]{remaining, retained == null ? -1 : retained.snapshotEpoch()};
            });
            events.drain();
            assertArrayEquals(new long[]{0, includeRetainedSnapshot ? 1 : 0}, state.get(5, TimeUnit.SECONDS));
        }
    }

    private static String removedGroup(int index) {
        return "removed-" + index + "-" + "g".repeat(120);
    }

    private static CoordinatorRecord snapshot(String group, int partition, long writeTimestamp) {
        return ShareCoordinatorRecordHelpers.newShareSnapshotRecord(group, TOPIC_ID, partition,
            new ShareGroupOffset.Builder().setSnapshotEpoch(0).setStateEpoch(0).setLeaderEpoch(1)
                .setStartOffset(5L).setStateBatches(List.of()).setCreateTimestamp(0L).setWriteTimestamp(writeTimestamp).build());
    }

    private static final class QueuedEvents implements CoordinatorEventProcessor {
        private final Deque<CoordinatorEvent> queue = new ArrayDeque<>();

        @Override
        public void enqueueLast(CoordinatorEvent event) {
            queue.addLast(event);
        }

        @Override
        public void enqueueFirst(CoordinatorEvent event) {
            queue.addFirst(event);
        }

        private void drain() {
            CoordinatorEvent event;
            while ((event = queue.pollFirst()) != null) {
                try {
                    event.run();
                } catch (Throwable exception) {
                    event.complete(exception);
                }
            }
        }

        @Override
        public void close() {
            drain();
        }
    }
}
