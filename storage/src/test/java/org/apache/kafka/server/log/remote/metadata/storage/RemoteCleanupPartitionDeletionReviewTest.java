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

package org.apache.kafka.server.log.remote.metadata.storage;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.StopPartition;
import org.apache.kafka.server.log.remote.TopicPartitionLog;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Timeout(30)
public class RemoteCleanupPartitionDeletionReviewTest {
    @Test
    void testConcurrentSnapshotCleanupFromTwoFollowersIsIdempotent() throws Exception {
        CleanupState state = new CleanupState();
        TopicIdPartition partition = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("removed-tail", 2));
        RemoteLogSegmentId segmentId = new RemoteLogSegmentId(partition, Uuid.randomUuid());
        state.cache.addCopyInProgressSegment(new RemoteLogSegmentMetadata(segmentId, 0, 9, 0, 0, 0, 100, Map.of(0, 0L)));
        state.cache.updateRemoteLogSegmentMetadata(new RemoteLogSegmentMetadataUpdate(segmentId, 0, Optional.empty(),
            RemoteLogSegmentState.COPY_SEGMENT_FINISHED, 0));
        state.cache.markInitialized();
        state.resources.add(segmentId);

        Map<String, Object> properties = Map.of(
            RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, true,
            RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, MemoryStorageManager.class.getName(),
            RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, CacheMetadataManager.class.getName(),
            RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "state", state,
            RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX + "state", state);
        RemoteLogManagerConfig config = new RemoteLogManagerConfig(new AbstractConfig(RemoteLogManagerConfig.configDef(), properties));
        Time time = new MockTime();
        try (Metrics firstMetrics = new Metrics(time);
             Metrics secondMetrics = new Metrics(time);
             BrokerTopicStats firstStats = new BrokerTopicStats(true);
             BrokerTopicStats secondStats = new BrokerTopicStats(true);
             RemoteLogManager first = manager(config, 0, time, firstStats, firstMetrics);
             RemoteLogManager second = manager(config, 1, time, secondStats, secondMetrics)) {
            UnifiedLog log = mock(UnifiedLog.class);
            when(log.remoteLogEnabled()).thenReturn(true);
            when(log.config()).thenReturn(new LogConfig(new Properties()));
            TopicPartitionLog hostedPartition = mock(TopicPartitionLog.class);
            when(hostedPartition.topicPartition()).thenReturn(partition.topicPartition());
            when(hostedPartition.unifiedLog()).thenReturn(Optional.of(log));
            Map<String, Uuid> topicIds = Map.of(partition.topic(), partition.topicId());
            first.onLeadershipChange(Set.of(), Set.of(hostedPartition), topicIds);
            second.onLeadershipChange(Set.of(), Set.of(hostedPartition), topicIds);

            Set<StopPartition> removals = Set.of(new StopPartition(partition.topicPartition(), true, true, false));
            List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
            var executor = Executors.newFixedThreadPool(2);
            try {
                var firstCleanup = executor.submit(() -> first.stopPartitions(removals, (tp, error) -> errors.add(error)));
                var secondCleanup = executor.submit(() -> second.stopPartitions(removals, (tp, error) -> errors.add(error)));
                firstCleanup.get(15, TimeUnit.SECONDS);
                secondCleanup.get(15, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
            }

            assertEquals(2, state.deletionAttempts.get(), "Both snapshot followers must reach the idempotent storage operation");
            assertTrue(state.resources.isEmpty());
            assertFalse(state.cache.listAllRemoteLogSegments().hasNext());
            assertTrue(errors.size() <= 1, "Only the late terminal metadata update may be rejected after physical deletion");
            for (Throwable error : errors) {
                Throwable failure = error;
                while (failure.getCause() != null) {
                    failure = failure.getCause();
                }
                assertInstanceOf(RemoteResourceNotFoundException.class, failure);
            }
            assertThrows(KafkaException.class, () -> first.fetchRemoteLogSegmentMetadata(partition.topicPartition(), 0, 0));
            assertThrows(KafkaException.class, () -> second.fetchRemoteLogSegmentMetadata(partition.topicPartition(), 0, 0));

            int errorCount = errors.size();
            first.stopPartitions(removals, (tp, error) -> errors.add(error));
            second.stopPartitions(removals, (tp, error) -> errors.add(error));
            assertEquals(2, state.deletionAttempts.get());
            assertEquals(errorCount, errors.size());
        }
    }

    private static RemoteLogManager manager(RemoteLogManagerConfig config, int brokerId, Time time,
                                            BrokerTopicStats stats, Metrics metrics) throws IOException {
        return new RemoteLogManager(config, brokerId, TestUtils.tempDirectory().toString(), "cleanup-review", time,
            ignored -> Optional.empty(), (tp, offset) -> { }, stats, metrics, Optional.empty());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(10, TimeUnit.SECONDS), "Both replicas must reach the cleanup phase");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static final class CleanupState {
        private final RemoteLogMetadataCache cache = new RemoteLogMetadataCache();
        private final Set<RemoteLogSegmentId> resources = ConcurrentHashMap.newKeySet();
        private final AtomicInteger deletionAttempts = new AtomicInteger();
        private final CountDownLatch listings = new CountDownLatch(2);
        private final CountDownLatch deletions = new CountDownLatch(2);
    }

    /** Uses the real metadata cache while replacing only its transport to the metadata topic. */
    public static class CacheMetadataManager extends NoOpRemoteLogMetadataManager {
        private CleanupState state;

        @Override
        public void configure(Map<String, ?> configs) {
            state = (CleanupState) configs.get("state");
        }

        @Override
        public Iterator<RemoteLogSegmentMetadata> listRemoteLogSegments(TopicIdPartition partition) {
            List<RemoteLogSegmentMetadata> snapshot = new ArrayList<>();
            synchronized (state.cache) {
                state.cache.listAllRemoteLogSegments().forEachRemaining(snapshot::add);
            }
            state.listings.countDown();
            await(state.listings);
            return snapshot.iterator();
        }

        @Override
        public CompletableFuture<Void> updateRemoteLogSegmentMetadata(RemoteLogSegmentMetadataUpdate update) {
            synchronized (state.cache) {
                try {
                    state.cache.updateRemoteLogSegmentMetadata(update);
                    return CompletableFuture.completedFuture(null);
                } catch (RemoteResourceNotFoundException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
        }
    }

    /** Implements the storage API's idempotent delete contract without an external object store. */
    public static class MemoryStorageManager extends NoOpRemoteStorageManager {
        private CleanupState state;

        @Override
        public void configure(Map<String, ?> configs) {
            state = (CleanupState) configs.get("state");
        }

        @Override
        public void deleteLogSegmentData(RemoteLogSegmentMetadata metadata) {
            state.resources.remove(metadata.remoteLogSegmentId());
            state.deletionAttempts.incrementAndGet();
            state.deletions.countDown();
            await(state.deletions);
        }
    }
}
