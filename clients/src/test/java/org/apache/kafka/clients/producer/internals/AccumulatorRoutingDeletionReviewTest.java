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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Record;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AccumulatorRoutingDeletionReviewTest {
    private static final String TOPIC = "accumulator-deletion-review";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Node BROKER = new Node(0, "localhost", 9092);

    @Test
    void testRemovedPartitionQueuesCannotStarveAnUnvisitedActivePartition() throws Exception {
        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        ProducerMetadata metadata = new ProducerMetadata(0, 0, 60000, 60000,
            logContext, new ClusterResourceListeners(), time);
        metadata.add(TOPIC, 0);
        metadata.updateWithCurrentRequestVersion(response(4), false, 0);
        try (Metrics metrics = new Metrics()) {
            int batchSize = 128;
            RecordAccumulator accumulator = new RecordAccumulator(logContext, batchSize, Compression.NONE,
                0, 0, 0, 60000, new RecordAccumulator.PartitionerConfig(true, 0, false, ""),
                metrics, "producer-metrics", time, null,
                new BufferPool(1024 * 1024, batchSize, metrics, time, "producer-buffer-metrics")) {
                @Override
                BuiltInPartitioner createBuiltInPartitioner(LogContext context, String topic,
                                                           int stickyBatchSize, boolean rackAware, String rack) {
                    return new BuiltInPartitioner(context, topic, stickyBatchSize, rackAware, rack) {
                        @Override
                        int randomPartition() {
                            return 1;
                        }
                    };
                }
            };
            try {
                // A keyed or explicit-partition workload has only visited IDs 0 and 2.
                // Deliver these records so that only their empty accumulator queues remain.
                for (int partition : List.of(0, 2)) {
                    accumulator.append(TOPIC, partition, 0, null, new byte[128], Record.EMPTY_HEADERS,
                        null, 0, 0, metadata.fetch());
                }
                accumulator.ready(metadata.fetchMetadataSnapshot(), 0);
                for (ProducerBatch batch : accumulator.drain(metadata.fetchMetadataSnapshot(), Set.of(BROKER),
                    Integer.MAX_VALUE, 0).get(BROKER.id())) {
                    batch.complete(0, 0);
                    accumulator.completeAndDeallocateBatch(batch);
                }

                // IDs 2 and 3 drain. ID 1 is healthy and active, but has never had a queue.
                metadata.updateWithCurrentRequestVersion(response(2), false, 1);
                assertEquals(2, metadata.fetch().availablePartitionsForTopic(TOPIC).size());
                accumulator.ready(metadata.fetchMetadataSnapshot(), 1);

                AtomicInteger selectedPartition = new AtomicInteger(-1);
                RecordAccumulator.AppendCallbacks callbacks = new RecordAccumulator.AppendCallbacks() {
                    @Override
                    public void setPartition(int partition) {
                        selectedPartition.set(partition);
                    }

                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
                    }
                };
                accumulator.append(TOPIC, RecordMetadata.UNKNOWN_PARTITION, 1, null, new byte[128],
                    Record.EMPTY_HEADERS, callbacks, 0, 1, metadata.fetch());

                // Warmup must still use both active IDs. Controlled random index 1 selects ID 1.
                // Counting the old ID-2 queue incorrectly enables adaptive stats containing only ID 0.
                assertEquals(1, selectedPartition.get(),
                    "A surviving active partition must remain eligible even if an old removed queue fills its warmup slot");
            } finally {
                accumulator.abortIncompleteBatches();
                accumulator.close();
            }
        }
    }

    private static MetadataResponse response(int activeCount) {
        return RequestTestUtils.metadataResponse(List.of(BROKER), "review-cluster", 0, List.of(
            new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC, TOPIC_ID, false,
                IntStream.range(0, 4).mapToObj(partition -> new MetadataResponse.PartitionMetadata(Errors.NONE,
                    new TopicPartition(TOPIC, partition), Optional.of(0), Optional.of(1),
                    List.of(0), List.of(0), List.of(), partition >= activeCount)).collect(Collectors.toList()),
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED)));
    }
}
