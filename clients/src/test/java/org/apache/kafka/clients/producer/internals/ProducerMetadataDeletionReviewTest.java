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

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProducerMetadataDeletionReviewTest {
    private static final String TOPIC = "metadata-deletion-review";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Node BROKER = new Node(0, "localhost", 9092);

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOlderDrainCannotEvictNewerActivePartition(boolean partialUpdate) {
        ProducerMetadata metadata = producerMetadata();
        // A reopened partition is already known at epoch 10. Another broker still has
        // the earlier draining image at epoch 9.
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 4, 10), false, 0);
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 2, 9), partialUpdate, 1);

        assertEquals(Set.of(0, 1, 2, 3), partitionIds(metadata),
            "An older leader epoch must not remove a newer active partition from producer routing");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOlderActiveMetadataCannotRestoreDrainingPartitions(boolean partialUpdate) {
        ProducerMetadata metadata = producerMetadata();
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 4, 8), false, 0);
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 2, 9), false, 1);
        assertEquals(Set.of(0, 1), partitionIds(metadata));

        // Reconnect to a lagging broker after observing the newer drain image.
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 4, 8), partialUpdate, 2);

        assertEquals(Set.of(0, 1), partitionIds(metadata),
            "Stale active metadata must not make a known draining partition writable again");
    }

    @Test
    void testDrainingMetadataErrorStillRequestsRefreshAndFencesOlderEpoch() {
        ProducerMetadata metadata = producerMetadata();
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 4, 8), false, 0);
        MetadataResponse drainingResponse = response(TOPIC_ID, 2, 9);
        drainingResponse.data().topics().find(TOPIC).partitions().get(2)
            .setErrorCode(Errors.NOT_LEADER_OR_FOLLOWER.code());

        metadata.updateWithCurrentRequestVersion(drainingResponse, false, 1);

        assertTrue(metadata.updateRequested(), "Routing exclusion must not suppress invalid-metadata retries");
        assertEquals(Optional.of(9), metadata.lastSeenLeaderEpoch(new TopicPartition(TOPIC, 2)));
        assertEquals(Set.of(0, 1), partitionIds(metadata));
    }

    @Test
    void testCancelDrainRestoresOnlyTheReopenedPrefix() {
        ProducerMetadata metadata = producerMetadata();
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 4, 3), false, 0);
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 2, 4), false, 1);
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 3, 4), true, 2);

        assertEquals(Set.of(0, 1, 2), partitionIds(metadata));
        assertTrue(metadata.partitionMetadataIfCurrent(new TopicPartition(TOPIC, 2)).isPresent());
        assertTrue(metadata.partitionMetadataIfCurrent(new TopicPartition(TOPIC, 3)).isEmpty());
    }

    @Test
    void testRecreatedTopicCanUseLowerLeaderEpochAfterDrain() {
        ProducerMetadata metadata = producerMetadata();
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 2, 20), false, 0);
        Uuid recreatedTopicId = Uuid.randomUuid();

        metadata.updateWithCurrentRequestVersion(response(recreatedTopicId, 4, 0), false, 1);

        assertEquals(recreatedTopicId, metadata.fetch().topicId(TOPIC));
        assertEquals(Set.of(0, 1, 2, 3), partitionIds(metadata));
        assertEquals(Optional.of(0), metadata.lastSeenLeaderEpoch(new TopicPartition(TOPIC, 0)));
    }

    @Test
    void testFetchLeadershipUpdatePreservesReadableDrainingState() {
        Metadata metadata = new Metadata(0, 0, 60000, new LogContext(), new ClusterResourceListeners());
        metadata.updateWithCurrentRequestVersion(response(TOPIC_ID, 2, 5), false, 0);
        TopicPartition drainingPartition = new TopicPartition(TOPIC, 3);
        Node newLeader = new Node(1, "localhost", 9093);

        assertEquals(Set.of(drainingPartition), metadata.updatePartitionLeadership(
            Map.of(drainingPartition, new Metadata.LeaderIdAndEpoch(Optional.of(1), Optional.of(6))),
            List.of(newLeader)));

        assertEquals(4, metadata.fetch().partitionCountForTopic(TOPIC));
        MetadataResponse.PartitionMetadata partition = metadata.partitionMetadataIfCurrent(drainingPartition).orElseThrow();
        assertTrue(partition.isDraining);
        assertEquals(Optional.of(1), partition.leaderId);
        assertEquals(Optional.of(6), partition.leaderEpoch);
    }

    private static ProducerMetadata producerMetadata() {
        ProducerMetadata metadata = new ProducerMetadata(0, 0, 60000, 60000,
            new LogContext(), new ClusterResourceListeners(), new MockTime());
        metadata.add(TOPIC, 0);
        return metadata;
    }

    private static Set<Integer> partitionIds(ProducerMetadata metadata) {
        return metadata.fetch().partitionsForTopic(TOPIC).stream()
            .map(partition -> partition.partition()).collect(Collectors.toSet());
    }

    private static MetadataResponse response(Uuid topicId, int activeCount, int leaderEpoch) {
        List<MetadataResponse.PartitionMetadata> partitions = IntStream.range(0, 4)
            .mapToObj(partition -> new MetadataResponse.PartitionMetadata(Errors.NONE,
                new TopicPartition(TOPIC, partition), Optional.of(0), Optional.of(leaderEpoch),
                List.of(0), List.of(0), List.of(), partition >= activeCount))
            .collect(Collectors.toList());
        return RequestTestUtils.metadataResponse(List.of(BROKER), "review-cluster", 0, List.of(
            new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC, topicId, false, partitions,
                MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED)));
    }
}
