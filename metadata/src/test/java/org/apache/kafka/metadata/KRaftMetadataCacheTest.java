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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class KRaftMetadataCacheTest {
    private static final ListenerName PLAINTEXT_LISTENER = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);

    /**
     * Test 2A: Verify that partitionAgeMs is computed correctly when the creation time is in the past.
     */
    @Test
    public void testPartitionAgeMsComputedCorrectly() {
        MockTime time = new MockTime(0, 10_000L, 0);

        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1, time);

        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId));
        delta.replay(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setReplicas(List.of(0))
            .setIsr(List.of(0))
            .setLeader(0)
            .setLeaderEpoch(0)
            .setPartitionEpoch(0)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
            .setCreationTimeMs(7_000L));

        // Register broker 0 so the partition leader is available
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setBrokerEpoch(1L)
            .setIncarnationId(Uuid.randomUuid())
            .setEndPoints(new BrokerEndpointCollection(
                List.of(new BrokerEndpoint()
                    .setName(PLAINTEXT_LISTENER.value())
                    .setHost("localhost")
                    .setPort(9092)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id))
                .iterator()))
            .setFenced(false));

        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        cache.setImage(image);

        List<MetadataResponseTopic> topics = cache.getTopicMetadata(
            Set.of(topicName),
            PLAINTEXT_LISTENER,
            false,
            false
        );

        assertFalse(topics.isEmpty(), "Topic metadata should not be empty");
        MetadataResponseTopic topic = topics.get(0);
        assertEquals(topicName, topic.name());
        assertFalse(topic.partitions().isEmpty(), "Partitions should not be empty");

        MetadataResponsePartition partition = topic.partitions().get(0);
        assertEquals(3_000L, partition.partitionAgeMs(), "Partition age should be 3000ms (10000 - 7000)");
    }

    /**
     * Test 2B: Verify that partitionAgeMs is -1 when creationTimeMs is unknown (-1).
     */
    @Test
    public void testPartitionAgeMsIsNegativeOneWhenCreationTimeUnknown() {
        MockTime time = new MockTime(0, 10_000L, 0);

        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1, time);

        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId));
        delta.replay(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setReplicas(List.of(0))
            .setIsr(List.of(0))
            .setLeader(0)
            .setLeaderEpoch(0)
            .setPartitionEpoch(0)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
            .setCreationTimeMs(-1L));

        // Register broker 0 so the partition leader is available
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setBrokerEpoch(1L)
            .setIncarnationId(Uuid.randomUuid())
            .setEndPoints(new BrokerEndpointCollection(
                List.of(new BrokerEndpoint()
                    .setName(PLAINTEXT_LISTENER.value())
                    .setHost("localhost")
                    .setPort(9092)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id))
                .iterator()))
            .setFenced(false));

        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        cache.setImage(image);

        List<MetadataResponseTopic> topics = cache.getTopicMetadata(
            Set.of(topicName),
            PLAINTEXT_LISTENER,
            false,
            false
        );

        assertFalse(topics.isEmpty(), "Topic metadata should not be empty");
        MetadataResponseTopic topic = topics.get(0);
        assertEquals(topicName, topic.name());
        assertFalse(topic.partitions().isEmpty(), "Partitions should not be empty");

        MetadataResponsePartition partition = topic.partitions().get(0);
        assertEquals(-1L, partition.partitionAgeMs(), "Partition age should be -1 when creation time is unknown");
    }

    /**
     * Test 2C: Verify that partitionAgeMs is 0 when there is clock skew (creation time is in the future).
     * The Math.max(0, ...) ensures negative values are clamped to 0.
     */
    @Test
    public void testPartitionAgeMsIsZeroWhenClockSkewOccurs() {
        MockTime time = new MockTime(0, 5_000L, 0);

        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1, time);

        Uuid topicId = Uuid.randomUuid();
        String topicName = "test-topic";

        MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(topicName).setTopicId(topicId));
        delta.replay(new PartitionRecord()
            .setTopicId(topicId)
            .setPartitionId(0)
            .setReplicas(List.of(0))
            .setIsr(List.of(0))
            .setLeader(0)
            .setLeaderEpoch(0)
            .setPartitionEpoch(0)
            .setLeaderRecoveryState(LeaderRecoveryState.RECOVERED.value())
            .setCreationTimeMs(10_000L));  // Future timestamp

        // Register broker 0 so the partition leader is available
        delta.replay(new RegisterBrokerRecord()
            .setBrokerId(0)
            .setBrokerEpoch(1L)
            .setIncarnationId(Uuid.randomUuid())
            .setEndPoints(new BrokerEndpointCollection(
                List.of(new BrokerEndpoint()
                    .setName(PLAINTEXT_LISTENER.value())
                    .setHost("localhost")
                    .setPort(9092)
                    .setSecurityProtocol(SecurityProtocol.PLAINTEXT.id))
                .iterator()))
            .setFenced(false));

        MetadataImage image = delta.apply(MetadataProvenance.EMPTY);
        cache.setImage(image);

        List<MetadataResponseTopic> topics = cache.getTopicMetadata(
            Set.of(topicName),
            PLAINTEXT_LISTENER,
            false,
            false
        );

        assertFalse(topics.isEmpty(), "Topic metadata should not be empty");
        MetadataResponseTopic topic = topics.get(0);
        assertEquals(topicName, topic.name());
        assertFalse(topic.partitions().isEmpty(), "Partitions should not be empty");

        MetadataResponsePartition partition = topic.partitions().get(0);
        assertEquals(0L, partition.partitionAgeMs(), "Partition age should be 0 when creation time is in the future (clock skew)");
    }
}
