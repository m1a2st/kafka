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
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.security.auth.SecurityProtocol;
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
     * Test 2A: Verify that creationTimeMs is passed through correctly in MetadataResponse.
     */
    @Test
    public void testCreationTimeMsPassedThroughCorrectly() {
        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);

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
        assertEquals(7_000L, partition.creationTimeMs(), "CreationTimeMs should be passed through unchanged");
    }

    /**
     * Test 2B: Verify that creationTimeMs is -1 when unknown.
     */
    @Test
    public void testCreationTimeMsIsNegativeOneWhenUnknown() {
        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);

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
        assertEquals(-1L, partition.creationTimeMs(), "CreationTimeMs should be -1 when unknown");
    }

    @Test
    public void testDescribeTopicPartitionsReturnsCreationTimeMs() {
        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);

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

        var responseData = cache.describeTopicResponse(
            List.of(topicName).iterator(),
            PLAINTEXT_LISTENER,
            t -> 0,
            100,
            false
        );

        assertFalse(responseData.topics().isEmpty());
        DescribeTopicPartitionsResponseTopic topic = responseData.topics().iterator().next();
        assertEquals(topicName, topic.name());
        assertFalse(topic.partitions().isEmpty());

        DescribeTopicPartitionsResponsePartition partition = topic.partitions().get(0);
        assertEquals(7_000L, partition.creationTimeMs());
    }

    @Test
    public void testDescribeTopicPartitionsReturnsNegativeOneWhenCreationTimeUnknown() {
        KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_1);

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

        var responseData = cache.describeTopicResponse(
            List.of(topicName).iterator(),
            PLAINTEXT_LISTENER,
            t -> 0,
            100,
            false
        );

        assertFalse(responseData.topics().isEmpty());
        DescribeTopicPartitionsResponsePartition partition = responseData.topics().iterator().next().partitions().get(0);
        assertEquals(-1L, partition.creationTimeMs());
    }
}
