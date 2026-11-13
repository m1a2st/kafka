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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DescribeClusterResponse;
import org.apache.kafka.common.requests.DescribeTopicPartitionsRequest;
import org.apache.kafka.common.requests.DescribeTopicPartitionsResponse;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaAdminClientDrainingTest {
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private static final String TOPIC = "draining-topic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Cluster CLUSTER = new Cluster("cluster", List.of(BROKER), List.of(), Set.of(), Set.of(), BROKER);

    @Test
    void testPaginatedDescribePreservesDrainingDeadline() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            prepareDescribeCluster(env);
            env.kafkaClient().prepareResponse(new DescribeTopicPartitionsResponse(
                describePage(new DescribeTopicPartitionsResponsePartition().setPartitionIndex(0))
                    .setNextCursor(new DescribeTopicPartitionsResponseData.Cursor().setTopicName(TOPIC).setPartitionIndex(1))));
            env.kafkaClient().prepareResponse(request -> {
                DescribeTopicPartitionsRequest describe = (DescribeTopicPartitionsRequest) request;
                return describe.data().cursor() != null && describe.data().cursor().partitionIndex() == 1;
            }, new DescribeTopicPartitionsResponse(describePage(
                new DescribeTopicPartitionsResponsePartition().setPartitionIndex(1).setIsDraining(true).setDrainDeadlineMs(12345L))));

            TopicDescription description = env.adminClient().describeTopics(List.of(TOPIC))
                .allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
            assertEquals(TOPIC_ID, description.topicId());
            assertEquals(2, description.partitions().size());
            assertFalse(description.partitions().get(0).isDraining());
            assertEquals(OptionalLong.empty(), description.partitions().get(0).drainDeadlineMs());
            assertTrue(description.partitions().get(1).isDraining());
            assertEquals(OptionalLong.of(12345L), description.partitions().get(1).drainDeadlineMs());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDescribeByIdPreservesMetadataDrainStateWithUnknownDeadline(boolean draining) throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            env.kafkaClient().prepareResponse(metadataResponse(draining));
            TopicDescription description = env.adminClient().describeTopics(TopicCollection.ofTopicIds(List.of(TOPIC_ID)))
                .allTopicIds().get(30, TimeUnit.SECONDS).get(TOPIC_ID);
            assertMetadataDescription(description, draining);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMetadataFallbackPreservesDrainStateWithUnknownDeadline(boolean draining) throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            env.kafkaClient().setNodeApiVersions(NodeApiVersions.create());
            prepareDescribeCluster(env);
            env.kafkaClient().prepareUnsupportedVersionResponse(request -> request instanceof DescribeTopicPartitionsRequest);
            env.kafkaClient().prepareResponse(metadataResponse(draining));
            TopicDescription description = env.adminClient().describeTopics(List.of(TOPIC))
                .allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
            assertMetadataDescription(description, draining);
        }
    }

    private static void assertMetadataDescription(TopicDescription description, boolean draining) {
        assertEquals(TOPIC_ID, description.topicId());
        assertEquals(List.of(0, 1), description.partitions().stream().map(TopicPartitionInfo::partition).collect(Collectors.toList()));
        assertFalse(description.partitions().get(0).isDraining());
        assertEquals(BROKER, description.partitions().get(0).leader());
        TopicPartitionInfo partition = description.partitions().get(1);
        assertEquals(draining, partition.isDraining());
        assertEquals(OptionalLong.empty(), partition.drainDeadlineMs());
        assertNull(partition.leader());
        assertNull(partition.elr());
        assertNull(partition.lastKnownElr());
        assertEquals(List.of(BROKER), partition.replicas());
    }

    private static DescribeTopicPartitionsResponseData describePage(DescribeTopicPartitionsResponsePartition partition) {
        partition.setLeaderId(BROKER.id()).setReplicaNodes(List.of(BROKER.id())).setIsrNodes(List.of(BROKER.id()))
            .setEligibleLeaderReplicas(List.of()).setLastKnownElr(List.of());
        DescribeTopicPartitionsResponseData data = new DescribeTopicPartitionsResponseData();
        data.topics().add(new DescribeTopicPartitionsResponseTopic().setName(TOPIC)
            .setTopicId(TOPIC_ID).setPartitions(List.of(partition)));
        return data;
    }

    private static MetadataResponse metadataResponse(boolean draining) {
        return RequestTestUtils.metadataResponse(CLUSTER.nodes(), "cluster", BROKER.id(), List.of(
            new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC, TOPIC_ID, false, List.of(
                new MetadataResponse.PartitionMetadata(Errors.NONE, new TopicPartition(TOPIC, 1), Optional.empty(),
                    Optional.of(0), List.of(0), List.of(0), List.of(), draining),
                new MetadataResponse.PartitionMetadata(Errors.NONE, new TopicPartition(TOPIC, 0), Optional.of(0),
                    Optional.of(0), List.of(0), List.of(0), List.of())), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED)));
    }

    private static void prepareDescribeCluster(AdminClientUnitTestEnv env) {
        env.kafkaClient().prepareResponse(new DescribeClusterResponse(new DescribeClusterResponseData()
            .setClusterId("cluster").setControllerId(BROKER.id()).setBrokers(new DescribeClusterResponseData.DescribeClusterBrokerCollection(
                List.of(new DescribeClusterResponseData.DescribeClusterBroker().setBrokerId(BROKER.id())
                    .setHost(BROKER.host()).setPort(BROKER.port())).iterator()))));
    }
}
