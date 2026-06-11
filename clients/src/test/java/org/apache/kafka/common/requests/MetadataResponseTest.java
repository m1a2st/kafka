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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;

import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class MetadataResponseTest {

    @Test
    void buildClusterTest() {
        Uuid zeroUuid = new Uuid(0L, 0L);
        Uuid randomUuid = Uuid.randomUuid();
        MetadataResponseData.MetadataResponseTopic topicMetadata1 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic1")
                .setErrorCode(Errors.NONE.code())
                .setPartitions(emptyList())
                .setIsInternal(false);
        MetadataResponseData.MetadataResponseTopic topicMetadata2 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic2")
                .setErrorCode(Errors.NONE.code())
                .setTopicId(zeroUuid)
                .setPartitions(emptyList())
                .setIsInternal(false);
        MetadataResponseData.MetadataResponseTopic topicMetadata3 = new MetadataResponseData.MetadataResponseTopic()
                .setName("topic3")
                .setErrorCode(Errors.NONE.code())
                .setTopicId(randomUuid)
                .setPartitions(emptyList())
                .setIsInternal(false);

        MetadataResponseData.MetadataResponseTopicCollection topics =
                new MetadataResponseData.MetadataResponseTopicCollection();
        topics.add(topicMetadata1);
        topics.add(topicMetadata2);
        topics.add(topicMetadata3);
        MetadataResponse metadataResponse = new MetadataResponse(new MetadataResponseData().setTopics(topics),
                ApiKeys.METADATA.latestVersion());
        Cluster cluster = metadataResponse.buildCluster();
        assertNull(cluster.topicName(Uuid.ZERO_UUID));
        assertNull(cluster.topicName(zeroUuid));
        assertEquals("topic3", cluster.topicName(randomUuid));
    }

    @Test
    void testPartitionAgeMsOmittedInVersion13() {
        // Build a MetadataResponseData with PartitionAgeMs set
        MetadataResponseData data = new MetadataResponseData();
        MetadataResponseTopic topic = new MetadataResponseTopic()
            .setName("test")
            .setTopicId(Uuid.randomUuid())
            .setErrorCode(Errors.NONE.code())
            .setIsInternal(false);
        topic.partitions().add(new MetadataResponsePartition()
            .setPartitionIndex(0)
            .setLeaderId(0)
            .setLeaderEpoch(1)
            .setReplicaNodes(List.of(0))
            .setIsrNodes(List.of(0))
            .setOfflineReplicas(List.of())
            .setPartitionAgeMs(5000L));
        data.topics().add(topic);

        // Serialize at v13 (pre-KIP-1327) and deserialize — PartitionAgeMs should be default (-1)
        short v13 = 13;
        MetadataResponseData deserializedV13 = new MetadataResponseData(
            MessageUtil.toByteBufferAccessor(data, v13), v13);
        assertEquals(-1L, deserializedV13.topics().iterator().next().partitions().get(0).partitionAgeMs(),
            "PartitionAgeMs should be -1 (default) when serialized at version 13");

        // Serialize at v14 (KIP-1327) and deserialize — PartitionAgeMs should be preserved
        short v14 = 14;
        MetadataResponseData deserializedV14 = new MetadataResponseData(
            MessageUtil.toByteBufferAccessor(data, v14), v14);
        assertEquals(5000L, deserializedV14.topics().iterator().next().partitions().get(0).partitionAgeMs(),
            "PartitionAgeMs should be preserved when serialized at version 14");
    }

    @Test
    void testCreationTimeMsOmittedInDescribeTopicPartitionsResponseVersion0() {
        // Build a DescribeTopicPartitionsResponseData with CreationTimeMs set
        DescribeTopicPartitionsResponseData data = new DescribeTopicPartitionsResponseData();
        DescribeTopicPartitionsResponseTopic topic = new DescribeTopicPartitionsResponseTopic()
            .setName("test")
            .setTopicId(Uuid.randomUuid())
            .setErrorCode(Errors.NONE.code())
            .setIsInternal(false);
        topic.partitions().add(new DescribeTopicPartitionsResponsePartition()
            .setPartitionIndex(0)
            .setLeaderId(0)
            .setLeaderEpoch(1)
            .setReplicaNodes(List.of(0))
            .setIsrNodes(List.of(0))
            .setOfflineReplicas(List.of())
            .setCreationTimeMs(7000L));
        data.topics().add(topic);

        // Serialize at v0 (pre-KIP-1327) and deserialize — CreationTimeMs should be default (-1)
        short v0 = 0;
        DescribeTopicPartitionsResponseData deserializedV0 = new DescribeTopicPartitionsResponseData(
            MessageUtil.toByteBufferAccessor(data, v0), v0);
        assertEquals(-1L, deserializedV0.topics().iterator().next().partitions().get(0).creationTimeMs(),
            "CreationTimeMs should be -1 (default) when serialized at version 0");

        // Serialize at v1 (KIP-1327) and deserialize — CreationTimeMs should be preserved
        short v1 = 1;
        DescribeTopicPartitionsResponseData deserializedV1 = new DescribeTopicPartitionsResponseData(
            MessageUtil.toByteBufferAccessor(data, v1), v1);
        assertEquals(7000L, deserializedV1.topics().iterator().next().partitions().get(0).creationTimeMs(),
            "CreationTimeMs should be preserved when serialized at version 1");
    }
}
