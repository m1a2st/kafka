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

import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponsePartition;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DescribeTopicPartitionsResponseTest {
    private static final Node LEADER = new Node(0, "localhost", 9092);

    @Test
    void testConversionPreservesDrainingMetadata() {
        TopicPartitionInfo info = DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo(
            new DescribeTopicPartitionsResponsePartition().setPartitionIndex(1).setLeaderId(0)
                .setReplicaNodes(List.of(0)).setIsrNodes(List.of(0))
                .setEligibleLeaderReplicas(List.of()).setLastKnownElr(List.of())
                .setIsDraining(true).setDrainDeadlineMs(123456789L), Map.of(0, LEADER));
        assertTrue(info.isDraining());
        assertEquals(OptionalLong.of(123456789L), info.drainDeadlineMs());
    }

    @Test
    void testConversionDefaultsRemainActive() {
        TopicPartitionInfo info = DescribeTopicPartitionsResponse.partitionToTopicPartitionInfo(
            new DescribeTopicPartitionsResponsePartition().setPartitionIndex(1).setLeaderId(0)
                .setReplicaNodes(List.of(0)).setIsrNodes(List.of(0))
                .setEligibleLeaderReplicas(List.of()).setLastKnownElr(List.of()), Map.of(0, LEADER));
        assertFalse(info.isDraining());
        assertEquals(OptionalLong.empty(), info.drainDeadlineMs());
    }

}
