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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Subscription;
import org.apache.kafka.clients.consumer.internals.AbstractStickyAssignor;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyConsumerDeletionReviewTest {
    private static final String TOPIC = "legacy-consumer-deletion-review";
    private static final String OTHER_TOPIC = "other-topic";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Node BROKER = new Node(0, "localhost", 9092);

    @ParameterizedTest
    @ValueSource(shorts = {0, 9, 13})
    void testLegacyMetadataKeepsRangeAssignmentUntilPhysicalRemoval(short version) {
        Metadata metadata = new Metadata(0, 0, 60000, new LogContext(), new ClusterResourceListeners());
        RangeAssignor assignor = new RangeAssignor();
        GroupSubscription subscription = new GroupSubscription(Map.of("member", new Subscription(List.of(TOPIC))));
        metadata.updateWithCurrentRequestVersion(legacyResponse(version, 4), false, 0);

        assertEquals(Set.of(partition(0), partition(1), partition(2), partition(3)),
            new HashSet<>(assignor.assign(metadata.fetch(), subscription).groupAssignment().get("member").partitions()));

        metadata.updateWithCurrentRequestVersion(legacyResponse(version, 2), false, 1);

        assertEquals(Set.of(partition(0), partition(1)),
            new HashSet<>(assignor.assign(metadata.fetch(), subscription).groupAssignment().get("member").partitions()));
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    void testClassicStickyAssignmentRemovesDeletedTail(boolean cooperative, boolean heterogeneous) {
        AbstractStickyAssignor assignor = stickyAssignor(cooperative);
        List<String> secondTopics = heterogeneous ? List.of(TOPIC, OTHER_TOPIC) : List.of(TOPIC);
        List<TopicPartition> secondOwnership = heterogeneous ?
            List.of(partition(0), partition(1), new TopicPartition(OTHER_TOPIC, 0)) : List.of(partition(0), partition(1));
        Map<String, Subscription> subscriptions = Map.of(
            "first", subscription(cooperative, "first", List.of(TOPIC), List.of(partition(2), partition(3)), 1),
            "second", subscription(cooperative, "second", secondTopics, secondOwnership, 1));
        Map<String, Integer> partitionCounts = heterogeneous ? Map.of(TOPIC, 2, OTHER_TOPIC, 1) : Map.of(TOPIC, 2);
        Set<TopicPartition> expected = new HashSet<>(List.of(partition(0), partition(1)));
        if (heterogeneous)
            expected.add(new TopicPartition(OTHER_TOPIC, 0));

        Map<String, List<TopicPartition>> firstAssignment = assignor.assign(partitionCounts, subscriptions);

        assertTrue(firstAssignment.values().stream().flatMap(List::stream).allMatch(expected::contains),
            "Removed tail IDs must be revoked before sticky assignment considers previous ownership");
        // Cooperative assignment may first revoke an active ID from its old owner. A second
        // generation must finish that transfer while continuing to exclude removed IDs.
        Map<String, Subscription> reconciledSubscriptions = new HashMap<>();
        reconciledSubscriptions.put("first", subscription(cooperative, "first", List.of(TOPIC), firstAssignment.get("first"), 2));
        reconciledSubscriptions.put("second", subscription(cooperative, "second", secondTopics, firstAssignment.get("second"), 2));
        Map<String, List<TopicPartition>> settledAssignment = assignor.assign(partitionCounts, reconciledSubscriptions);
        List<TopicPartition> assigned = settledAssignment.values().stream().flatMap(List::stream).collect(Collectors.toList());

        assertEquals(expected, new HashSet<>(assigned));
        assertEquals(expected.size(), assigned.size(), "Every active partition must be assigned exactly once");
    }

    @SuppressWarnings("removal") // Both constructors are deprecated; the fixture needs a previous generation.
    private static Subscription subscription(boolean cooperative, String member, List<String> topics,
                                             List<TopicPartition> ownedPartitions, int generation) {
        AbstractStickyAssignor previousMember = stickyAssignor(cooperative);
        previousMember.onAssignment(new Assignment(ownedPartitions),
            new ConsumerGroupMetadata("deletion-review-group", generation, member, Optional.empty()));
        return new Subscription(topics, previousMember.subscriptionUserData(new HashSet<>(topics)),
            ownedPartitions, generation, Optional.empty());
    }

    private static AbstractStickyAssignor stickyAssignor(boolean cooperative) {
        return cooperative ? new CooperativeStickyAssignor() : new StickyAssignor();
    }

    private static TopicPartition partition(int partition) {
        return new TopicPartition(TOPIC, partition);
    }

    private static MetadataResponse legacyResponse(short version, int retainedCount) {
        MetadataResponse response = RequestTestUtils.metadataResponse(List.of(BROKER), "review-cluster", 0,
            List.of(new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC, TOPIC_ID, false,
                IntStream.range(0, retainedCount).mapToObj(partition -> new MetadataResponse.PartitionMetadata(Errors.NONE,
                    partition(partition), Optional.of(0), Optional.of(1), List.of(0), List.of(0), List.of(), partition >= 2))
                    .collect(Collectors.toList()), MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED)), version);
        return MetadataResponse.parse(MessageUtil.toByteBufferAccessor(response.data(), version), version);
    }
}
