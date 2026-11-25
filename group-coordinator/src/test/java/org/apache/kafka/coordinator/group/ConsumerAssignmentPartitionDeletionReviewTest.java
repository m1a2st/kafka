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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.assignor.UniformAssignor;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupBuilder;
import org.apache.kafka.coordinator.group.modern.consumer.ConsumerGroupMember;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ADDRESS;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_CLIENT_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.Utils.toAssignmentWithEpochs;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConsumerAssignmentPartitionDeletionReviewTest {
    private static final String GROUP = "consumer-deletion-review-group";
    private static final String MEMBER = "consumer-deletion-review-member";
    private static final String TOPIC = "consumer-deletion-review-topic";
    private static final String OTHER_TOPIC = "other-consumer-deletion-review-topic";
    private static final Uuid TOPIC_ID = new Uuid(31L, 37L);
    private static final Uuid OTHER_TOPIC_ID = new Uuid(41L, 43L);

    @Test
    void homogeneousDefaultConsumerAssignorMustRebalanceAfterTailRemoval() {
        assertRebalanceAfterRemoval(false);
    }

    @Test
    void heterogeneousDefaultConsumerAssignorMustRebalanceAfterTailRemoval() {
        assertRebalanceAfterRemoval(true);
    }

    private static void assertRebalanceAfterRemoval(boolean heterogeneous) {
        MetadataImage original = new MetadataImageBuilder().addTopic(TOPIC_ID, TOPIC, 4)
            .addTopic(OTHER_TOPIC_ID, OTHER_TOPIC, 1).build();
        CoordinatorMetadataImage image = new KRaftCoordinatorMetadataImage(original);
        ConsumerGroupBuilder group = new ConsumerGroupBuilder(GROUP, 10)
            .withMember(member(MEMBER, TOPIC, TOPIC_ID, Set.of(0, 1, 2, 3)))
            .withAssignment(MEMBER, Map.of(TOPIC_ID, Set.of(0, 1, 2, 3)))
            .withAssignmentEpoch(10);
        if (heterogeneous) {
            group.withMember(member("other-member", OTHER_TOPIC, OTHER_TOPIC_ID, Set.of(0)))
                .withAssignment("other-member", Map.of(OTHER_TOPIC_ID, Set.of(0)))
                .withMetadataHash(computeGroupHash(Map.of(TOPIC, computeTopicHash(TOPIC, image),
                    OTHER_TOPIC, computeTopicHash(OTHER_TOPIC, image))));
        } else {
            group.withMetadataHash(computeGroupHash(Map.of(TOPIC, computeTopicHash(TOPIC, image))));
        }
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(image)
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNORS_CONFIG, List.of(new UniformAssignor()))
            .withConfig(GroupCoordinatorConfig.CONSUMER_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 0)
            .withConsumerGroup(group)
            .build();
        MetadataDelta delta = new MetadataDelta.Builder().setImage(original).build();
        delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(3));
        delta.replay(new RemovePartitionRecord().setTopicId(TOPIC_ID).setPartitionId(2));
        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
            new KRaftCoordinatorMetadataImage(delta.apply(new MetadataProvenance(1L, 0, 0L, true))));

        assertDoesNotThrow(() -> context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP).setMemberId(MEMBER).setMemberEpoch(10)
            .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                .setTopicId(TOPIC_ID).setPartitions(List.of(0, 1, 2, 3))))));

        assertEquals(Map.of(TOPIC_ID, Set.of(0, 1)), context.groupMetadataManager.consumerGroup(GROUP)
            .targetAssignment().get(MEMBER).partitions());
        ConsumerGroupMember revoking = context.groupMetadataManager.consumerGroup(GROUP).members().get(MEMBER);
        assertEquals(10, revoking.memberEpoch());
        assertEquals(MemberState.UNREVOKED_PARTITIONS, revoking.state());
        assertEquals(Map.of(TOPIC_ID, Map.of(0, 10, 1, 10)), revoking.assignedPartitions());
        assertEquals(Map.of(TOPIC_ID, Map.of(2, 10, 3, 10)), revoking.partitionsPendingRevocation());

        context.consumerGroupHeartbeat(new ConsumerGroupHeartbeatRequestData()
            .setGroupId(GROUP).setMemberId(MEMBER).setMemberEpoch(10)
            .setTopicPartitions(List.of(new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                .setTopicId(TOPIC_ID).setPartitions(List.of(0, 1)))));
        ConsumerGroupMember reconciled = context.groupMetadataManager.consumerGroup(GROUP).members().get(MEMBER);
        assertEquals(11, reconciled.memberEpoch());
        assertEquals(MemberState.STABLE, reconciled.state());
        assertTrue(reconciled.partitionsPendingRevocation().isEmpty());
        if (heterogeneous) {
            assertEquals(Map.of(OTHER_TOPIC_ID, Set.of(0)), context.groupMetadataManager.consumerGroup(GROUP)
                .targetAssignment().get("other-member").partitions());
        }
    }

    private static ConsumerGroupMember member(String id, String topic, Uuid topicId, Set<Integer> partitions) {
        return new ConsumerGroupMember.Builder(id).setState(MemberState.STABLE)
            .setMemberEpoch(10).setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID).setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setRebalanceTimeoutMs(5000).setSubscribedTopicNames(List.of(topic))
            .setServerAssignorName(UniformAssignor.NAME)
            .setAssignedPartitions(toAssignmentWithEpochs(Map.of(topicId, partitions), 10))
            .build();
    }
}
