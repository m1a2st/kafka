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
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataDelta;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.assignor.SimpleAssignor;
import org.apache.kafka.coordinator.group.modern.MemberState;
import org.apache.kafka.coordinator.group.modern.share.ShareGroup.InitMapValue;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupBuilder;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupMember;
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class SharePartitionDeletionReviewTest {
    private static final String GROUP = "share-deletion-review-group";
    private static final String MEMBER = "share-deletion-review-member";
    private static final String TOPIC = "share-deletion-review-topic";

    @Test
    void realShareAssignmentMustRevokeDeletedIdsAndKeepEveryLivePartition() {
        Uuid topicId = new Uuid(0L, 16L);
        GroupMetadataManagerTestContext context = context(topicId);

        shrink(context, topicId);
        context.shareGroupHeartbeat(heartbeat(10));

        assertEquals(Map.of(topicId, Set.of(0, 1)), context.groupMetadataManager.shareGroup(GROUP)
            .targetAssignment().get(MEMBER).partitions());
        assertEquals(Map.of(topicId, Set.of(0, 1)), context.groupMetadataManager.shareGroup(GROUP)
            .members().get(MEMBER).assignedPartitions());
    }

    @Test
    void aSettledShrinkMustNotTriggerAnotherRebalanceOnEveryHeartbeat() {
        Uuid topicId = new Uuid(0L, 2L);
        GroupMetadataManagerTestContext context = context(topicId);
        shrink(context, topicId);
        var firstResponse = context.shareGroupHeartbeat(heartbeat(10)).response().getKey();
        assertEquals(Map.of(topicId, Set.of(0, 1)), context.groupMetadataManager.shareGroup(GROUP)
            .targetAssignment().get(MEMBER).partitions());
        int settledEpoch = context.groupMetadataManager.shareGroup(GROUP).groupEpoch();

        context.time.sleep(1001L);
        context.shareGroupHeartbeat(heartbeat(firstResponse.memberEpoch()));

        assertEquals(settledEpoch, context.groupMetadataManager.shareGroup(GROUP).groupEpoch(),
            "A deleted initialized partition must not be treated as an unassigned live partition");
        assertFalse(context.groupMetadataManager.initializedAssignmentPending(context.groupMetadataManager.shareGroup(GROUP)));
    }

    private static GroupMetadataManagerTestContext context(Uuid topicId) {
        CoordinatorMetadataImage image = new MetadataImageBuilder().addTopic(topicId, TOPIC, 4)
            .buildCoordinatorMetadataImage();
        ShareGroupMember member = new ShareGroupMember.Builder(MEMBER)
            .setState(MemberState.STABLE)
            .setMemberEpoch(10)
            .setPreviousMemberEpoch(9)
            .setClientId(DEFAULT_CLIENT_ID)
            .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
            .setSubscribedTopicNames(List.of(TOPIC))
            .setAssignedPartitions(Map.of(topicId, Set.of(0, 1, 2, 3)))
            .build();
        GroupMetadataManagerTestContext context = new GroupMetadataManagerTestContext.Builder()
            .withMetadataImage(image)
            .withShareGroupAssignor(new SimpleAssignor())
            .withConfig(GroupCoordinatorConfig.SHARE_GROUP_ASSIGNMENT_INTERVAL_MS_CONFIG, 0)
            .withShareGroup(new ShareGroupBuilder(GROUP, 10)
                .withMember(member)
                .withAssignment(MEMBER, Map.of(topicId, Set.of(0, 1, 2, 3)))
                .withAssignmentEpoch(10)
                .withMetadataHash(computeGroupHash(Map.of(TOPIC, computeTopicHash(TOPIC, image)))))
            .build();
        context.replay(GroupCoordinatorRecordHelpers.newShareGroupStatePartitionMetadataRecord(GROUP,
            Map.of(), Map.of(topicId, new InitMapValue(TOPIC, Set.of(0, 1, 2, 3), 0L)), Map.of()));
        return context;
    }

    private static void shrink(GroupMetadataManagerTestContext context, Uuid topicId) {
        MetadataImage before = new MetadataImageBuilder().addTopic(topicId, TOPIC, 4).build();
        MetadataDelta delta = new MetadataDelta.Builder().setImage(before).build();
        delta.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(3));
        delta.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        context.groupMetadataManager.onMetadataUpdate(new KRaftCoordinatorMetadataDelta(delta),
            new KRaftCoordinatorMetadataImage(delta.apply(new MetadataProvenance(1L, 0, 0L, true))));
    }

    private static ShareGroupHeartbeatRequestData heartbeat(int epoch) {
        return new ShareGroupHeartbeatRequestData().setGroupId(GROUP).setMemberId(MEMBER).setMemberEpoch(epoch);
    }
}
