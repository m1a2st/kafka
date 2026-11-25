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
package org.apache.kafka.coordinator.group.assignor;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.api.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.api.assignor.PartitionAssignor;
import org.apache.kafka.coordinator.group.api.assignor.SubscriptionType;
import org.apache.kafka.coordinator.group.modern.Assignment;
import org.apache.kafka.coordinator.group.modern.GroupSpecImpl;
import org.apache.kafka.coordinator.group.modern.MemberSubscriptionAndAssignmentImpl;
import org.apache.kafka.coordinator.group.modern.SubscribedTopicDescriberImpl;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.coordinator.group.AssignmentTestUtil.invertedTargetAssignment;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HETEROGENEOUS;
import static org.apache.kafka.coordinator.group.api.assignor.SubscriptionType.HOMOGENEOUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AssignmentPartitionShrinkTest {
    private static final Uuid FIRST_TOPIC = new Uuid(53L, 59L);
    private static final Uuid SECOND_TOPIC = new Uuid(61L, 67L);

    static Stream<Arguments> assignors() {
        return Stream.of(new UniformAssignor(), new RangeAssignor(), new SimpleAssignor())
            .flatMap(assignor -> Stream.of(Arguments.of(assignor, HOMOGENEOUS), Arguments.of(assignor, HETEROGENEOUS)));
    }

    @ParameterizedTest
    @MethodSource("assignors")
    void shrinkingSeveralTopicsKeepsEveryLivePartitionAndNeverMutatesPriorOwnership(
        PartitionAssignor assignor,
        SubscriptionType subscriptionType
    ) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new LinkedHashMap<>();
        if (subscriptionType == HOMOGENEOUS) {
            members.put("a", member(Set.of(FIRST_TOPIC, SECOND_TOPIC), Map.of(FIRST_TOPIC, Set.of(0, 1, 2, 3))));
            members.put("b", member(Set.of(FIRST_TOPIC, SECOND_TOPIC), Map.of(SECOND_TOPIC, Set.of(0, 1, 2, 3))));
        } else {
            members.put("a", member(Set.of(FIRST_TOPIC, SECOND_TOPIC),
                Map.of(FIRST_TOPIC, Set.of(0, 1, 2, 3), SECOND_TOPIC, Set.of(0, 1))));
            members.put("b", member(Set.of(SECOND_TOPIC), Map.of(SECOND_TOPIC, Set.of(2, 3))));
        }
        Map<String, Map<Uuid, Set<Integer>>> priorAssignments = new HashMap<>();
        members.forEach((id, member) -> {
            Map<Uuid, Set<Integer>> prior = new HashMap<>();
            member.partitions().forEach((topicId, partitions) -> prior.put(topicId, new HashSet<>(partitions)));
            priorAssignments.put(id, prior);
        });
        var metadata = new MetadataImageBuilder().addTopic(FIRST_TOPIC, "first", 2)
            .addTopic(SECOND_TOPIC, "second", 1).buildCoordinatorMetadataImage();

        GroupAssignment assignment = assignor.assign(new GroupSpecImpl(members, subscriptionType,
            invertedTargetAssignment(members)), new SubscribedTopicDescriberImpl(metadata));

        Map<Uuid, Set<Integer>> covered = new HashMap<>();
        Map<Uuid, Map<Integer, Integer>> ownerCounts = new HashMap<>();
        assignment.members().forEach((id, assigned) -> assigned.partitions().forEach((topicId, partitions) -> {
            assertTrue(members.get(id).subscribedTopicIds().contains(topicId));
            int partitionCount = topicId.equals(FIRST_TOPIC) ? 2 : 1;
            partitions.forEach(partition -> assertTrue(partition >= 0 && partition < partitionCount,
                "Removed partition " + topicId + "-" + partition + " assigned to " + id));
            covered.computeIfAbsent(topicId, __ -> new HashSet<>()).addAll(partitions);
            partitions.forEach(partition -> ownerCounts.computeIfAbsent(topicId, __ -> new HashMap<>())
                .merge(partition, 1, Integer::sum));
        }));
        assertEquals(Map.of(FIRST_TOPIC, Set.of(0, 1), SECOND_TOPIC, Set.of(0)), covered);
        if (assignor instanceof UniformAssignor || assignor instanceof RangeAssignor) {
            assertEquals(Map.of(FIRST_TOPIC, Map.of(0, 1, 1, 1), SECOND_TOPIC, Map.of(0, 1)), ownerCounts);
        }
        members.forEach((id, member) -> assertEquals(priorAssignments.get(id), member.partitions()));
        if (subscriptionType == HETEROGENEOUS) {
            assertEquals(Set.of(0, 1), assignment.members().get("a").partitions().get(FIRST_TOPIC));
        }
    }

    @ParameterizedTest
    @EnumSource(SubscriptionType.class)
    void shareAssignmentsMustRespectInitializationBoundsForPreviouslyOwnedPartitions(SubscriptionType subscriptionType) {
        Map<String, MemberSubscriptionAndAssignmentImpl> members = new LinkedHashMap<>();
        members.put("a", member(Set.of(FIRST_TOPIC, SECOND_TOPIC),
            Map.of(FIRST_TOPIC, Set.of(0, 1, 2), SECOND_TOPIC, Set.of(0, 1))));
        members.put("b", member(subscriptionType == HOMOGENEOUS ? Set.of(FIRST_TOPIC, SECOND_TOPIC) : Set.of(SECOND_TOPIC),
            Map.of(SECOND_TOPIC, Set.of(0, 1))));
        var metadata = new MetadataImageBuilder().addTopic(FIRST_TOPIC, "first", 3)
            .addTopic(SECOND_TOPIC, "second", 2).buildCoordinatorMetadataImage();
        GroupAssignment assignment = new SimpleAssignor().assign(new GroupSpecImpl(members, subscriptionType,
            invertedTargetAssignment(members), Optional.of(Map.of(FIRST_TOPIC, Set.of(0), SECOND_TOPIC, Set.of()))),
            new SubscribedTopicDescriberImpl(metadata));

        Set<Integer> covered = new HashSet<>();
        assignment.members().forEach((id, assigned) -> {
            assertTrue(assigned.partitions().getOrDefault(SECOND_TOPIC, Set.of()).isEmpty());
            Set<Integer> partitions = assigned.partitions().getOrDefault(FIRST_TOPIC, Set.of());
            assertTrue(Set.of(0).containsAll(partitions));
            covered.addAll(partitions);
        });
        assertEquals(Set.of(0), covered);
    }

    private static MemberSubscriptionAndAssignmentImpl member(Set<Uuid> topics, Map<Uuid, Set<Integer>> assignment) {
        return new MemberSubscriptionAndAssignmentImpl(Optional.empty(), Optional.empty(), topics, new Assignment(assignment));
    }
}
