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

package org.apache.kafka.image;

import org.apache.kafka.common.DirectoryId;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.metadata.LeaderRecoveryState;
import org.apache.kafka.metadata.PartitionRegistration;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicDeltaTest {

    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final String TOPIC_NAME = "test-topic";

    private static TopicImage createTopicImage(int numPartitions, int... replicas) {
        Map<Integer, PartitionRegistration> partitions = new java.util.HashMap<>();
        Uuid[] directories = new Uuid[replicas.length];
        for (int i = 0; i < directories.length; i++) {
            directories[i] = DirectoryId.random();
        }
        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new PartitionRegistration.Builder().
                setReplicas(replicas).
                setDirectories(directories).
                setIsr(replicas).
                setLeader(replicas[0]).
                setLeaderRecoveryState(LeaderRecoveryState.RECOVERED).
                setLeaderEpoch(0).
                setPartitionEpoch(0).
                build());
        }
        return new TopicImage(TOPIC_NAME, TOPIC_ID, partitions);
    }

    @Test
    public void testDrainingPartitionsExposedInLocalChanges() {
        TopicImage image = createTopicImage(4, 0, 1, 2);
        TopicDelta delta = new TopicDelta(image);

        delta.replayPartitionDraining(2, 1000L);
        delta.replayPartitionDraining(3, 1000L);

        // Broker 0 is a replica of all partitions, should see draining for partitions 2 and 3
        LocalReplicaChanges changes = delta.localChanges(0);
        assertEquals(2, changes.drainingPartitions().size());
        assertTrue(changes.drainingPartitions().contains(new TopicPartition(TOPIC_NAME, 2)));
        assertTrue(changes.drainingPartitions().contains(new TopicPartition(TOPIC_NAME, 3)));

        // Deletes should be empty (draining != deleted)
        assertTrue(changes.deletes().isEmpty());
    }

    @Test
    public void testDrainingPartitionNotVisibleToNonReplica() {
        TopicImage image = createTopicImage(4, 0, 1);
        TopicDelta delta = new TopicDelta(image);

        delta.replayPartitionDraining(2, 1000L);

        // Broker 5 is NOT a replica, should not see draining
        LocalReplicaChanges changes = delta.localChanges(5);
        assertTrue(changes.drainingPartitions().isEmpty());
    }

    @Test
    public void testRemovePartitionClearsDraining() {
        TopicImage image = createTopicImage(4, 0, 1, 2);
        TopicDelta delta = new TopicDelta(image);

        delta.replayPartitionDraining(3, 1000L);
        assertTrue(delta.drainingPartitions().contains(3));

        delta.replayRemovePartition(3);
        assertFalse(delta.drainingPartitions().contains(3));

        // Partition 3 should appear in deletes, not draining
        LocalReplicaChanges changes = delta.localChanges(0);
        assertTrue(changes.drainingPartitions().isEmpty());
        assertTrue(changes.deletes().contains(new TopicPartition(TOPIC_NAME, 3)));
    }

    @Test
    public void testApplyExcludesRemovedPartitions() {
        TopicImage image = createTopicImage(4, 0, 1, 2);
        TopicDelta delta = new TopicDelta(image);

        delta.replayRemovePartition(3);

        TopicImage applied = delta.apply();
        assertEquals(3, applied.partitions().size());
        assertFalse(applied.partitions().containsKey(3));
    }
    @Test
    public void testResumeClearsInheritedDrainingDeadline() {
        TopicImage image = createTopicImage(3, 0, 1);
        TopicDelta draining = new TopicDelta(image);
        draining.replayPartitionDraining(2, 1000L);
        TopicDelta resumed = new TopicDelta(draining.apply());
        resumed.replayPartitionDraining(2, -1L);
        assertTrue(resumed.apply().drainingPartitions().isEmpty());
        assertTrue(resumed.localChanges(0).drainingPartitions().isEmpty());
        assertEquals(image.partitions(), resumed.apply().partitions());
    }

    @Test
    public void testRemoveThenRecreateInSameDelta() {
        TopicImage image = createTopicImage(3, 0, 1);
        TopicDelta delta = new TopicDelta(image);
        delta.replayRemovePartition(2);
        delta.replay(new org.apache.kafka.common.metadata.PartitionRecord()
            .setTopicId(TOPIC_ID).setPartitionId(2).setReplicas(java.util.List.of(0, 1))
            .setIsr(java.util.List.of(0, 1)).setLeader(1).setLeaderEpoch(1).setPartitionEpoch(1));
        assertEquals(3, delta.apply().partitions().size());
        assertEquals(1, delta.apply().partitions().get(2).leader);
        assertTrue(delta.removedPartitions().isEmpty());
        assertTrue(delta.localChanges(0).deletes().isEmpty());
    }

    @Test
    public void testInheritedDrainingStateFollowsNewReplica() {
        TopicImage image = createTopicImage(3, 0, 1);
        TopicDelta draining = new TopicDelta(image);
        draining.replayPartitionDraining(2, 1000L);
        TopicDelta reassigned = new TopicDelta(draining.apply());
        reassigned.replay(new org.apache.kafka.common.metadata.PartitionChangeRecord()
            .setTopicId(TOPIC_ID).setPartitionId(2).setReplicas(java.util.List.of(0, 1, 2))
            .setDirectories(java.util.List.of(DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED, DirectoryId.UNASSIGNED)));
        assertEquals(java.util.Set.of(new TopicPartition(TOPIC_NAME, 2)),
            reassigned.localChanges(2).drainingPartitions());
    }

    @Test
    public void testForcedDrainingPartitionsAreImmutableAndLimitedToDrainingPartitions() {
        TopicImage original = createTopicImage(3, 0, 1);
        Set<Integer> forced = new HashSet<>(Set.of(1, 2));
        TopicImage image = new TopicImage(original.name(), original.id(), original.partitions(),
            Map.of(1, 1000L), original.partitionIdHighWatermark(), forced);
        assertEquals(Set.of(1), image.forcedDrainingPartitions());
        forced.clear();
        assertEquals(Set.of(1), image.forcedDrainingPartitions());
        assertThrows(UnsupportedOperationException.class, () -> image.forcedDrainingPartitions().clear());
    }

    @Test
    public void testNormalDrainReplacesForceOverrideWithinSameDelta() {
        TopicDelta delta = new TopicDelta(createTopicImage(3, 0, 1));
        delta.replayPartitionDraining(2, 1000L, true);
        assertEquals(Set.of(2), delta.apply().forcedDrainingPartitions());
        delta.replayPartitionDraining(2, 2000L);
        assertEquals(Map.of(2, 2000L), delta.apply().drainingPartitionDeadlines());
        assertEquals(Set.of(), delta.apply().forcedDrainingPartitions());
    }

    @Test
    public void testCancelClearsForceOverrideWithinSameDelta() {
        TopicDelta delta = new TopicDelta(createTopicImage(3, 0, 1));
        delta.replayPartitionDraining(2, 1000L, true);
        delta.replayPartitionDraining(2, -1L, true);
        assertEquals(Set.of(), delta.apply().drainingPartitions());
        assertEquals(Set.of(), delta.apply().forcedDrainingPartitions());
    }

    @Test
    public void testRemovalClearsForceOverrideWithinSameDelta() {
        TopicDelta delta = new TopicDelta(createTopicImage(3, 0, 1));
        delta.replayPartitionDraining(2, 1000L, true);
        delta.replayRemovePartition(2);
        assertEquals(Set.of(), delta.apply().drainingPartitions());
        assertEquals(Set.of(), delta.apply().forcedDrainingPartitions());
    }

}
