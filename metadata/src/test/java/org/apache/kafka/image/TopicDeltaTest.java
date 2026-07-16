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

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        delta.replayPartitionDraining(2);
        delta.replayPartitionDraining(3);

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

        delta.replayPartitionDraining(2);

        // Broker 5 is NOT a replica, should not see draining
        LocalReplicaChanges changes = delta.localChanges(5);
        assertTrue(changes.drainingPartitions().isEmpty());
    }

    @Test
    public void testRemovePartitionClearsDraining() {
        TopicImage image = createTopicImage(4, 0, 1, 2);
        TopicDelta delta = new TopicDelta(image);

        delta.replayPartitionDraining(3);
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
}
