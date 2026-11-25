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
import org.apache.kafka.common.metadata.ClearElrRecord;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.metadata.RecordTestUtils.replayAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ImagePartitionDeletionReviewTest {
    private static final String NAME = "deletion-review";
    private static final Uuid ID = Uuid.randomUuid();
    private static final ImageWriterOptions OPTIONS =
        new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV2).setEligibleLeaderReplicasEnabled(true).build();

    private static PartitionRecord partitionRecord(Uuid topicId, int partitionId, boolean hasElr) {
        return new PartitionRecord().setTopicId(topicId).setPartitionId(partitionId)
            .setReplicas(List.of(0, 1, 2)).setDirectories(List.of(DirectoryId.MIGRATING, DirectoryId.MIGRATING, DirectoryId.MIGRATING))
            .setIsr(List.of(0, 1)).setLeader(0).setLeaderEpoch(4).setPartitionEpoch(7)
            .setEligibleLeaderReplicas(hasElr ? List.of(2) : List.of());
    }

    private static TopicImage topic(int count, boolean hasElr) {
        Map<Integer, PartitionRegistration> partitions = new HashMap<>();
        for (int id = 0; id < count; id++) {
            partitions.put(id, new PartitionRegistration(partitionRecord(ID, id, hasElr)));
        }
        return new TopicImage(NAME, ID, partitions);
    }

    private static TopicsDelta loadSnapshot(TopicsImage previous, TopicImage snapshotTopic) {
        RecordListWriter writer = new RecordListWriter();
        snapshotTopic.write(writer, OPTIONS);
        TopicsDelta restored = new TopicsDelta(previous);
        replayAll(restored, writer.records());
        restored.finishSnapshot();
        return restored;
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testSnapshotOmittingRemovedTailDeletesExistingLocalReplicas(int brokerId) {
        TopicImage previous = topic(4, false);
        TopicImage snapshot = new TopicImage(NAME, ID,
            Map.of(0, previous.partitions().get(0), 1, previous.partitions().get(1)), Map.of(), 4);

        TopicsDelta restored = loadSnapshot(TopicsImage.EMPTY.including(previous), snapshot);

        assertEquals(Set.of(0, 1), restored.apply().getTopic(ID).partitions().keySet());
        assertEquals(Set.of(new TopicPartition(NAME, 2), new TopicPartition(NAME, 3)),
            restored.localChanges(brokerId).deletes(),
            "A snapshot that omits removed IDs must stop replicas from the previously applied image");
        assertTrue(restored.localChanges(9).deletes().isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testElrClearAfterRemovalDoesNotRecreateReplicaWork(int brokerId) {
        TopicImage previous = topic(2, true);
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY.including(previous));
        delta.replay(new RemovePartitionRecord().setTopicId(ID).setPartitionId(1));
        // A following min.insync.replicas update can clear ELR in the same published delta.
        delta.replay(new ClearElrRecord().setTopicName(NAME));

        assertFalse(delta.apply().getTopic(ID).partitions().containsKey(1));
        LocalReplicaChanges changes = delta.localChanges(brokerId);
        TopicPartition removed = new TopicPartition(NAME, 1);
        assertEquals(Set.of(removed), changes.deletes());
        assertFalse(changes.leaders().containsKey(removed), "Deleted replicas must not become leaders again");
        assertFalse(changes.followers().containsKey(removed), "Deleted replicas must not become followers again");
        assertTrue(changes.directoryIds().keySet().stream().noneMatch(tp -> tp.partition() == 1));
    }

    @Test
    void testSnapshotOmittingCanceledDrainResumesExistingReplica() {
        TopicImage active = topic(2, false);
        TopicImage previous = new TopicImage(NAME, ID, active.partitions(), Map.of(1, 100L), 2, Set.of(1));

        TopicsDelta restored = loadSnapshot(TopicsImage.EMPTY.including(previous), active);

        assertEquals(active.partitions(), restored.apply().getTopic(ID).partitions());
        assertTrue(restored.apply().getTopic(ID).drainingPartitions().isEmpty());
        assertTrue(restored.apply().getTopic(ID).forcedDrainingPartitions().isEmpty());
        assertTrue(restored.localChanges(0).resumedPartitions().contains(new TopicPartition(NAME, 1)));
        assertTrue(restored.localChanges(0).drainingPartitions().isEmpty());
    }

    @Test
    void testSnapshotAfterPartialRemovalPreservesAllocationHistoryAndForce() {
        TopicImage original = topic(4, false);
        TopicImage partial = new TopicImage(NAME, ID,
            Map.of(0, original.partitions().get(0), 1, original.partitions().get(1), 2, original.partitions().get(2)),
            Map.of(2, 100L), 4, Set.of(2));

        TopicImage restored = loadSnapshot(TopicsImage.EMPTY, partial).apply().getTopic(ID);

        assertEquals(Set.of(0, 1, 2), restored.partitions().keySet());
        assertEquals(4, restored.partitionIdHighWatermark());
        assertEquals(Map.of(2, 100L), restored.drainingPartitionDeadlines());
        assertEquals(Set.of(2), restored.forcedDrainingPartitions());
    }

    @Test
    void testRemovalSupersedesEarlierLeadershipChangeInSameDelta() {
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY.including(topic(2, false)));
        delta.replay(new PartitionChangeRecord().setTopicId(ID).setPartitionId(1).setLeader(1));
        delta.replay(new RemovePartitionRecord().setTopicId(ID).setPartitionId(1));

        for (int brokerId : List.of(0, 1)) {
            LocalReplicaChanges changes = delta.localChanges(brokerId);
            assertEquals(Set.of(new TopicPartition(NAME, 1)), changes.deletes());
            assertTrue(changes.leaders().isEmpty());
            assertTrue(changes.followers().isEmpty());
            assertTrue(changes.drainingPartitions().isEmpty());
        }
    }

    @Test
    void testRecreatedTopicDoesNotInheritRemovedTopicDrainOrAllocationHistory() {
        TopicImage original = topic(3, false);
        TopicImage previous = new TopicImage(NAME, ID, original.partitions(), Map.of(2, 100L), 5, Set.of(2));
        TopicsDelta delta = new TopicsDelta(TopicsImage.EMPTY.including(previous));
        delta.replay(new RemoveTopicRecord().setTopicId(ID));
        Uuid replacementId = Uuid.randomUuid();
        delta.replay(new TopicRecord().setName(NAME).setTopicId(replacementId));
        delta.replay(partitionRecord(replacementId, 0, false));

        TopicImage replacement = delta.apply().getTopic(NAME);
        assertEquals(replacementId, replacement.id());
        assertEquals(1, replacement.partitionIdHighWatermark());
        assertTrue(replacement.drainingPartitions().isEmpty());
        assertTrue(replacement.forcedDrainingPartitions().isEmpty());
        assertEquals(Set.of(new TopicPartition(NAME, 0), new TopicPartition(NAME, 1), new TopicPartition(NAME, 2)),
            delta.localChanges(0).deletes());
        assertTrue(delta.localChanges(0).resumedPartitions().contains(new TopicPartition(NAME, 0)));

        TopicsDelta restored = loadSnapshot(TopicsImage.EMPTY.including(previous), replacement);
        assertEquals(replacement, restored.apply().getTopic(NAME));
        assertEquals(Set.of(ID), restored.deletedTopicIds());
        assertTrue(restored.changedTopic(replacementId).removedPartitions().isEmpty());
        assertEquals(Set.of(new TopicPartition(NAME, 0), new TopicPartition(NAME, 1), new TopicPartition(NAME, 2)),
            restored.localChanges(0).deletes());
    }
}
