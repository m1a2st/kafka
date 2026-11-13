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

package org.apache.kafka.controller;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.AlterPartitionReassignmentsResponseData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.message.DeletePartitionsRequestData.DeletePartitionsTopic;
import org.apache.kafka.common.message.DeletePartitionsResponseData.DeletePartitionsTopicResult;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.controller.ReplicationControlManagerTest.ReplicationControlTestContext;
import org.apache.kafka.image.TopicDelta;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.TopicIdPartition;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.common.protocol.Errors.NONE;
import static org.apache.kafka.common.protocol.Errors.UNKNOWN_TOPIC_OR_PARTITION;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(40)
public class DeletePartitionsTest {
    @Test
    public void testDeletePartitions() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", 4, (short) 3, NONE.code()).topicId();

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("foo").setCount(2));

        ControllerResult<List<DeletePartitionsTopicResult>> result =
            ctx.replicationControl.deletePartitions(requestContext, topics);

        // Should produce 2 PartitionDrainingRecord (for partitions 2 and 3)
        assertEquals(2, result.records().size());
        assertEquals(NONE.code(), result.response().get(0).errorCode());

        // Replay the records
        ctx.replay(result.records());

        // All partitions should still exist (they are draining, not removed yet)
        assertNotNull(ctx.replicationControl.getPartition(topicId, 0));
        assertNotNull(ctx.replicationControl.getPartition(topicId, 2));
        assertNotNull(ctx.replicationControl.getPartition(topicId, 3));
    }

    @Test
    public void testDeletePartitionsUnknownTopic() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("nonexistent").setCount(1));

        ControllerResult<List<DeletePartitionsTopicResult>> result =
            ctx.replicationControl.deletePartitions(requestContext, topics);

        assertEquals(UNKNOWN_TOPIC_OR_PARTITION.code(), result.response().get(0).errorCode());
    }

    @Test
    public void testDeletePartitionsInvalidCount() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", 4, (short) 3, NONE.code());

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);

        // Count >= current should fail
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("foo").setCount(4));
        ControllerResult<List<DeletePartitionsTopicResult>> result =
            ctx.replicationControl.deletePartitions(requestContext, topics);
        assertEquals(Errors.INVALID_DELETE_PARTITION_COUNT.code(), result.response().get(0).errorCode());

        // Count = 0 should fail
        topics = List.of(new DeletePartitionsTopic().setName("foo").setCount(0));
        result = ctx.replicationControl.deletePartitions(requestContext, topics);
        assertEquals(Errors.INVALID_DELETE_PARTITION_COUNT.code(), result.response().get(0).errorCode());
    }

    @Test
    public void testDeletePartitionsAlreadyDraining() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("foo", 4, (short) 3, NONE.code());

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("foo").setCount(2));

        // First call should succeed
        ControllerResult<List<DeletePartitionsTopicResult>> result =
            ctx.replicationControl.deletePartitions(requestContext, topics);
        assertEquals(NONE.code(), result.response().get(0).errorCode());
        ctx.replay(result.records());

        // Second call for same range should fail with PARTITION_OPERATION_IN_PROGRESS
        result = ctx.replicationControl.deletePartitions(requestContext, topics);
        assertEquals(Errors.PARTITION_OPERATION_IN_PROGRESS.code(), result.response().get(0).errorCode());
    }

    @Test
    public void testDeletePartitionsInternalTopic() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        ctx.createTestTopic("__consumer_offsets", 4, (short) 3, NONE.code());

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("__consumer_offsets").setCount(2));

        ControllerResult<List<DeletePartitionsTopicResult>> result =
            ctx.replicationControl.deletePartitions(requestContext, topics);

        assertEquals(Errors.INVALID_REQUEST.code(), result.response().get(0).errorCode());
    }

    @Test
    public void testDeletePartitionsDuringReassignment() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ReplicationControlManager replication = ctx.replicationControl;
        ctx.registerBrokers(0, 1, 2, 3);
        ctx.unfenceBrokers(0, 1, 2, 3);
        ctx.createTestTopic("foo", new int[][] {
            new int[] {0, 1, 2}, new int[] {0, 1, 2},
            new int[] {0, 1, 2}, new int[] {0, 1, 2}});

        ControllerResult<AlterPartitionReassignmentsResponseData> alterResult =
            replication.alterPartitionReassignments(
                new AlterPartitionReassignmentsRequestData().setTopics(List.of(
                    new ReassignableTopic().setName("foo").setPartitions(List.of(
                        new ReassignablePartition().setPartitionIndex(3).
                            setReplicas(List.of(1, 2, 3)))))));
        ctx.replay(alterResult.records());

        ControllerRequestContext requestContext = anonymousContextFor(ApiKeys.CREATE_PARTITIONS);
        List<DeletePartitionsTopic> topics = List.of(
            new DeletePartitionsTopic().setName("foo").setCount(2));

        ControllerResult<List<DeletePartitionsTopicResult>> result =
            replication.deletePartitions(requestContext, topics);

        assertEquals(Errors.PARTITION_OPERATION_IN_PROGRESS.code(), result.response().get(0).errorCode());
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4, 6})
    public void testExpansionReopensDrainingPartitions(int targetCount) {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", 4, (short) 3, NONE.code()).topicId();
        PartitionRegistration original = ctx.replicationControl.getPartition(topicId, 2);
        ctx.replicationControl.replay(new PartitionDrainingRecord()
            .setTopicId(topicId).setPartitionId(2).setDeadlineMs(1).setForce(true));
        ctx.replicationControl.replay(new PartitionDrainingRecord()
            .setTopicId(topicId).setPartitionId(3).setDeadlineMs(1).setForce(true));

        ControllerResult<List<CreatePartitionsTopicResult>> result = ctx.replicationControl.createPartitions(
            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(
                new CreatePartitionsTopic().setName("foo").setCount(targetCount).setAssignments(null)));
        assertEquals(NONE.code(), result.response().get(0).errorCode());
        assertEquals(targetCount - 2, result.records().size());
        // Validation itself must not cancel deletion before the records are committed.
        assertEquals(2, ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
        ctx.replay(result.records());
        assertEquals(original, ctx.replicationControl.getPartition(topicId, 2));
        assertNotNull(ctx.replicationControl.getPartition(topicId, targetCount - 1));
        Set<Integer> stillDraining = ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().stream()
            .map(record -> ((RemovePartitionRecord) record.message()).partitionId()).collect(Collectors.toSet());
        assertEquals(targetCount == 3 ? Set.of(3) : Set.of(), stillDraining);
    }

    @Test
    public void testExpansionCannotReuseDeletedPartitionIds() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0);
        ctx.unfenceBrokers(0);
        Uuid topicId = ctx.createTestTopic("foo", 3, (short) 1, NONE.code()).topicId();
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        ControllerResult<List<CreatePartitionsTopicResult>> result = ctx.replicationControl.createPartitions(
            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(
                new CreatePartitionsTopic().setName("foo").setCount(3).setAssignments(null)));
        assertEquals(Errors.INVALID_PARTITIONS.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
    }

    @Test
    public void testDeletedPartitionIdsStayFencedAfterSnapshot() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0);
        ctx.unfenceBrokers(0);
        Uuid topicId = ctx.createTestTopic("foo", 3, (short) 1, NONE.code()).topicId();
        TopicDelta delta = new TopicDelta(new TopicImage("foo", topicId, Map.of(
            0, ctx.replicationControl.getPartition(topicId, 0),
            1, ctx.replicationControl.getPartition(topicId, 1),
            2, ctx.replicationControl.getPartition(topicId, 2))));
        delta.replayRemovePartition(2);
        RecordListWriter writer = new RecordListWriter();
        delta.apply().write(writer, new ImageWriterOptions.Builder(ctx.featureControl.metadataVersionOrThrow()).build());

        ReplicationControlTestContext restored = new ReplicationControlTestContext.Builder().build();
        restored.registerBrokers(0);
        restored.unfenceBrokers(0);
        restored.replay(writer.records());
        ControllerResult<List<CreatePartitionsTopicResult>> result = restored.replicationControl.createPartitions(
            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(
                new CreatePartitionsTopic().setName("foo").setCount(3).setAssignments(null)));
        assertEquals(Errors.INVALID_PARTITIONS.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
    }

    @Test
    public void testInvalidExpansionDoesNotCancelDraining() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0);
        ctx.unfenceBrokers(0);
        Uuid topicId = ctx.createTestTopic("foo", 3, (short) 1, NONE.code()).topicId();
        ctx.replicationControl.replay(new PartitionDrainingRecord()
            .setTopicId(topicId).setPartitionId(2).setDeadlineMs(1).setForce(true));
        ControllerResult<List<CreatePartitionsTopicResult>> result = ctx.replicationControl.createPartitions(
            anonymousContextFor(ApiKeys.CREATE_PARTITIONS), List.of(
                new CreatePartitionsTopic().setName("foo").setCount(4).setAssignments(List.of())));
        assertEquals(Errors.INVALID_REPLICA_ASSIGNMENT.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
        assertEquals(1, ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
    }

    @Test
    public void testDeletePartitionsDeadlineExpiry() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", 4, (short) 3, NONE.code()).topicId();

        long expiredDeadlineMs = 1L;
        ctx.replay(List.of(
            new ApiMessageAndVersion(new PartitionDrainingRecord().
                setTopicId(topicId).setPartitionId(2).setDeadlineMs(expiredDeadlineMs).setForce(true), (short) 0),
            new ApiMessageAndVersion(new PartitionDrainingRecord().
                setTopicId(topicId).setPartitionId(3).setDeadlineMs(expiredDeadlineMs).setForce(true), (short) 0)
        ));

        ControllerResult<Boolean> result =
            ctx.replicationControl.maybeRemoveExpiredDrainingPartitions();

        assertEquals(2, result.records().size());
        for (ApiMessageAndVersion record : result.records()) {
            assertInstanceOf(RemovePartitionRecord.class, record.message());
        }

        Set<Integer> removedPartitions = new HashSet<>();
        for (ApiMessageAndVersion record : result.records()) {
            RemovePartitionRecord rpr = (RemovePartitionRecord) record.message();
            assertEquals(topicId, rpr.topicId());
            removedPartitions.add(rpr.partitionId());
        }
        assertTrue(removedPartitions.contains(2));
        assertTrue(removedPartitions.contains(3));

        ctx.replay(result.records());

        assertNull(ctx.replicationControl.getPartition(topicId, 2));
        assertNull(ctx.replicationControl.getPartition(topicId, 3));
        assertNotNull(ctx.replicationControl.getPartition(topicId, 0));
        assertNotNull(ctx.replicationControl.getPartition(topicId, 1));
    }

    @Test
    public void testRemovePartitionPreservesRetainedPartitionIsr() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", new int[][] {
            {0, 1, 2}, {0, 1, 2}, {0, 1, 2}
        }).topicId();

        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(1));

        ctx.fenceBrokers(0);
        PartitionRegistration retainedPartition = ctx.replicationControl.getPartition(topicId, 0);
        assertEquals(1, retainedPartition.leader);
        assertArrayEquals(new int[] {1, 2}, retainedPartition.isr);

        ctx.fenceBrokers(0, 1);
        retainedPartition = ctx.replicationControl.getPartition(topicId, 0);
        assertEquals(2, retainedPartition.leader);
        assertArrayEquals(new int[] {2}, retainedPartition.isr);
        assertNull(ctx.replicationControl.getPartition(topicId, 1));
        assertNull(ctx.replicationControl.getPartition(topicId, 2));
    }

    @Test
    public void testRemovePartitionPreservesRetainedPartitionElr() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().setIsElrEnabled(true).build();
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        Uuid topicId = ctx.createTestTopic("foo", new int[][] {
            {0, 1, 2}, {0, 1, 2}, {0, 1, 2}
        }).topicId();
        ctx.alterTopicConfig("foo", TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3");
        ctx.fenceBrokers(2);
        assertArrayEquals(new int[] {2}, ctx.replicationControl.getPartition(topicId, 0).elr);

        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(1));
        ctx.handleBrokersShutdown(false, 2);

        assertArrayEquals(new int[0], ctx.replicationControl.getPartition(topicId, 0).elr);
        assertFalse(ctx.replicationControl.brokersToElrs().partitionsWithBrokerInElr(2).hasNext());
    }

    @Test
    public void testRemovePartitionCleansUpDirectoryAssignments() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        Uuid sharedDir = Uuid.fromString("suitdzfTTdqoWcy8VqmkUg");
        Uuid removedDir = Uuid.fromString("yh3acnzGSeurSTj8aIhOjw");
        Uuid followerDir = Uuid.fromString("OmpmJ8RjQliQlEFht56DwQ");
        ctx.registerBrokersWithDirs(0, List.of(sharedDir, removedDir), 1, List.of(followerDir));
        ctx.unfenceBrokers(0, 1);
        Uuid topicId = ctx.createTestTopic("foo", new int[][] {{0, 1}, {0, 1}, {0, 1}}).topicId();
        ctx.assignReplicasToDirs(0, Map.of(
            new TopicIdPartition(topicId, 0), sharedDir,
            new TopicIdPartition(topicId, 1), sharedDir,
            new TopicIdPartition(topicId, 2), removedDir));

        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(1));

        List<ApiMessageAndVersion> records = new ArrayList<>();
        ctx.replicationControl.handleDirectoriesOffline(0, ctx.currentBrokerEpoch(0), List.of(sharedDir, removedDir), records);
        short recordVersion = ctx.featureControl.metadataVersionOrThrow().partitionChangeRecordVersion();
        assertEquals(List.of(new ApiMessageAndVersion(new PartitionChangeRecord().setTopicId(topicId).
            setPartitionId(0).setLeader(1).setIsr(List.of(1)), recordVersion)),
            records.stream().filter(record -> record.message() instanceof PartitionChangeRecord).collect(Collectors.toList()));
        ctx.replay(records);
        assertEquals(1, ctx.replicationControl.getPartition(topicId, 0).leader);
        assertArrayEquals(new int[] {1}, ctx.replicationControl.getPartition(topicId, 0).isr);
    }

    @Test
    public void testRemoveLeaderlessPartitionBeforeBrokerUnfencing() {
        ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
        ctx.registerBrokers(0);
        ctx.unfenceBrokers(0);
        Uuid topicId = ctx.createTestTopic("foo", new int[][] {{0}, {0}}).topicId();
        ctx.fenceBrokers(0);
        assertEquals(NO_LEADER, ctx.replicationControl.getPartition(topicId, 0).leader);
        assertEquals(NO_LEADER, ctx.replicationControl.getPartition(topicId, 1).leader);

        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(1));
        ctx.unfenceBrokers(0);

        assertEquals(0, ctx.replicationControl.getPartition(topicId, 0).leader);
        assertArrayEquals(new int[] {0}, ctx.replicationControl.getPartition(topicId, 0).isr);
        assertNull(ctx.replicationControl.getPartition(topicId, 1));
    }
}
