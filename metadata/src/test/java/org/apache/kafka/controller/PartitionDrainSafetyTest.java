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
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.PartitionDrainStatus;
import org.apache.kafka.common.message.DeletePartitionsRequestData.DeletePartitionsTopic;
import org.apache.kafka.common.metadata.PartitionChangeRecord;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.controller.ReplicationControlManagerTest.ReplicationControlTestContext;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.PartitionRegistration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextWithMutationQuotaExceededFor;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionDrainSafetyTest {
    private final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
    private final Uuid topicId;

    PartitionDrainSafetyTest() {
        ctx.registerBrokers(0, 1);
        ctx.unfenceBrokers(0, 1);
        topicId = ctx.createTestTopic("foo", new int[][] {{0, 1}, {0, 1}}).topicId();
    }

    private PartitionDrainingRecord drain(long deadline) {
        return new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(1).setDeadlineMs(deadline);
    }

    private PartitionDrainStatus ready() {
        PartitionRegistration partition = ctx.replicationControl.getPartition(topicId, 1);
        return new PartitionDrainStatus().setTopicId(topicId).setPartitionId(1)
            .setLeaderEpoch(partition.leaderEpoch).setPartitionEpoch(partition.partitionEpoch)
            .setMetadataOffset(100).setReady(true);
    }

    private void heartbeat(int brokerId, List<PartitionDrainStatus> statuses) {
        long epoch = ctx.currentBrokerEpoch(brokerId);
        ctx.clusterControl.trackBrokerHeartbeat(brokerId, epoch);
        ctx.replay(ctx.replicationControl.processBrokerHeartbeat(new BrokerHeartbeatRequestData()
            .setBrokerId(brokerId).setBrokerEpoch(epoch).setCurrentMetadataOffset(1000)
            .setDrainingPartitions(statuses), 0).records());
    }

    private void heartbeat(PartitionDrainStatus status) {
        heartbeat(ctx.replicationControl.getPartition(topicId, 1).leader, List.of(status));
    }

    private int removals() {
        return ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size();
    }

    @Test
    void testExpiredDrainWithoutLeaderConfirmationRemains() {
        ctx.replicationControl.replay(drain(1));
        assertEquals(0, removals());
    }

    @Test
    void testBlockedTransactionRemainsAfterDeadline() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready().setReady(false).setBlockedReason("Transaction is still unstable"));
        assertEquals(0, removals());
    }

    @Test
    void testReadyLeaderAllowsRemovalOnlyAfterDeadline() {
        ctx.replicationControl.replay(drain(Long.MAX_VALUE));
        heartbeat(ready());
        assertEquals(0, removals());
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
    }

    @ParameterizedTest
    @ValueSource(strings = {"leaderEpoch", "partitionEpoch", "topicId", "partitionId", "metadataOffset"})
    void testStaleOrMismatchedReportIsRejected(String field) {
        ctx.replicationControl.replay(drain(1));
        PartitionDrainStatus status = ready();
        switch (field) {
            case "leaderEpoch" -> status.setLeaderEpoch(status.leaderEpoch() - 1);
            case "partitionEpoch" -> status.setPartitionEpoch(status.partitionEpoch() - 1);
            case "topicId" -> status.setTopicId(Uuid.randomUuid());
            case "partitionId" -> status.setPartitionId(0);
            case "metadataOffset" -> status.setMetadataOffset(-1);
            default -> throw new AssertionError(field);
        }
        heartbeat(status);
        assertEquals(0, removals());
    }

    @Test
    void testFollowerCannotConfirmReadiness() {
        ctx.replicationControl.replay(drain(1));
        int leader = ctx.replicationControl.getPartition(topicId, 1).leader;
        heartbeat(1 - leader, List.of(ready()));
        assertEquals(0, removals());
    }

    @Test
    void testOmittedOrBlockedStatusInvalidatesPreviousReadiness() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        heartbeat(ctx.replicationControl.getPartition(topicId, 1).leader, List.of());
        assertEquals(0, removals());
        heartbeat(ready());
        assertEquals(1, removals());
        heartbeat(ready().setReady(false).setBlockedReason("Marker awaits replication"));
        assertEquals(0, removals());
    }

    @Test
    void testLeaderChangeRequiresFreshConfirmation() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        ctx.replicationControl.replay(new PartitionChangeRecord().setTopicId(topicId).setPartitionId(1)
            .setLeader(1).setIsr(List.of(1)));
        assertEquals(0, removals());
        heartbeat(ready());
        assertEquals(1, removals());
    }

    @Test
    void testExpiredBrokerSessionInvalidatesReadiness() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        ctx.time.sleep(2000);
        assertEquals(0, removals());
    }

    @Test
    void testControllerFailoverRequiresFreshConfirmation() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        ctx.clusterControl.deactivate();
        ctx.clusterControl.activate();
        assertEquals(0, removals());
        heartbeat(ready());
        assertEquals(1, removals());
    }

    @Test
    void testCancelAndRedrainRequireFreshConfirmation() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        ctx.replicationControl.replay(drain(-1));
        ctx.replicationControl.replay(drain(1));
        assertEquals(0, removals());
        heartbeat(ready());
        assertEquals(1, removals());
    }

    @Test
    void testExplicitForceBypassesReadinessButPreservesDeadline() {
        ctx.replicationControl.replay(drain(Long.MAX_VALUE).setForce(true));
        assertEquals(0, removals());
        ctx.replicationControl.replay(drain(1).setForce(true));
        assertEquals(1, removals());
        ctx.replicationControl.replay(drain(-1));
        ctx.replicationControl.replay(drain(1));
        assertEquals(0, removals());
    }

    @Test
    void testForceUpgradesBlockedDrainWithoutExtendingDeadline() {
        ctx.replicationControl.replay(drain(1));
        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName("foo").setCount(1).setForce(true)));
        assertEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertEquals(1, result.records().size());
        PartitionDrainingRecord record = (PartitionDrainingRecord) result.records().get(0).message();
        assertTrue(record.force());
        assertEquals(1, record.deadlineMs());
        ctx.replay(result.records());
        assertEquals(1, removals());
    }

    @Test
    void testNormalRequestDefaultsToProtectedDrain() {
        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName("foo").setCount(1)));
        assertEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertFalse(((PartitionDrainingRecord) result.records().get(0).message()).force());
    }

    @Test
    void testReportMustHaveAppliedCurrentDrainRecord() {
        ctx.replicationControl.replay(drain(1), 101L);
        heartbeat(ready().setMetadataOffset(100));
        assertEquals(0, removals());
        heartbeat(ready().setMetadataOffset(101));
        assertEquals(1, removals());
        ctx.replicationControl.replay(drain(-1), 102L);
        ctx.replicationControl.replay(drain(1), 103L);
        // A delayed report from the previous drain has matching epochs but the wrong metadata offset.
        heartbeat(ready().setMetadataOffset(101));
        assertEquals(0, removals());
        heartbeat(ready().setMetadataOffset(103));
        assertEquals(1, removals());
    }

    @Test
    void testBrokerReregistrationInvalidatesReadiness() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        int leader = ctx.replicationControl.getPartition(topicId, 1).leader;
        RegisterBrokerRecord record = (RegisterBrokerRecord) ctx.clusterControl.registration(leader)
            .toRecord(new ImageWriterOptions.Builder(ctx.featureControl.metadataVersionOrThrow()).build()).message();
        ctx.clusterControl.replay(record.setBrokerEpoch(record.brokerEpoch() + 1L).setFenced(false), 100L);
        assertEquals(0, removals());
        heartbeat(ready());
        assertEquals(1, removals());
    }

    @Test
    void testRejectedForceUpgradeDoesNotBypassProtection() {
        ctx.replicationControl.replay(drain(1));
        var result = ctx.replicationControl.deletePartitions(
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName("foo").setCount(1).setForce(true)));
        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
        assertEquals(0, removals());
    }

    @Test
    void testDuplicateStatusCannotAuthorizeRemoval() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(0, List.of(ready(), ready()));
        assertEquals(0, removals());
    }

    @Test
    void testExpiredHeartbeatRevokesPreviousReadiness() {
        ctx.replicationControl.replay(drain(1));
        heartbeat(ready());
        assertEquals(1, removals());
        int leader = ctx.replicationControl.getPartition(topicId, 1).leader;
        ctx.replicationControl.processExpiredBrokerHeartbeat(new BrokerHeartbeatRequestData()
            .setBrokerId(leader).setBrokerEpoch(ctx.currentBrokerEpoch(leader)).setCurrentMetadataOffset(1000));
        assertEquals(0, removals());
    }


    @Test
    void testRemovalWaitsForContinuousTailReadiness() {
        Uuid id = ctx.createTestTopic("bar", new int[][] {{0, 1}, {0, 1}, {0, 1}, {0, 1}}).topicId();
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(2).setDeadlineMs(1));
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(3).setDeadlineMs(1));
        PartitionRegistration lower = ctx.replicationControl.getPartition(id, 2);
        PartitionRegistration tail = ctx.replicationControl.getPartition(id, 3);
        PartitionDrainStatus lowerReady = new PartitionDrainStatus().setTopicId(id).setPartitionId(2)
            .setLeaderEpoch(lower.leaderEpoch).setPartitionEpoch(lower.partitionEpoch).setMetadataOffset(100).setReady(true);
        PartitionDrainStatus tailReady = new PartitionDrainStatus().setTopicId(id).setPartitionId(3)
            .setLeaderEpoch(tail.leaderEpoch).setPartitionEpoch(tail.partitionEpoch).setMetadataOffset(100).setReady(true);
        heartbeat(0, List.of(lowerReady, tailReady.duplicate().setReady(false).setBlockedReason("open transaction")));
        assertEquals(0, removals());
        heartbeat(0, List.of(lowerReady, tailReady));
        var result = ctx.replicationControl.maybeRemoveExpiredDrainingPartitions();
        assertEquals(List.of(3, 2), result.records().stream()
            .map(record -> ((RemovePartitionRecord) record.message()).partitionId()).toList());
        ctx.replay(result.records());
        assertNotNull(ctx.replicationControl.getPartition(id, 0));
        assertNotNull(ctx.replicationControl.getPartition(id, 1));
        assertNull(ctx.replicationControl.getPartition(id, 2));
        assertNull(ctx.replicationControl.getPartition(id, 3));
    }

    @Test
    void testForceUpgradeTargetsRetainedTailAfterPartialRemoval() {
        Uuid id = ctx.createTestTopic("bar", new int[][] {{0, 1}, {0, 1}, {0, 1}, {0, 1}}).topicId();
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(2).setDeadlineMs(1));
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(3).setDeadlineMs(1));
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(id).setPartitionId(2));
        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName("bar").setCount(2).setForce(true)));
        assertEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertEquals(1, result.records().size());
        PartitionDrainingRecord record = (PartitionDrainingRecord) result.records().get(0).message();
        assertEquals(3, record.partitionId());
        assertEquals(1, record.deadlineMs());
        assertTrue(record.force());
        ctx.replay(result.records());
        var removals = ctx.replicationControl.maybeRemoveExpiredDrainingPartitions();
        assertEquals(1, removals.records().size());
        ctx.replay(removals.records());
        assertNotNull(ctx.replicationControl.getPartition(id, 0));
        assertNotNull(ctx.replicationControl.getPartition(id, 1));
        assertNull(ctx.replicationControl.getPartition(id, 2));
        assertNull(ctx.replicationControl.getPartition(id, 3));
    }


    @Test
    void testDeletionRejectsMissingRetainedPrefixWithoutRecords() {
        Uuid id = ctx.createTestTopic("bar", new int[][] {{0, 1}, {0, 1}, {0, 1}, {0, 1}}).topicId();
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(id).setPartitionId(1));
        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName("bar").setCount(2).setForce(true)));
        assertEquals(Errors.INVALID_DELETE_PARTITION_COUNT.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
        assertEquals(0, removals());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testActiveOrUnexpiredTailBlocksLowerPartitionRemoval(boolean tailDraining) {
        Uuid id = ctx.createTestTopic("bar", new int[][] {{0, 1}, {0, 1}, {0, 1}, {0, 1}}).topicId();
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(2)
            .setDeadlineMs(1).setForce(true));
        if (tailDraining) {
            ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(3)
                .setDeadlineMs(Long.MAX_VALUE).setForce(true));
        }
        assertEquals(0, removals());
    }

    @Test
    void testBlockedTopicDoesNotPreventAnotherTopicRemoval() {
        ctx.replicationControl.replay(drain(1));
        Uuid id = ctx.createTestTopic("bar", new int[][] {{0, 1}, {0, 1}}).topicId();
        ctx.replicationControl.replay(new PartitionDrainingRecord().setTopicId(id).setPartitionId(1)
            .setDeadlineMs(1).setForce(true));
        var result = ctx.replicationControl.maybeRemoveExpiredDrainingPartitions();
        assertEquals(1, result.records().size());
        assertEquals(id, ((RemovePartitionRecord) result.records().get(0).message()).topicId());
    }

}
