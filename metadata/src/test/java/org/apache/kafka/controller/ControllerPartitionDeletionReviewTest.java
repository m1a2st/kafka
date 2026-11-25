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

import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignablePartition;
import org.apache.kafka.common.message.AlterPartitionReassignmentsRequestData.ReassignableTopic;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.PartitionDrainStatus;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.DeletePartitionsRequestData.DeletePartitionsTopic;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.controller.ReplicationControlManagerTest.ReplicationControlTestContext;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RecordListWriter;
import org.apache.kafka.metadata.KafkaConfigSchema;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextFor;
import static org.apache.kafka.controller.ControllerRequestContextUtil.anonymousContextWithMutationQuotaExceededFor;
import static org.apache.kafka.metadata.LeaderConstants.NO_LEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControllerPartitionDeletionReviewTest {
    private static final String NAME = "controller-deletion-review";
    private final ReplicationControlTestContext ctx = new ReplicationControlTestContext.Builder().build();
    private final Uuid topicId;

    ControllerPartitionDeletionReviewTest() {
        ctx.registerBrokers(0, 1, 2);
        ctx.unfenceBrokers(0, 1, 2);
        topicId = ctx.createTestTopic(NAME, new int[][] {{0, 1}, {0, 1}, {0, 1}, {0, 1}}).topicId();
    }

    private PartitionDrainingRecord drain(int partitionId, boolean force) {
        return new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(partitionId).setDeadlineMs(1L).setForce(force);
    }

    private PartitionDrainStatus ready(ReplicationControlTestContext context, int partitionId, long metadataOffset) {
        PartitionRegistration partition = context.replicationControl.getPartition(topicId, partitionId);
        return new PartitionDrainStatus().setTopicId(topicId).setPartitionId(partitionId)
            .setLeaderEpoch(partition.leaderEpoch).setPartitionEpoch(partition.partitionEpoch)
            .setMetadataOffset(metadataOffset).setReady(true);
    }

    private void heartbeat(ReplicationControlTestContext context, long appliedOffset, PartitionDrainStatus status) {
        int brokerId = context.replicationControl.getPartition(topicId, status.partitionId()).leader;
        long brokerEpoch = context.currentBrokerEpoch(brokerId);
        context.clusterControl.trackBrokerHeartbeat(brokerId, brokerEpoch);
        context.replay(context.replicationControl.processBrokerHeartbeat(new BrokerHeartbeatRequestData()
            .setBrokerId(brokerId).setBrokerEpoch(brokerEpoch).setCurrentMetadataOffset(appliedOffset)
            .setDrainingPartitions(List.of(status)), 0L).records());
    }

    @Test
    void testReportBeyondHeartbeatAppliedOffsetCannotAuthorizeDeletion() {
        ctx.replicationControl.replay(drain(3, false), 100L);
        heartbeat(ctx, 100L, ready(ctx, 3, 101L));
        assertTrue(ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().isEmpty());

        heartbeat(ctx, 101L, ready(ctx, 3, 101L));
        assertEquals(1, ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
    }

    @Test
    void testRestoredDrainNeedsReportCoveringSnapshotBoundary() {
        TopicImage image = new TopicImage(NAME, topicId, Map.of(
            0, ctx.replicationControl.getPartition(topicId, 0),
            1, ctx.replicationControl.getPartition(topicId, 1),
            2, ctx.replicationControl.getPartition(topicId, 2),
            3, ctx.replicationControl.getPartition(topicId, 3)), Map.of(3, 1L));
        RecordListWriter writer = new RecordListWriter();
        image.write(writer, new ImageWriterOptions.Builder(MetadataVersion.IBP_4_4_IV2).build());
        ReplicationControlTestContext restored = new ReplicationControlTestContext.Builder().build();
        restored.registerBrokers(0, 1, 2);
        restored.unfenceBrokers(0, 1, 2);
        for (var record : writer.records()) {
            if (record.message() instanceof PartitionDrainingRecord draining) {
                restored.replicationControl.replay(draining, 500L);
            } else {
                restored.replay(List.of(record));
            }
        }

        heartbeat(restored, 1000L, ready(restored, 3, 499L));
        assertTrue(restored.replicationControl.maybeRemoveExpiredDrainingPartitions().records().isEmpty());
        heartbeat(restored, 1000L, ready(restored, 3, 500L));
        assertEquals(1, restored.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
    }

    @Test
    void testRejectedReopenDoesNotCancelForcedDrain() {
        ctx.replicationControl.replay(drain(3, true));

        var result = ctx.replicationControl.createPartitions(
            anonymousContextWithMutationQuotaExceededFor(ApiKeys.CREATE_PARTITIONS),
            List.of(new CreatePartitionsTopic().setName(NAME).setCount(4).setAssignments(null)));

        assertEquals(Errors.THROTTLING_QUOTA_EXCEEDED.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
        assertEquals(1, ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
    }

    @Test
    void testPartialReopenPreservesRemovedIdFence() {
        ctx.replicationControl.replay(drain(2, true));
        ctx.replicationControl.replay(drain(3, true));
        ctx.replicationControl.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(3));
        PartitionRegistration retainedLog = ctx.replicationControl.getPartition(topicId, 2);

        var reopen = ctx.replicationControl.createPartitions(anonymousContextFor(ApiKeys.CREATE_PARTITIONS),
            List.of(new CreatePartitionsTopic().setName(NAME).setCount(3).setAssignments(null)));
        assertEquals(Errors.NONE.code(), reopen.response().get(0).errorCode());
        assertEquals(1, reopen.records().size());
        ctx.replay(reopen.records());
        assertEquals(retainedLog, ctx.replicationControl.getPartition(topicId, 2));
        assertTrue(ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().isEmpty());

        var reuse = ctx.replicationControl.createPartitions(anonymousContextFor(ApiKeys.CREATE_PARTITIONS),
            List.of(new CreatePartitionsTopic().setName(NAME).setCount(4).setAssignments(null)));
        assertEquals(Errors.INVALID_PARTITIONS.code(), reuse.response().get(0).errorCode());
        assertTrue(reuse.records().isEmpty());
    }

    @Test
    void testDrainRejectsTargetedReassignmentAndAllowsRetainedPartitionReassignment() {
        ctx.replicationControl.replay(drain(3, false));
        var result = ctx.replicationControl.alterPartitionReassignments(
            new AlterPartitionReassignmentsRequestData().setTopics(List.of(new ReassignableTopic().setName(NAME)
                .setPartitions(List.of(
                    new ReassignablePartition().setPartitionIndex(0).setReplicas(List.of(1, 2)),
                    new ReassignablePartition().setPartitionIndex(3).setReplicas(List.of(1, 2)))))));

        assertEquals(Errors.NONE.code(), result.response().responses().get(0).partitions().get(0).errorCode());
        assertEquals(Errors.PARTITION_OPERATION_IN_PROGRESS.code(),
            result.response().responses().get(0).partitions().get(1).errorCode());
        assertFalse(result.records().isEmpty());
        ctx.replay(result.records());
        assertTrue(ctx.replicationControl.getPartition(topicId, 0).addingReplicas.length > 0);
        assertEquals(0, ctx.replicationControl.getPartition(topicId, 3).addingReplicas.length);
        assertTrue(ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().isEmpty());
    }

    @Test
    void testDeleteBeforeMetadataVersionGateProducesNoRecords() {
        ReplicationControlTestContext older = new ReplicationControlTestContext.Builder()
            .setMetadataVersion(MetadataVersion.IBP_4_4_IV1).build();
        older.registerBrokers(0);
        older.unfenceBrokers(0);
        older.createTestTopic(NAME, new int[][] {{0}, {0}});

        var result = older.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName(NAME).setCount(1).setForce(true)));

        assertEquals(Errors.UNSUPPORTED_VERSION.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty());
    }

    @Test
    void testMetadataDowngradeCannotDiscardDrainState() {
        ctx.replicationControl.replay(drain(3, true));
        var result = ctx.featureControl.updateFeatures(
            Map.of(MetadataVersion.FEATURE_NAME, MetadataVersion.IBP_4_4_IV1.featureLevel()),
            Map.of(MetadataVersion.FEATURE_NAME, FeatureUpdate.UpgradeType.SAFE_DOWNGRADE), false, 0);

        assertEquals(Errors.INVALID_UPDATE_VERSION, result.response().error());
        assertTrue(result.records().isEmpty());
        assertEquals(MetadataVersion.IBP_4_4_IV2, ctx.featureControl.metadataVersionOrThrow());
        assertEquals(1, ctx.replicationControl.maybeRemoveExpiredDrainingPartitions().records().size());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMaximumDrainTimeoutCannotBecomeCancellation(boolean force) {
        ConfigDef topicConfigs = new ConfigDef().define(TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG, Long.MAX_VALUE, ConfigDef.Range.atLeast(0), ConfigDef.Importance.MEDIUM, "");
        ConfigurationControlManager configurations = new ConfigurationControlManager.Builder()
            .setSnapshotRegistry(ctx.snapshotRegistry).setFeatureControl(ctx.featureControl)
            .setKafkaConfigSchema(new KafkaConfigSchema(Map.of(ConfigResource.Type.TOPIC, topicConfigs), Map.of())).build();
        ReplicationControlManager replication = new ReplicationControlManager.Builder()
            .setSnapshotRegistry(ctx.snapshotRegistry).setConfigurationControl(configurations)
            .setClusterControl(ctx.clusterControl).setFeatureControl(ctx.featureControl).build();
        replication.replay(new TopicRecord().setName(NAME).setTopicId(topicId));
        for (int id = 0; id < 2; id++) {
            replication.replay(new PartitionRecord().setTopicId(topicId).setPartitionId(id)
                .setReplicas(List.of(0)).setIsr(List.of(0)).setLeader(0));
        }
        long before = System.currentTimeMillis();

        var result = replication.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName(NAME).setCount(1).setForce(force)));

        assertEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertEquals(1, result.records().size());
        PartitionDrainingRecord accepted = (PartitionDrainingRecord) result.records().get(0).message();
        assertTrue(accepted.deadlineMs() >= before,
            "A valid nonnegative timeout must not overflow into a negative cancellation record");
        assertEquals(Long.MAX_VALUE, accepted.deadlineMs());
        assertEquals(force, accepted.force());
        replication.replay(accepted);
        assertNotNull(replication.getPartition(topicId, 1));
        assertTrue(replication.maybeRemoveExpiredDrainingPartitions().records().isEmpty());
    }

    @Test
    void testFailedOversizedDeletionProducesNoPartialDrainRecords() {
        String largeTopic = "oversized-deletion-review";
        Uuid largeTopicId = Uuid.randomUuid();
        ctx.replicationControl.replay(new TopicRecord().setName(largeTopic).setTopicId(largeTopicId));
        // This is valid existing topic state: partitions may have been added in several requests.
        for (int id = 0; id < QuorumController.MAX_RECORDS_PER_USER_OP + 2; id++) {
            ctx.replicationControl.replay(new PartitionRecord().setTopicId(largeTopicId).setPartitionId(id)
                .setReplicas(List.of(0)).setIsr(List.of(0)).setLeader(0));
        }

        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName(largeTopic).setCount(1)));

        assertNotEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertTrue(result.records().isEmpty(),
            "A failed topic operation must not commit a partial non-tail drain and fence thousands of partitions");
    }

    @Test
    void testForceRemovalRemainsAllowedWhenAllReplicasAreFenced() {
        ctx.replicationControl.replay(drain(3, true));
        ctx.fenceBrokers(0, 1, 2);
        assertEquals(NO_LEADER, ctx.replicationControl.getPartition(topicId, 3).leader);

        var result = ctx.replicationControl.maybeRemoveExpiredDrainingPartitions();

        assertEquals(1, result.records().size());
        RemovePartitionRecord removal = (RemovePartitionRecord) result.records().get(0).message();
        assertEquals(topicId, removal.topicId());
        assertEquals(3, removal.partitionId());
    }

    @Test
    void testRejectedOversizedTopicPreservesEarlierSuccessfulTopicRecords() {
        String largeTopic = "oversized-after-success-review";
        Uuid largeTopicId = Uuid.randomUuid();
        ctx.replicationControl.replay(new TopicRecord().setName(largeTopic).setTopicId(largeTopicId));
        for (int id = 0; id < QuorumController.MAX_RECORDS_PER_USER_OP + 1; id++) {
            ctx.replicationControl.replay(new PartitionRecord().setTopicId(largeTopicId).setPartitionId(id)
                .setReplicas(List.of(0)).setIsr(List.of(0)).setLeader(0));
        }

        var result = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName(NAME).setCount(3),
                new DeletePartitionsTopic().setName(largeTopic).setCount(1)));

        assertEquals(2, result.response().size());
        assertEquals(NAME, result.response().get(0).name());
        assertEquals(Errors.NONE.code(), result.response().get(0).errorCode());
        assertEquals(largeTopic, result.response().get(1).name());
        assertNotEquals(Errors.NONE.code(), result.response().get(1).errorCode());
        assertEquals(1, result.records().size(),
            "Rejecting one topic must preserve earlier accepted records without retaining rejected topic records");
        PartitionDrainingRecord accepted = (PartitionDrainingRecord) result.records().get(0).message();
        assertEquals(topicId, accepted.topicId());
        assertEquals(3, accepted.partitionId());

        var standalone = ctx.replicationControl.deletePartitions(anonymousContextFor(ApiKeys.DELETE_PARTITIONS),
            List.of(new DeletePartitionsTopic().setName(largeTopic).setCount(1)));
        assertEquals(Errors.NONE.code(), standalone.response().get(0).errorCode(),
            "The rejected topic must fit the record limit when it is requested alone");
        assertEquals(QuorumController.MAX_RECORDS_PER_USER_OP, standalone.records().size());
    }
}
