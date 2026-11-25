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

package org.apache.kafka.controller.metrics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.metadata.RemoveTopicRecord;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.image.AclsImage;
import org.apache.kafka.image.ClientQuotasImage;
import org.apache.kafka.image.ClusterImage;
import org.apache.kafka.image.ConfigurationsImage;
import org.apache.kafka.image.DelegationTokenImage;
import org.apache.kafka.image.FeaturesImage;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.ProducerIdsImage;
import org.apache.kafka.image.ScramImage;
import org.apache.kafka.image.TopicsImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.loader.LogDeltaManifest;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.writer.ImageReWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.fault.MockFaultHandler;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.NON_PREFERRED_LEADER;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.NORMAL;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.FakePartitionRegistrationType.OFFLINE;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.fakePartitionRegistration;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.fakeTopicImage;
import static org.apache.kafka.controller.metrics.ControllerMetricsTestUtils.fakeTopicsImage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ControllerMetadataMetricsPublisherTest {
    static class TestEnv implements AutoCloseable {
        MockFaultHandler faultHandler =
                new MockFaultHandler("ControllerMetadataMetricsPublisher");
        MetricsRegistry registry = new MetricsRegistry();
        ControllerMetadataMetrics metrics =
                new ControllerMetadataMetrics(Optional.of(registry));
        ControllerMetadataMetricsPublisher publisher;

        TestEnv() {
            this(0);
        }

        TestEnv(int nodeId) {
            publisher = new ControllerMetadataMetricsPublisher(nodeId, metrics, faultHandler);
            publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(nodeId), 1));
        }

        @Override
        public void close() {
            try {
                publisher.close();
                faultHandler.maybeRethrowFirstException();
            } finally {
                registry.shutdown();
            }
        }
    }

    @Test
    public void testMetricsBeforePublishing() {
        try (TestEnv env = new TestEnv()) {
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(0, env.metrics.globalTopicCount());
            assertEquals(0, env.metrics.globalPartitionCount());
            assertEquals(0, env.metrics.offlinePartitionCount());
            assertEquals(0, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }

    static MetadataImage fakeImageFromTopicsImage(TopicsImage topicsImage) {
        return new MetadataImage(
            MetadataProvenance.EMPTY,
            FeaturesImage.EMPTY,
            ClusterImage.EMPTY,
            topicsImage,
            ConfigurationsImage.EMPTY,
            ClientQuotasImage.EMPTY,
            ProducerIdsImage.EMPTY,
            AclsImage.EMPTY,
            ScramImage.EMPTY,
            DelegationTokenImage.EMPTY);
    }

    static final TopicsImage TOPICS_IMAGE1;

    static final MetadataImage IMAGE1;

    static {
        TOPICS_IMAGE1 = fakeTopicsImage(
            fakeTopicImage("foo",
                Uuid.fromString("JKNp6fQaT-icHxh654ok-w"),
                    fakePartitionRegistration(NORMAL)),
            fakeTopicImage("bar",
                Uuid.fromString("pEMSdUVWTXaFQUzLTznFSw"),
                    fakePartitionRegistration(NORMAL),
                    fakePartitionRegistration(NORMAL),
                    fakePartitionRegistration(NON_PREFERRED_LEADER)),
            fakeTopicImage("quux",
                    Uuid.fromString("zkUT4lyyRke6VIaTw6RQWg"),
                    fakePartitionRegistration(OFFLINE),
                    fakePartitionRegistration(OFFLINE),
                    fakePartitionRegistration(OFFLINE))
        );
        IMAGE1 = fakeImageFromTopicsImage(TOPICS_IMAGE1);
    }

    @Test
    public void testPublish() {
        try (TestEnv env = new TestEnv()) {
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(0, env.metrics.globalTopicCount());
            assertEquals(0, env.metrics.globalPartitionCount());
            assertEquals(0, env.metrics.offlinePartitionCount());
            assertEquals(0, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }

    static LoaderManifest fakeManifest(boolean isSnapshot) {
        if (isSnapshot) {
            return new SnapshotManifest(MetadataProvenance.EMPTY, 0);
        } else {
            return LogDeltaManifest.newBuilder()
                .provenance(MetadataProvenance.EMPTY)
                .leaderAndEpoch(LeaderAndEpoch.UNKNOWN)
                .numBatches(0)
                .elapsedNs(0)
                .numBytes(0).build();
        }
    }

    @Test
    public void testLoadSnapshot() {
        try (TestEnv env = new TestEnv()) {
            MetadataDelta delta = new MetadataDelta.Builder()
                .setImage(MetadataImage.EMPTY)
                .build();
            ImageReWriter writer = new ImageReWriter(delta);
            IMAGE1.write(writer, new ImageWriterOptions.Builder(MetadataVersion.MINIMUM_VERSION).build());
            env.publisher.onMetadataUpdate(delta, IMAGE1, fakeManifest(true));
            assertEquals(0, env.metrics.activeBrokerCount());
            assertEquals(3, env.metrics.globalTopicCount());
            assertEquals(7, env.metrics.globalPartitionCount());
            assertEquals(3, env.metrics.offlinePartitionCount());
            assertEquals(4, env.metrics.preferredReplicaImbalanceCount());
            assertEquals(0, env.metrics.metadataErrorCount());
        }
    }
    @Test
    public void testDrainingMetricsAfterSnapshotResumeAndRemoval() {
        try (TestEnv env = new TestEnv()) {
            Uuid topicId = TOPICS_IMAGE1.getTopic("bar").id();
            MetadataDelta draining = new MetadataDelta.Builder().setImage(IMAGE1).build();
            draining.replay(new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(2).setDeadlineMs(1000));
            MetadataImage drainingImage = draining.apply(MetadataProvenance.EMPTY);
            env.publisher.onMetadataUpdate(draining, drainingImage, fakeManifest(true));
            assertEquals(1, env.metrics.drainingPartitionCount());

            MetadataDelta resume = new MetadataDelta.Builder().setImage(drainingImage).build();
            resume.replay(new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(2).setDeadlineMs(-1));
            MetadataImage resumedImage = resume.apply(MetadataProvenance.EMPTY);
            env.publisher.onMetadataUpdate(resume, resumedImage, fakeManifest(false));
            assertEquals(0, env.metrics.drainingPartitionCount());
            assertEquals(7, env.metrics.globalPartitionCount());

            MetadataDelta removed = new MetadataDelta.Builder().setImage(resumedImage).build();
            removed.replay(new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(2).setDeadlineMs(2000));
            removed.replay(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(2));
            env.publisher.onMetadataUpdate(removed, removed.apply(MetadataProvenance.EMPTY), fakeManifest(false));
            assertEquals(0, env.metrics.drainingPartitionCount());
            assertEquals(6, env.metrics.globalPartitionCount());
        }
    }

    private static final Uuid BAR_ID = TOPICS_IMAGE1.getTopic("bar").id();

    private static MetadataImage drainingImage(int... partitionIds) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(IMAGE1).build();
        for (int partitionId : partitionIds) {
            delta.replay(drainRecord(partitionId, 1000));
        }
        return delta.apply(MetadataProvenance.EMPTY);
    }

    private static PartitionDrainingRecord drainRecord(int partitionId, long deadlineMs) {
        return new PartitionDrainingRecord().setTopicId(BAR_ID).setPartitionId(partitionId).setDeadlineMs(deadlineMs);
    }

    private static RemovePartitionRecord removeRecord(int partitionId) {
        return new RemovePartitionRecord().setTopicId(BAR_ID).setPartitionId(partitionId);
    }

    private static void publishSnapshot(TestEnv env, MetadataImage image) {
        env.publisher.onMetadataUpdate(new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build(),
                image, fakeManifest(true));
    }

    private static MetadataImage publishLog(TestEnv env, MetadataImage image, ApiMessage... records) {
        MetadataDelta delta = new MetadataDelta.Builder().setImage(image).build();
        for (ApiMessage record : records) {
            delta.replay(record);
        }
        MetadataImage newImage = delta.apply(MetadataProvenance.EMPTY);
        env.publisher.onMetadataUpdate(delta, newImage, fakeManifest(false));
        return newImage;
    }

    private static MetricName drainMetricName(String name) {
        return new MetricName("kafka.controller", "KafkaController", name, null,
                "kafka.controller:type=KafkaController,name=" + name);
    }

    private static long completedDrains(TestEnv env) {
        Meter meter = (Meter) env.registry.allMetrics().get(drainMetricName("DrainCompletedTotal"));
        assertNotNull(meter, "DrainCompletedTotal must be registered");
        return meter.count();
    }

    @SuppressWarnings("unchecked")
    private static void assertDrainGauges(TestEnv env, int expected) {
        assertEquals(expected, env.metrics.drainingPartitionCount());
        for (String name : new String[] {"DrainingPartitionCount", "PendingDrainRemovals"}) {
            Gauge<Integer> gauge = (Gauge<Integer>) env.registry.allMetrics().get(drainMetricName(name));
            assertNotNull(gauge, name + " must be registered");
            assertEquals(expected, gauge.value(), name);
        }
    }

    @Test
    public void testDrainGaugesWaitForKnownLocalLeadership() {
        try (TestEnv env = new TestEnv()) {
            env.publisher.onControllerChange(LeaderAndEpoch.UNKNOWN);
            publishSnapshot(env, drainingImage(2));
            assertDrainGauges(env, 0);
            assertEquals(7, env.metrics.globalPartitionCount());
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 2));
            assertDrainGauges(env, 1);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.empty(), 3));
            assertDrainGauges(env, 0);
        }
    }

    @Test
    public void testDrainGaugesUseConfiguredNonzeroControllerId() {
        try (TestEnv env = new TestEnv(42)) {
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 2));
            publishSnapshot(env, drainingImage(1, 2));
            assertDrainGauges(env, 0);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(42), 3));
            assertDrainGauges(env, 2);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.empty(), 4));
            assertDrainGauges(env, 0);
        }
    }

    @Test
    public void testStandbyDrainGaugesRetainMetadataAcrossLeadershipChanges() {
        try (TestEnv env = new TestEnv()) {
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 2));
            publishSnapshot(env, IMAGE1);
            MetadataImage image = publishLog(env, IMAGE1, drainRecord(2, 1000));
            assertDrainGauges(env, 0);
            assertEquals(7, env.metrics.globalPartitionCount());
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 3));
            assertDrainGauges(env, 1);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 4));
            assertDrainGauges(env, 1);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 5));
            image = publishLog(env, image, drainRecord(1, 2000));
            assertDrainGauges(env, 0);
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 6));
            assertDrainGauges(env, 2);
            assertEquals(7, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testCancelAndRemoveDrainsHaveDifferentCompletionEffects() {
        try (TestEnv env = new TestEnv()) {
            MetadataImage image = drainingImage(1, 2);
            publishSnapshot(env, image);
            assertDrainGauges(env, 2);
            assertEquals(0L, completedDrains(env));
            image = publishLog(env, image, drainRecord(2, -1));
            assertDrainGauges(env, 1);
            assertEquals(0L, completedDrains(env));
            assertEquals(7, env.metrics.globalPartitionCount());
            publishLog(env, image, removeRecord(1));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
            assertEquals(6, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testCoalescedDrainAndRemoveRecordsCountEachCompletion() {
        try (TestEnv env = new TestEnv()) {
            publishSnapshot(env, IMAGE1);
            MetadataImage image = publishLog(env, IMAGE1, drainRecord(2, 1000), removeRecord(2));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
            image = publishLog(env, image, drainRecord(1, 2000), drainRecord(1, -1));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
            publishLog(env, image, drainRecord(1, 3000).setForce(true), removeRecord(1),
                    drainRecord(0, 3000), removeRecord(0));
            assertDrainGauges(env, 0);
            assertEquals(3L, completedDrains(env));
            assertEquals(4, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testRepeatedRemovalOfMissingPartitionDoesNotCountAgain() {
        try (TestEnv env = new TestEnv()) {
            MetadataImage image = drainingImage(2);
            publishSnapshot(env, image);
            image = publishLog(env, image, removeRecord(2), removeRecord(2), removeRecord(99));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
            assertEquals(6, env.metrics.globalPartitionCount());
            publishLog(env, image, removeRecord(2));
            assertEquals(1L, completedDrains(env));
            assertEquals(6, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testWholeTopicDeletionClearsPendingWithoutCompletingIndividualDrains() {
        try (TestEnv env = new TestEnv()) {
            MetadataImage image = drainingImage(1, 2);
            publishSnapshot(env, image);
            publishLog(env, image, new RemoveTopicRecord().setTopicId(BAR_ID));
            assertDrainGauges(env, 0);
            assertEquals(0L, completedDrains(env));
            assertEquals(2, env.metrics.globalTopicCount());
            assertEquals(4, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testCompletionSurvivesCoalescedWholeTopicDeletion() {
        try (TestEnv env = new TestEnv()) {
            MetadataImage image = drainingImage(2);
            publishSnapshot(env, image);
            publishLog(env, image, removeRecord(2), new RemoveTopicRecord().setTopicId(BAR_ID));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
            assertEquals(4, env.metrics.globalPartitionCount());
        }
    }

    @Test
    public void testSnapshotsInitializeGaugesWithoutCompletingOrResettingDrains() {
        MetadataImage removedImage;
        try (TestEnv env = new TestEnv()) {
            MetadataImage image = drainingImage(2);
            publishSnapshot(env, image);
            publishSnapshot(env, IMAGE1);
            assertDrainGauges(env, 0);
            assertEquals(0L, completedDrains(env));
            publishSnapshot(env, image);
            removedImage = publishLog(env, image, removeRecord(2));
            assertEquals(1L, completedDrains(env));
            publishSnapshot(env, removedImage);
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
        }
        try (TestEnv restarted = new TestEnv()) {
            publishSnapshot(restarted, removedImage);
            assertDrainGauges(restarted, 0);
            assertEquals(0L, completedDrains(restarted));
        }
    }

    @Test
    public void testStandbyCompletionsAreNotBackfilledOnActivation() {
        try (TestEnv env = new TestEnv()) {
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 2));
            MetadataImage image = drainingImage(2);
            publishSnapshot(env, image);
            image = publishLog(env, image, removeRecord(2));
            assertDrainGauges(env, 0);
            assertEquals(0L, completedDrains(env));
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 3));
            assertDrainGauges(env, 0);
            assertEquals(0L, completedDrains(env));
            image = publishLog(env, image, drainRecord(1, 2000), removeRecord(1));
            assertEquals(1L, completedDrains(env));
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(1), 4));
            publishLog(env, image, drainRecord(0, 3000), removeRecord(0));
            assertEquals(1L, completedDrains(env));
            env.publisher.onControllerChange(new LeaderAndEpoch(OptionalInt.of(0), 5));
            assertDrainGauges(env, 0);
            assertEquals(1L, completedDrains(env));
        }
    }

}
