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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder.TopicsInfo;
import org.apache.kafka.streams.processor.internals.TopologyMetadata.Subtopology;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamsChangelogPartitionDeletionReviewTest {
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private static final List<Node> BROKERS = List.of(BROKER);

    @Test
    void shrinkingOneSubtopologyKeepsTheOtherSubtopologysRestorePartitions() throws Exception {
        try (final MockAdminClient admin = new MockAdminClient(BROKERS, BROKER)) {
            addTopic(admin, "first-changelog", 4, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"));
            addTopic(admin, "second-changelog", 3, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, "compact"));
            final InternalTopicManager manager = manager(admin);
            final Subtopology first = new Subtopology(0, null);
            final Subtopology second = new Subtopology(1, null);
            final Map<Subtopology, TopicsInfo> groups = Map.of(
                first, topicsInfo("first-source", "first-changelog"),
                second, topicsInfo("second-source", "second-changelog"));
            final ChangelogTopics before = new ChangelogTopics(manager, groups, Map.of(
                first, Set.of(new TaskId(0, 0), new TaskId(0, 1), new TaskId(0, 2), new TaskId(0, 3)),
                second, Set.of(new TaskId(1, 0), new TaskId(1, 1), new TaskId(1, 2))), "[review] ");
            before.setup();

            final ChangelogTopics after = new ChangelogTopics(manager, groups, Map.of(
                first, Set.of(new TaskId(0, 0)),
                second, Set.of(new TaskId(1, 0), new TaskId(1, 1), new TaskId(1, 2))), "[review] ");
            after.setup();

            assertEquals(Set.of(new TaskId(0, 0), new TaskId(1, 0), new TaskId(1, 1), new TaskId(1, 2)),
                after.statefulTaskIds());
            assertEquals(Set.of(new TopicPartition("first-changelog", 0), new TopicPartition("second-changelog", 0),
                new TopicPartition("second-changelog", 1), new TopicPartition("second-changelog", 2)),
                after.preExistingNonSourceTopicBasedPartitions());
            assertTrue(after.preExistingPartitionsFor(new TaskId(0, 3)).isEmpty());
            assertEquals(4, admin.describeTopics(List.of("first-changelog")).allTopicNames().get()
                .get("first-changelog").partitions().size());
            assertEquals(3, admin.describeTopics(List.of("second-changelog")).allTopicNames().get()
                .get("second-changelog").partitions().size());
        }
    }

    enum StoreKind {
        UNWINDOWED, WINDOWED, VERSIONED
    }

    @ParameterizedTest
    @EnumSource(StoreKind.class)
    void surplusChangelogPartitionsMustNotSuppressUnsafeRetentionConfiguration(final StoreKind kind) {
        try (final MockAdminClient admin = new MockAdminClient(BROKERS, BROKER)) {
            final InternalTopicConfig config;
            final Map<String, String> brokerConfig = new HashMap<>();
            final String unsafeProperty;
            switch (kind) {
                case UNWINDOWED:
                    config = new UnwindowedUnversionedChangelogTopicConfig("changelog", Map.of());
                    brokerConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
                    unsafeProperty = TopicConfig.CLEANUP_POLICY_CONFIG;
                    break;
                case WINDOWED:
                    config = new WindowedChangelogTopicConfig("changelog", Map.of(), 1000L);
                    brokerConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact,delete");
                    brokerConfig.put(TopicConfig.RETENTION_MS_CONFIG, "999");
                    brokerConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, null);
                    unsafeProperty = TopicConfig.RETENTION_MS_CONFIG;
                    break;
                case VERSIONED:
                    config = new VersionedChangelogTopicConfig("changelog", Map.of(), 1000L);
                    brokerConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, "compact");
                    brokerConfig.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
                    unsafeProperty = TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG;
                    break;
                default:
                    throw new AssertionError(kind);
            }
            config.setNumberOfPartitions(1);
            addTopic(admin, "changelog", 3, brokerConfig);

            final InternalTopicManager.ValidationResult validation = manager(admin).validate(Map.of("changelog", config));

            assertTrue(validation.missingTopics().isEmpty());
            assertEquals(Set.of("changelog"), validation.misconfigurationsForTopics().keySet());
            final List<String> problems = validation.misconfigurationsForTopics().get("changelog");
            assertEquals(1, problems.size());
            assertTrue(problems.get(0).contains(unsafeProperty));
        }
    }

    private static TopicsInfo topicsInfo(final String source, final String changelog) {
        return new TopicsInfo(Set.of(), Set.of(source), Map.of(),
            Map.of(changelog, new UnwindowedUnversionedChangelogTopicConfig(changelog, Map.of())));
    }

    private static InternalTopicManager manager(final MockAdminClient admin) {
        return new InternalTopicManager(new MockTime(), admin, new StreamsConfig(Map.of(
            StreamsConfig.APPLICATION_ID_CONFIG, "deletion-review-app",
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")));
    }

    private static void addTopic(final MockAdminClient admin, final String topic, final int partitions,
                                 final Map<String, String> configs) {
        admin.addTopic(false, topic, IntStream.range(0, partitions)
            .mapToObj(partition -> new TopicPartitionInfo(partition, BROKER, BROKERS, BROKERS))
            .collect(Collectors.toList()), configs);
    }
}
