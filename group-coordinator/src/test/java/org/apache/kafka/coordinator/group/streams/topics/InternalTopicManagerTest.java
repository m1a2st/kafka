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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatResponse.Status;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.coordinator.common.runtime.KRaftCoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InternalTopicManagerTest {

    private static final Logger LOG = LoggerFactory.getLogger(InternalTopicManagerTest.class);
    public static final MockTime TIME = new MockTime();
    public static final String SOURCE_TOPIC_1 = "source_topic1";
    public static final String SOURCE_TOPIC_2 = "source_topic2";
    public static final String REPARTITION_TOPIC = "repartition_topic";
    public static final String STATE_CHANGELOG_TOPIC_1 = "state_changelog_topic1";
    public static final String STATE_CHANGELOG_TOPIC_2 = "state_changelog_topic2";
    public static final String SUBTOPOLOGY_1 = "subtopology1";
    public static final String SUBTOPOLOGY_2 = "subtopology2";
    public static final String CONFIG_KEY = "cleanup.policy";
    public static final String CONFIG_VALUE = "compact";

    @Test
    void testConfigureTopicsSetsConfigurationExceptionWhenSourceTopicIsMissing() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .build();
        // SOURCE_TOPIC_2 is missing from topicMetadata
        StreamsTopology topology = makeTestTopology();

        final ConfiguredTopology configuredTopology = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0, topology, new KRaftCoordinatorMetadataImage(metadataImage), TIME);

        assertEquals(Optional.empty(), configuredTopology.subtopologies());
        assertTrue(configuredTopology.topicConfigurationException().isPresent());
        assertEquals(Status.MISSING_SOURCE_TOPICS, configuredTopology.topicConfigurationException().get().status());
        assertEquals(String.format("Source topics %s are missing.", SOURCE_TOPIC_2), configuredTopology.topicConfigurationException().get().getMessage());
    }

    @Test
    void testConfigureTopics() {
        MetadataImage metadataImage = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_2, 2)
            .build();
        StreamsTopology topology = makeTestTopology();

        ConfiguredTopology configuredTopology = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0, topology, new KRaftCoordinatorMetadataImage(metadataImage), TIME);
        final Map<String, CreatableTopic> internalTopicsToBeCreated = configuredTopology.internalTopicsToBeCreated();

        assertEquals(2, internalTopicsToBeCreated.size());
        assertEquals(
            new CreatableTopic()
                .setName(REPARTITION_TOPIC)
                .setNumPartitions(2)
                .setReplicationFactor((short) 3),
            internalTopicsToBeCreated.get(REPARTITION_TOPIC)
        );
        assertEquals(
            new CreatableTopic()
                .setName(STATE_CHANGELOG_TOPIC_1)
                .setNumPartitions(2)
                .setReplicationFactor((short) -1)
                .setConfigs(
                    new CreatableTopicConfigCollection(
                        List.of(new CreatableTopicConfig().setName(CONFIG_KEY).setValue(CONFIG_VALUE)).iterator())
                ),
            internalTopicsToBeCreated.get(STATE_CHANGELOG_TOPIC_1));

        Optional<Map<String, ConfiguredSubtopology>> expectedConfiguredTopology = Optional.of(makeExpectedConfiguredSubtopologies());
        assertEquals(expectedConfiguredTopology, configuredTopology.subtopologies());
    }

    @Test
    void testRetainedChangelogPartitionsAfterSourceShrink() {
        final Uuid sourceId = Uuid.randomUuid();
        final MetadataImage initialImage = new MetadataImageBuilder()
            .addTopic(sourceId, SOURCE_TOPIC_1, 3)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 3)
            .build();
        final Subtopology subtopology = new Subtopology().setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(List.of(SOURCE_TOPIC_1))
            .setStateChangelogTopics(List.of(new StreamsGroupTopologyValue.TopicInfo().setName(STATE_CHANGELOG_TOPIC_1)));
        final StreamsTopology topology = new StreamsTopology(1, Map.of(SUBTOPOLOGY_1, subtopology));
        final ConfiguredTopology beforeShrink = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0,
            topology, new KRaftCoordinatorMetadataImage(initialImage), TIME);
        assertTrue(beforeShrink.isReady());
        assertEquals(3, beforeShrink.subtopologies().orElseThrow().get(SUBTOPOLOGY_1).numberOfTasks());

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(initialImage).build();
        delta.replay(new RemovePartitionRecord().setTopicId(sourceId).setPartitionId(2));
        final MetadataImage shrunkImage = delta.apply(MetadataProvenance.EMPTY);
        final ConfiguredTopology afterShrink = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 1,
            topology, new KRaftCoordinatorMetadataImage(shrunkImage), TIME);

        assertEquals(Optional.empty(), afterShrink.topicConfigurationException());
        final ConfiguredSubtopology configured = afterShrink.subtopologies().orElseThrow().get(SUBTOPOLOGY_1);
        assertEquals(2, configured.numberOfTasks());
        assertEquals(2, configured.stateChangelogTopics().get(STATE_CHANGELOG_TOPIC_1).numberOfPartitions());
        assertTrue(afterShrink.internalTopicsToBeCreated().isEmpty());
        assertEquals(3, shrunkImage.topics().getTopic(STATE_CHANGELOG_TOPIC_1).partitions().size());
    }

    @Test
    void testUnderprovisionedChangelogStillFails() {
        final MetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 1)
            .build();
        final Subtopology subtopology = new Subtopology().setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(List.of(SOURCE_TOPIC_1))
            .setStateChangelogTopics(List.of(new StreamsGroupTopologyValue.TopicInfo().setName(STATE_CHANGELOG_TOPIC_1)));
        final StreamsTopology topology = new StreamsTopology(1, Map.of(SUBTOPOLOGY_1, subtopology));

        final ConfiguredTopology configured = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0,
            topology, new KRaftCoordinatorMetadataImage(image), TIME);

        assertTrue(configured.topicConfigurationException().isPresent());
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, configured.topicConfigurationException().orElseThrow().status());
        assertTrue(configured.topicConfigurationException().orElseThrow().getMessage().contains(STATE_CHANGELOG_TOPIC_1));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void testRepartitionTopicStillRequiresExactCount(final int repartitionPartitions) {
        final MetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), REPARTITION_TOPIC, repartitionPartitions)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_2, Math.max(2, repartitionPartitions))
            .build();

        final ConfiguredTopology configured = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0,
            makeTestTopology(), new KRaftCoordinatorMetadataImage(image), TIME);

        assertTrue(configured.topicConfigurationException().isPresent());
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, configured.topicConfigurationException().orElseThrow().status());
        assertTrue(configured.topicConfigurationException().orElseThrow().getMessage().contains(REPARTITION_TOPIC));
    }

    @Test
    void testRepartitionChangelogStillRequiresExactCount() {
        final MetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 2)
            .addTopic(Uuid.randomUuid(), REPARTITION_TOPIC, 3)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 2)
            .build();
        final StreamsTopology topology = makeTestTopology();
        topology.subtopologies().get(SUBTOPOLOGY_2).setStateChangelogTopics(List.of(
            new StreamsGroupTopologyValue.TopicInfo().setName(REPARTITION_TOPIC)));

        final ConfiguredTopology configured = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0,
            topology, new KRaftCoordinatorMetadataImage(image), TIME);

        assertTrue(configured.topicConfigurationException().isPresent());
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, configured.topicConfigurationException().orElseThrow().status());
        assertTrue(configured.topicConfigurationException().orElseThrow().getMessage().contains(REPARTITION_TOPIC));
    }

    @Test
    void testSourceShrinkStillRejectsMismatchedCopartitionedInputs() {
        final MetadataImage image = new MetadataImageBuilder()
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_1, 2)
            .addTopic(Uuid.randomUuid(), SOURCE_TOPIC_2, 3)
            .addTopic(Uuid.randomUuid(), STATE_CHANGELOG_TOPIC_1, 3)
            .build();
        final Subtopology subtopology = new Subtopology().setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(List.of(SOURCE_TOPIC_1, SOURCE_TOPIC_2))
            .setStateChangelogTopics(List.of(new StreamsGroupTopologyValue.TopicInfo().setName(STATE_CHANGELOG_TOPIC_1)))
            .setCopartitionGroups(List.of(new StreamsGroupTopologyValue.CopartitionGroup()
                .setSourceTopics(List.of((short) 0, (short) 1))));
        final StreamsTopology topology = new StreamsTopology(1, Map.of(SUBTOPOLOGY_1, subtopology));

        final ConfiguredTopology configured = InternalTopicManager.configureTopics(LOG, "test-group", "test-member", 0,
            topology, new KRaftCoordinatorMetadataImage(image), TIME);

        assertTrue(configured.topicConfigurationException().isPresent());
        assertEquals(Status.INCORRECTLY_PARTITIONED_TOPICS, configured.topicConfigurationException().orElseThrow().status());
        assertTrue(configured.topicConfigurationException().orElseThrow().getMessage().contains("same number of partitions"));
    }

    private static Map<String, ConfiguredSubtopology> makeExpectedConfiguredSubtopologies() {
        return mkMap(
            mkEntry(SUBTOPOLOGY_1,
                new ConfiguredSubtopology(
                    2,
                    Set.of(SOURCE_TOPIC_1),
                    Map.of(),
                    Set.of(REPARTITION_TOPIC),
                    Map.of(STATE_CHANGELOG_TOPIC_1,
                        new ConfiguredInternalTopic(
                            STATE_CHANGELOG_TOPIC_1,
                            2,
                            Optional.empty(),
                            Map.of(CONFIG_KEY, CONFIG_VALUE)
                        ))
                )
            ),
            mkEntry(SUBTOPOLOGY_2,
                new ConfiguredSubtopology(
                    2,
                    Set.of(SOURCE_TOPIC_2),
                    Map.of(REPARTITION_TOPIC,
                        new ConfiguredInternalTopic(REPARTITION_TOPIC,
                            2,
                            Optional.of((short) 3),
                            Map.of()
                        )
                    ),
                    Set.of(),
                    Map.of(STATE_CHANGELOG_TOPIC_2,
                        new ConfiguredInternalTopic(STATE_CHANGELOG_TOPIC_2,
                            2,
                            Optional.empty(),
                            Map.of()
                        )))
            )
        );
    }

    private static StreamsTopology makeTestTopology() {
        // Create a subtopology source -> repartition
        Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_1)
            .setSourceTopics(List.of(SOURCE_TOPIC_1))
            .setRepartitionSinkTopics(List.of(REPARTITION_TOPIC))
            .setStateChangelogTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_1)
                    .setTopicConfigs(List.of(
                        new StreamsGroupTopologyValue.TopicConfig()
                            .setKey(CONFIG_KEY)
                            .setValue(CONFIG_VALUE)
                    ))
            ));
        // Create a subtopology repartition/source2 -> sink (copartitioned)
        Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId(SUBTOPOLOGY_2)
            .setSourceTopics(List.of(SOURCE_TOPIC_2))
            .setRepartitionSourceTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(REPARTITION_TOPIC)
                    .setReplicationFactor((short) 3)
            ))
            .setStateChangelogTopics(List.of(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName(STATE_CHANGELOG_TOPIC_2)
            ))
            .setCopartitionGroups(List.of(
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setSourceTopics(List.of((short) 0))
                    .setRepartitionSourceTopics(List.of((short) 0))
            ));

        return new StreamsTopology(3, Map.of(SUBTOPOLOGY_1, subtopology1, SUBTOPOLOGY_2, subtopology2));
    }

}
