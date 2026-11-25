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
package org.apache.kafka.clients.admin;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.replication.factor", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.min.isr", value = "1"),
        @ClusterConfigProperty(key = "transaction.state.log.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "group.initial.rebalance.delay.ms", value = "0")
    }
)
public class PartitionDeletionReviewTest {
    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void normalRemovalWaitsForTransactionWhileAnotherTopicCompletes(ClusterInstance cluster) throws Exception {
        String blocked = "review-transaction-blocked";
        String control = "review-transaction-control";
        try (Admin admin = cluster.admin();
             Producer<String, String> producer = stringProducer(cluster, Map.of(
                 ProducerConfig.TRANSACTIONAL_ID_CONFIG, "review-normal-transaction"))) {
            createTopic(admin, blocked, 2, 0);
            createTopic(admin, control, 2, 0);
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(blocked, 1, "key", "unresolved")).get(20, TimeUnit.SECONDS);

            admin.deletePartitions(Map.of(blocked, 1, control, 1)).all().get(20, TimeUnit.SECONDS);
            // Completion on the control topic proves that removal processing has actually run.
            waitForCount(admin, control, 1);
            TopicDescription description = describe(admin, blocked);
            assertEquals(2, description.partitions().size(), "An unresolved transaction must block normal removal");
            assertTrue(description.partitions().get(1).isDraining());

            producer.abortTransaction();
            waitForCount(admin, blocked, 1);
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(blocked, 0, "key", "surviving-transaction")).get(20, TimeUnit.SECONDS);
            producer.commitTransaction();
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void forcedRemovalAllowsTransactionCoordinatorToConverge(ClusterInstance cluster) throws Exception {
        String topic = "review-forced-transaction";
        try (Admin admin = cluster.admin();
             Producer<String, String> producer = stringProducer(cluster, Map.of(
                 ProducerConfig.TRANSACTIONAL_ID_CONFIG, "review-force-transaction"))) {
            createTopic(admin, topic, 2, 0);
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 1, "key", "forced-away")).get(20, TimeUnit.SECONDS);
            admin.deletePartitions(Map.of(topic, 1), new DeletePartitionsOptions().force(true))
                .all().get(20, TimeUnit.SECONDS);
            waitForCount(admin, topic, 1);

            producer.abortTransaction();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 0, "key", "after-force")).get(20, TimeUnit.SECONDS);
            producer.commitTransaction();
            assertEquals(List.of("after-force"), consumePartition(cluster, topic, 0, 1));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void reopeningRetainsLogAndContinuesOffsets(ClusterInstance cluster) throws Exception {
        String topic = "review-reopen-log";
        try (Admin admin = cluster.admin(); Producer<String, String> producer = stringProducer(cluster, Map.of())) {
            createTopic(admin, topic, 2, 60000);
            assertEquals(0L, producer.send(new ProducerRecord<>(topic, 1, "key", "before"))
                .get(20, TimeUnit.SECONDS).offset());
            admin.deletePartitions(Map.of(topic, 1)).all().get(20, TimeUnit.SECONDS);
            waitForDraining(admin, topic, 1);

            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get(20, TimeUnit.SECONDS);
            TestUtils.waitForCondition(() -> !describe(admin, topic).partitions().get(1).isDraining(),
                30000, "Reopened partition must become active");
            assertEquals(1L, producer.send(new ProducerRecord<>(topic, 1, "key", "after"))
                .get(20, TimeUnit.SECONDS).offset());
            assertEquals(List.of("before", "after"), consumePartition(cluster, topic, 1, 2));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void keyedAndUnkeyedProducersRouteOnlyToActivePartitions(ClusterInstance cluster) throws Exception {
        String topic = "review-producer-convergence";
        try (Admin admin = cluster.admin()) {
            createTopic(admin, topic, 4, 60000);
            admin.deletePartitions(Map.of(topic, 2)).all().get(20, TimeUnit.SECONDS);
            waitForDraining(admin, topic, 2);
            try (Producer<String, String> producer = stringProducer(cluster, Map.of())) {
                for (int i = 0; i < 32; i++) {
                    assertTrue(producer.send(new ProducerRecord<>(topic, "key-" + i, "keyed-" + i))
                        .get(20, TimeUnit.SECONDS).partition() < 2);
                    assertTrue(producer.send(new ProducerRecord<String, String>(topic, "unkeyed-" + i))
                        .get(20, TimeUnit.SECONDS).partition() < 2);
                }
            }
            assertEquals(4, describe(admin, topic).partitions().size(),
                "Successful routing must not depend on physically deleting the draining replicas");
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void classicSubscriptionIncludesRetainedDrainingPartitions(ClusterInstance cluster) throws Exception {
        verifySubscriptionDuringDrain(cluster, "classic");
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void consumerProtocolSubscriptionIncludesRetainedDrainingPartitions(ClusterInstance cluster) throws Exception {
        verifySubscriptionDuringDrain(cluster, "consumer");
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void consumerProtocolGroupReconcilesAfterPhysicalRemoval(ClusterInstance cluster) throws Exception {
        String topic = "review-consumer-physical-removal";
        try (Admin admin = cluster.admin();
             Producer<String, String> producer = stringProducer(cluster, Map.of(
                 ProducerConfig.TRANSACTIONAL_ID_CONFIG, "review-consumer-removal-transaction"));
             Consumer<String, String> consumer = stringConsumer(cluster, Map.of(
                 ConsumerConfig.GROUP_ID_CONFIG, "review-consumer-removal-group",
                 ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer"))) {
            createTopic(admin, topic, 4, 0);
            producer.initTransactions();
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(topic, 3, "key", "hold-tail-until-assigned"))
                .get(20, TimeUnit.SECONDS);
            consumer.subscribe(List.of(topic));
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100));
                return consumer.assignment().size() == 4;
            }, 30000, "Consumer must own the original partition set before shrinking");

            admin.deletePartitions(Map.of(topic, 2)).all().get(20, TimeUnit.SECONDS);
            waitForDraining(admin, topic, 3);
            producer.abortTransaction();
            waitForCount(admin, topic, 2);

            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100));
                return consumer.assignment().equals(Set.of(
                    new TopicPartition(topic, 0), new TopicPartition(topic, 1)));
            }, 15000, "The consumer group must revoke physically removed tail partitions and keep consuming");
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void physicalRemovalPreventsPartitionIdReuse(ClusterInstance cluster) throws Exception {
        String topic = "review-partition-id-fencing";
        try (Admin admin = cluster.admin()) {
            createTopic(admin, topic, 3, 0);
            admin.deletePartitions(Map.of(topic, 1)).all().get(20, TimeUnit.SECONDS);
            waitForCount(admin, topic, 1);
            ExecutionException exception = assertThrows(ExecutionException.class,
                () -> admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2)))
                    .all().get(20, TimeUnit.SECONDS));
            assertInstanceOf(InvalidPartitionsException.class, exception.getCause());
            assertEquals(1, describe(admin, topic).partitions().size());
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void brokerRestartRestoresDrainAndReopeningKeepsData(ClusterInstance cluster) throws Exception {
        String topic = "review-drain-restart";
        try (Admin admin = cluster.admin()) {
            createTopic(admin, topic, 2, 60000);
            try (Producer<String, String> producer = stringProducer(cluster, Map.of())) {
                producer.send(new ProducerRecord<>(topic, 1, "key", "before-restart")).get(20, TimeUnit.SECONDS);
            }
            admin.deletePartitions(Map.of(topic, 1)).all().get(20, TimeUnit.SECONDS);
            waitForDraining(admin, topic, 1);
            OptionalLong deadline = describe(admin, topic).partitions().get(1).drainDeadlineMs();
            int brokerId = cluster.brokerIds().iterator().next();
            cluster.shutdownBroker(brokerId);
            cluster.startBroker(brokerId);
            waitForDraining(admin, topic, 1);
            assertEquals(deadline, describe(admin, topic).partitions().get(1).drainDeadlineMs());

            admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get(20, TimeUnit.SECONDS);
            try (Producer<String, String> producer = stringProducer(cluster, Map.of())) {
                assertEquals(1L, producer.send(new ProducerRecord<>(topic, 1, "key", "after-restart"))
                    .get(20, TimeUnit.SECONDS).offset());
            }
            assertEquals(List.of("before-restart", "after-restart"), consumePartition(cluster, topic, 1, 2));
        }
    }

    private static void verifySubscriptionDuringDrain(ClusterInstance cluster, String protocol) throws Exception {
        String topic = "review-subscription-" + protocol;
        try (Admin admin = cluster.admin()) {
            createTopic(admin, topic, 2, 60000);
            try (Producer<String, String> producer = stringProducer(cluster, Map.of())) {
                producer.send(new ProducerRecord<>(topic, 1, "key", "readable-during-drain")).get(20, TimeUnit.SECONDS);
            }
            admin.deletePartitions(Map.of(topic, 1)).all().get(20, TimeUnit.SECONDS);
            waitForDraining(admin, topic, 1);
            try (Consumer<String, String> consumer = stringConsumer(cluster, Map.of(
                ConsumerConfig.GROUP_ID_CONFIG, "review-group-" + protocol,
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, protocol))) {
                consumer.subscribe(List.of(topic));
                List<String> values = new ArrayList<>();
                TestUtils.waitForCondition(() -> {
                    consumer.poll(Duration.ofMillis(100)).forEach(record -> values.add(record.value()));
                    return !values.isEmpty();
                }, 30000, "A subscription started during draining must still consume retained tail data");
                assertEquals(List.of("readable-during-drain"), values);
                assertTrue(consumer.assignment().contains(new TopicPartition(topic, 1)));
                assertTrue(describe(admin, topic).partitions().get(1).isDraining());
            }
        }
    }

    private static Producer<String, String> stringProducer(ClusterInstance cluster, Map<String, Object> overrides) {
        Map<String, Object> config = new HashMap<>(overrides);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 20000);
        config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 120000);
        return cluster.producer(config);
    }

    private static Consumer<String, String> stringConsumer(ClusterInstance cluster, Map<String, Object> overrides) {
        Map<String, Object> config = new HashMap<>(overrides);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return cluster.consumer(config);
    }

    private static List<String> consumePartition(ClusterInstance cluster, String topic, int partition, int expected)
        throws Exception {
        try (Consumer<String, String> consumer = stringConsumer(cluster, Map.of())) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            consumer.assign(List.of(topicPartition));
            consumer.seekToBeginning(List.of(topicPartition));
            List<String> values = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(100)).forEach(record -> values.add(record.value()));
                return values.size() >= expected;
            }, 30000, "Expected retained records in " + topicPartition);
            assertEquals(expected, values.size());
            return values;
        }
    }

    private static void createTopic(Admin admin, String topic, int count, long drainTimeoutMs) throws Exception {
        NewTopic newTopic = new NewTopic(topic, count, (short) 1)
            .configs(Map.of(TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_CONFIG, Long.toString(drainTimeoutMs)));
        admin.createTopics(List.of(newTopic)).all().get(20, TimeUnit.SECONDS);
        waitForCount(admin, topic, count);
    }

    private static TopicDescription describe(Admin admin, String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(20, TimeUnit.SECONDS).get(topic);
    }

    private static void waitForCount(Admin admin, String topic, int expected) throws Exception {
        TestUtils.waitForCondition(() -> describe(admin, topic).partitions().size() == expected,
            65000, "Expected " + expected + " physical partitions in " + topic);
    }

    private static void waitForDraining(Admin admin, String topic, int partition) throws Exception {
        TestUtils.waitForCondition(() -> {
            TopicDescription description = describe(admin, topic);
            return description.partitions().size() > partition && description.partitions().get(partition).isDraining();
        }, 30000, "Expected draining partition " + topic + "-" + partition);
    }
}
