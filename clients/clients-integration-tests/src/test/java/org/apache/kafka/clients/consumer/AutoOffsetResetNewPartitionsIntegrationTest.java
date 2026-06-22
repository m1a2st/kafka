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
package org.apache.kafka.clients.consumer;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_NEW_PARTITIONS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for KIP-1327: auto.offset.reset.new.partitions
 *
 * This feature allows consumers to use a different offset reset strategy for
 * "newly expanded" partitions (created after the consumer group) vs pre-existing
 * partitions (created before the consumer group).
 *
 * These tests only run with the CONSUMER protocol (KIP-848) since the feature
 * is not supported with the CLASSIC protocol.
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 3,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class AutoOffsetResetNewPartitionsIntegrationTest {

    private final ClusterInstance cluster;
    private final String topic = "test-topic";

    public AutoOffsetResetNewPartitionsIntegrationTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 1, (short) 3);
    }

    /**
     * Tests that pre-existing partitions use auto.offset.reset (earliest) while
     * newly expanded partitions use auto.offset.reset.new.partitions (latest).
     *
     * Flow:
     * 1. Create topic with 1 partition, produce records
     * 2. Start consumer group with auto.offset.reset=earliest, auto.offset.reset.new.partitions=latest
     * 3. Consumer reads from partition 0 at earliest (pre-existing partition)
     * 4. Add partition 1, produce records to it
     * 5. Consumer is assigned partition 1 but starts at latest (newly expanded partition)
     */
    @ClusterTest
    public void testNewlyExpandedPartitionUsesNewPartitionsPolicy() throws Exception {
        TopicPartition tp0 = new TopicPartition(topic, 0);

        // Produce 5 records to partition 0 before the consumer group starts
        sendRecords(cluster, tp0, 5);

        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            AUTO_OFFSET_RESET_NEW_PARTITIONS_CONFIG, "latest",
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));

            // Wait until consumer is assigned partition 0 and consumes from earliest
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(records::add);
                return records.size() >= 5;
            }, 30000, "Consumer should consume all 5 records from pre-existing partition 0 at earliest offset");

            assertEquals(5, records.size());
            // Verify they are from partition 0 starting at offset 0
            assertEquals(0, records.get(0).offset());
            assertEquals(tp0, new TopicPartition(records.get(0).topic(), records.get(0).partition()));

            // Now add a new partition (partition 1) - this will have creationTime > groupCreationTime
            try (Admin admin = cluster.admin()) {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get();
            }
            cluster.waitTopicCreation(topic, 2);

            TopicPartition tp1 = new TopicPartition(topic, 1);

            // Produce 5 records to the new partition 1
            try (Producer<byte[], byte[]> producer = cluster.producer()) {
                for (int i = 0; i < 5; i++) {
                    producer.send(new ProducerRecord<>(topic, 1,
                        ("key-" + i).getBytes(), ("value-" + i).getBytes()));
                }
                producer.flush();
            }

            // Wait for consumer to be assigned the new partition
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500));
                return consumer.assignment().contains(tp1);
            }, 30000, "Consumer should be assigned the new partition 1");

            // Produce more records to partition 1 AFTER the consumer is assigned
            try (Producer<byte[], byte[]> producer = cluster.producer()) {
                for (int i = 5; i < 10; i++) {
                    producer.send(new ProducerRecord<>(topic, 1,
                        ("key-" + i).getBytes(), ("value-" + i).getBytes()));
                }
                producer.flush();
            }

            // Consumer should only see records produced AFTER assignment (latest policy)
            // because auto.offset.reset.new.partitions=latest was applied.
            // The 5 records produced before assignment should be skipped.
            List<ConsumerRecord<byte[], byte[]>> newPartitionRecords = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : polled) {
                    if (r.partition() == 1) {
                        newPartitionRecords.add(r);
                    }
                }
                return newPartitionRecords.size() >= 5;
            }, 30000, "Consumer should consume records from new partition 1 starting at latest");

            // The first record from partition 1 should be at offset 5 (latest at time of assignment)
            // since 5 records were produced before the consumer was assigned.
            assertTrue(newPartitionRecords.get(0).offset() >= 5,
                "First consumed offset on new partition should be >= 5 (latest at assignment), but was " +
                    newPartitionRecords.get(0).offset());
        }
    }

    /**
     * Tests that when auto.offset.reset.new.partitions is not configured, all partitions
     * (both pre-existing and newly expanded) use the base auto.offset.reset strategy.
     */
    @ClusterTest
    public void testWithoutNewPartitionsConfigUsesBasePolicy() throws Exception {
        TopicPartition tp0 = new TopicPartition(topic, 0);

        // Produce records to partition 0
        sendRecords(cluster, tp0, 5);

        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));

            // Wait until consumer reads from partition 0 at earliest
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(records::add);
                return records.size() >= 5;
            }, 30000, "Consumer should consume all records from partition 0 at earliest");

            assertEquals(5, records.size());

            // Add new partition
            try (Admin admin = cluster.admin()) {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get();
            }
            cluster.waitTopicCreation(topic, 2);

            // Produce records to new partition BEFORE consumer is assigned
            try (Producer<byte[], byte[]> producer = cluster.producer()) {
                for (int i = 0; i < 5; i++) {
                    producer.send(new ProducerRecord<>(topic, 1,
                        ("key-" + i).getBytes(), ("value-" + i).getBytes()));
                }
                producer.flush();
            }

            // Wait for consumer to be assigned the new partition and consume from earliest
            List<ConsumerRecord<byte[], byte[]>> newPartitionRecords = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : polled) {
                    if (r.partition() == 1) {
                        newPartitionRecords.add(r);
                    }
                }
                return newPartitionRecords.size() >= 5;
            }, 30000, "Consumer should consume records from new partition 1 at earliest");

            // Without auto.offset.reset.new.partitions, even new partitions use base policy (earliest)
            assertEquals(0, newPartitionRecords.get(0).offset(),
                "Without new-partitions config, new partition should also reset to earliest");
        }
    }

    /**
     * Tests that the classic consumer protocol rejects auto.offset.reset.new.partitions config.
     */
    @ClusterTest
    public void testClassicProtocolRejectsNewPartitionsConfig() {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            AUTO_OFFSET_RESET_NEW_PARTITIONS_CONFIG, "latest"
        );

        // Classic protocol should throw when the config is set
        org.junit.jupiter.api.Assertions.assertThrows(Exception.class, () -> {
            try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
                consumer.subscribe(List.of(topic));
                consumer.poll(Duration.ofMillis(5000));
            }
        });
    }

    /**
     * Tests that pre-existing partitions (partition created before group) use base policy
     * when auto.offset.reset.new.partitions is configured but all partitions are pre-existing.
     */
    @ClusterTest
    public void testAllPreExistingPartitionsUseBasePolicy() throws Exception {
        TopicPartition tp0 = new TopicPartition(topic, 0);

        // Produce records BEFORE consumer group starts
        sendRecords(cluster, tp0, 10);

        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "earliest",
            AUTO_OFFSET_RESET_NEW_PARTITIONS_CONFIG, "latest",
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));

            // Even with new-partitions=latest, partition 0 is pre-existing,
            // so it should use auto.offset.reset=earliest
            List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(records::add);
                return records.size() >= 10;
            }, 30000, "Consumer should consume all 10 records from pre-existing partition 0");

            assertEquals(10, records.size());
            assertEquals(0, records.get(0).offset(),
                "Pre-existing partition should use base policy (earliest)");
        }
    }

    /**
     * Tests that newly expanded partitions with auto.offset.reset.new.partitions=earliest
     * properly consume from the beginning.
     */
    @ClusterTest
    public void testNewPartitionsPolicyEarliest() throws Exception {
        TopicPartition tp0 = new TopicPartition(topic, 0);

        // Produce records to initial partition
        sendRecords(cluster, tp0, 5);

        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "latest",
            AUTO_OFFSET_RESET_NEW_PARTITIONS_CONFIG, "earliest",
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));

            // Partition 0 is pre-existing, auto.offset.reset=latest, so no records
            // should be consumed (they were produced before subscription)
            Set<TopicPartition> expectedAssignment = new HashSet<>();
            expectedAssignment.add(tp0);
            TestUtils.waitForCondition(() -> {
                consumer.poll(Duration.ofMillis(500));
                return consumer.assignment().containsAll(expectedAssignment);
            }, 30000, "Consumer should be assigned partition 0");

            // Add new partition
            try (Admin admin = cluster.admin()) {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(2))).all().get();
            }
            cluster.waitTopicCreation(topic, 2);

            // Produce records to new partition BEFORE consumer is assigned it
            try (Producer<byte[], byte[]> producer = cluster.producer()) {
                for (int i = 0; i < 5; i++) {
                    producer.send(new ProducerRecord<>(topic, 1,
                        ("key-" + i).getBytes(), ("value-" + i).getBytes()));
                }
                producer.flush();
            }

            // Wait for consumer to be assigned the new partition and consume from earliest
            List<ConsumerRecord<byte[], byte[]>> newPartitionRecords = new ArrayList<>();
            TestUtils.waitForCondition(() -> {
                ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<byte[], byte[]> r : polled) {
                    if (r.partition() == 1) {
                        newPartitionRecords.add(r);
                    }
                }
                return newPartitionRecords.size() >= 5;
            }, 30000, "Consumer should consume all records from new partition 1 at earliest");

            // New partition should use new-partitions policy (earliest), starting at offset 0
            assertEquals(0, newPartitionRecords.get(0).offset(),
                "New partition with new-partitions=earliest should start at offset 0");
            assertEquals(5, newPartitionRecords.size());
        }
    }
}
