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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.common.MetadataVersion;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.apache.kafka.clients.ClientsTestUtils.awaitAssignment;
import static org.apache.kafka.clients.ClientsTestUtils.consumeAndVerifyRecords;
import static org.apache.kafka.clients.ClientsTestUtils.pollUntilTrue;
import static org.apache.kafka.clients.ClientsTestUtils.sendRecords;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_LATEST_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for KIP-1327: auto.offset.reset.latest.max.age.
 *
 * <p>Hot partition (age ≤ maxAge): LATEST reset is promoted to EARLIEST so a new
 * consumer group reads all records in a freshly created topic.
 *
 * <p>Cold partition (age > maxAge): LATEST reset stays LATEST.
 *
 * <p>Only the CONSUMER (async) group protocol is tested because the feature is not
 * supported with the classic protocol.
 *
 * <p>Both tests use {@code subscribe()} so that {@code ConsumerMembershipManager
 * .processAssignmentReceived()} is called and triggers a fresh metadata fetch
 * (with {@code partitionAgeMs}) before the first offset reset is computed.
 */
@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = PlaintextConsumerLatestMaxAgeTest.BROKER_COUNT,
    serverProperties = {
        @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
        @ClusterConfigProperty(key = OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "3"),
        @ClusterConfigProperty(key = GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG, value = "100"),
    }
)
public class PlaintextConsumerLatestMaxAgeTest {

    public static final int BROKER_COUNT = 3;
    private final ClusterInstance cluster;
    private final String topic = "topic";
    private final TopicPartition tp = new TopicPartition(topic, 0);

    public PlaintextConsumerLatestMaxAgeTest(ClusterInstance cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        cluster.createTopic(topic, 1, (short) BROKER_COUNT);
    }

    /**
     * Hot partition test: partition age ≈ 0 ms which is far below the 1-hour maxAge.
     * The LATEST offset reset should be promoted to EARLIEST so the consumer reads
     * all records from the beginning of the (freshly created) topic.
     */
    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV1)
    public void testAsyncConsumerLatestMaxAgeHotPartitionResetsToEarliest() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "latest",
            AUTO_OFFSET_RESET_LATEST_MAX_AGE_CONFIG, 3_600_000L, // 1 hour — partition is much younger
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );
        var totalRecords = 10;
        sendRecords(cluster, tp, totalRecords, 0);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            // Use subscribe so ConsumerMembershipManager.processAssignmentReceived() runs
            // and requests a fresh metadata fetch (with partitionAgeMs) before the first
            // offset reset is computed.
            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp));

            // Partition is freshly created (age ≈ 0 ms << 3 600 000 ms) →
            // hot → LATEST reset promoted to EARLIEST → reads from offset 0
            consumeAndVerifyRecords(consumer, tp, totalRecords, 0);
        }
    }

    /**
     * Cold partition test: maxAge is set to 1 ms, and we sleep 100 ms before creating
     * the consumer so the partition is already "cold" by the time the offset reset runs.
     * The LATEST reset must stay LATEST — the consumer should start at the end and
     * only read records produced after it joins.
     */
    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV1)
    public void testAsyncConsumerLatestMaxAgeColdPartitionKeepsLatest() throws InterruptedException {
        Map<String, Object> config = Map.of(
            GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name().toLowerCase(Locale.ROOT),
            AUTO_OFFSET_RESET_CONFIG, "latest",
            AUTO_OFFSET_RESET_LATEST_MAX_AGE_CONFIG, 1L, // 1 ms — partition will be older
            FETCH_MAX_WAIT_MS_CONFIG, 0
        );
        var totalRecords = 10;
        sendRecords(cluster, tp, totalRecords, 0);

        // Ensure the partition is older than 1 ms before the consumer connects
        Thread.sleep(100);

        try (Consumer<byte[], byte[]> consumer = cluster.consumer(config)) {
            consumer.subscribe(List.of(topic));
            awaitAssignment(consumer, Set.of(tp));

            // Partition age >> 1 ms → cold → LATEST reset stays → consumer positioned at end
            pollUntilTrue(consumer, () -> consumer.position(tp) == totalRecords,
                "Consumer should advance to end offset after LATEST reset");

            // Produce one more record; consumer should see only this new record (offset 10)
            sendRecords(cluster, tp, 1, 100);
            var records = consumer.poll(Duration.ofMillis(5000));
            assertEquals(1, records.count());
            assertEquals(totalRecords, records.iterator().next().offset());
        }
    }
}
