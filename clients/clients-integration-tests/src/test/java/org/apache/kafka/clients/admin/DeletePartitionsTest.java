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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TimeoutException;
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

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
    types = {Type.KRAFT},
    brokers = 1,
    serverProperties = {
        @ClusterConfigProperty(key = "auto.create.topics.enable", value = "false"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    }
)
public class DeletePartitionsTest {

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void testDeletePartitionsFullLifecycle(ClusterInstance cluster) throws Exception {
        String topic = "lifecycle-test";

        try (Admin admin = cluster.admin()) {
            createTopicWithDrainTimeout(admin, topic, 6, 5000);
            produceToAllPartitions(cluster, topic, 6, 10);
            verifyConsumeCount(cluster, topic, 6, 60);

            admin.deletePartitions(Map.of(topic, 3), new DeletePartitionsOptions()).all().get();

            verifyProduceToDrainingRejected(cluster, topic, 3, 6);
            verifyNonDrainingConsumable(cluster, topic, 3);
            waitForPartitionCount(admin, topic, 3, 60000);
            verifyLogDirsGone(cluster, topic, Set.of(3, 4, 5));

            verifyPartitionIdReuseRejected(admin, topic, 5, 3);
            // The rejection helper also accepted one write to surviving partition 0.
            verifyConsumeCount(cluster, topic, 3, 31);
            verifyLogDirsGone(cluster, topic, Set.of(3, 4, 5));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void testDrainTimeoutConfigRespected(ClusterInstance cluster) throws Exception {
        String topic = "drain-timeout-test";

        try (Admin admin = cluster.admin()) {
            createTopicWithDrainTimeout(admin, topic, 4, 3000);
            long start = System.currentTimeMillis();
            admin.deletePartitions(Map.of(topic, 2), new DeletePartitionsOptions()).all().get();
            waitForPartitionCount(admin, topic, 2, 60000);
            long elapsed = System.currentTimeMillis() - start;

            assertTrue(elapsed >= 3000,
                "Removal should take at least drain timeout (3s), took " + elapsed + "ms");
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2)
    @Timeout(180)
    public void testRemovedPartitionDataIsGone(ClusterInstance cluster) throws Exception {
        String topic = "data-gone-test";

        try (Admin admin = cluster.admin()) {
            createTopicWithDrainTimeout(admin, topic, 4, 3000);
            produceToPartitions(cluster, topic, List.of(0, 1), 5, "retained-");
            produceToPartitions(cluster, topic, List.of(3), 100, "old-");

            admin.deletePartitions(Map.of(topic, 2), new DeletePartitionsOptions()).all().get();
            waitForPartitionCount(admin, topic, 2, 60000);
            verifyLogDirsGone(cluster, topic, Set.of(2, 3));

            verifyPartitionIdReuseRejected(admin, topic, 4, 2);
            verifyConsumeCount(cluster, topic, 2, 10);
            verifyLogDirsGone(cluster, topic, Set.of(2, 3));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2, brokers = 3)
    @Timeout(180)
    public void testDeletePartitionsWithMultipleReplicas(ClusterInstance cluster) throws Exception {
        String topic = "multi-replica-test";

        try (Admin admin = cluster.admin()) {
            createTopicWithDrainTimeoutAndReplicas(admin, topic, 4, (short) 3, 5000);
            produceToAllPartitions(cluster, topic, 4, 20);
            verifyConsumeCount(cluster, topic, 4, 80);

            admin.deletePartitions(Map.of(topic, 2), new DeletePartitionsOptions()).all().get();

            verifyProduceToDrainingRejected(cluster, topic, 2, 4);
            verifyNonDrainingConsumable(cluster, topic, 2);
            waitForPartitionCount(admin, topic, 2, 60000);
            verifyLogDirsGoneAllBrokers(cluster, topic, Set.of(2, 3));

            verifyPartitionIdReuseRejected(admin, topic, 4, 2);
            // The rejection helper also accepted one write to surviving partition 0.
            verifyConsumeCount(cluster, topic, 2, 41);
            verifyLogDirsGoneAllBrokers(cluster, topic, Set.of(2, 3));
        }
    }

    @ClusterTest(metadataVersion = MetadataVersion.IBP_4_4_IV2, brokers = 3)
    @Timeout(180)
    public void testDeletePartitionsReplicaIsrShrinkDuringDrain(ClusterInstance cluster) throws Exception {
        String topic = "isr-drain-test";

        try (Admin admin = cluster.admin()) {
            createTopicWithDrainTimeoutAndReplicas(admin, topic, 3, (short) 3, 5000);
            produceToAllPartitions(cluster, topic, 3, 10);

            admin.deletePartitions(Map.of(topic, 1), new DeletePartitionsOptions()).all().get();

            TestUtils.waitForCondition(() -> {
                try {
                    TopicDescription desc = admin.describeTopics(List.of(topic))
                        .topicNameValues().get(topic).get();
                    return desc.partitions().size() == 1;
                } catch (Exception e) {
                    return false;
                }
            }, 60000, "Expected 1 partition after drain completes");

            verifyLogDirsGoneAllBrokers(cluster, topic, Set.of(1, 2));

            TopicDescription desc = admin.describeTopics(List.of(topic))
                .topicNameValues().get(topic).get();
            assertEquals(3, desc.partitions().get(0).replicas().size(),
                "Remaining partition 0 should still have RF=3");
        }
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private void createTopicWithDrainTimeout(Admin admin, String topic, int partitions, int timeoutMs)
        throws Exception {
        createTopicWithDrainTimeoutAndReplicas(admin, topic, partitions, (short) 1, timeoutMs);
    }

    private void createTopicWithDrainTimeoutAndReplicas(Admin admin, String topic, int partitions,
                                                         short replicas, int timeoutMs) throws Exception {
        NewTopic newTopic = new NewTopic(topic, partitions, replicas);
        newTopic.configs(Map.of(TopicConfig.PARTITION_DRAIN_TIMEOUT_MS_CONFIG, String.valueOf(timeoutMs)));
        admin.createTopics(List.of(newTopic)).all().get();
        waitForPartitionCount(admin, topic, partitions, 30000);
    }

    private void produceToAllPartitions(ClusterInstance cluster, String topic, int numPartitions, int msgsPerPartition)
        throws Exception {
        try (Producer<String, String> producer = createStringProducer(cluster)) {
            for (int p = 0; p < numPartitions; p++) {
                for (int i = 0; i < msgsPerPartition; i++) {
                    producer.send(new ProducerRecord<>(topic, p, null,
                        "key-" + p + "-" + i, "p" + p + "-msg" + i)).get();
                }
            }
        }
    }

    private void produceToPartitions(ClusterInstance cluster, String topic, List<Integer> partitions,
                                     int msgsPerPartition, String prefix) throws Exception {
        try (Producer<String, String> producer = createStringProducer(cluster)) {
            for (int p : partitions) {
                for (int i = 0; i < msgsPerPartition; i++) {
                    producer.send(new ProducerRecord<>(topic, p, null,
                        prefix + "key-" + i, prefix + "p" + p + "-msg" + i)).get();
                }
            }
        }
    }

    private void verifyConsumeCount(ClusterInstance cluster, String topic, int numPartitions, int expected)
        throws Exception {
        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) tps.add(new TopicPartition(topic, i));

        try (Consumer<String, String> consumer = createStringConsumer(cluster)) {
            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            int total = 0;
            long deadline = System.currentTimeMillis() + 30000;
            while (total < expected && System.currentTimeMillis() < deadline) {
                total += consumer.poll(Duration.ofMillis(1000)).count();
            }
            assertEquals(expected, total, "Expected " + expected + " total records");
        }
    }

    private void verifyProduceToDrainingRejected(ClusterInstance cluster, String topic,
                                                  int targetCount, int originalCount) throws Exception {
        try (Producer<String, String> producer = cluster.producer(Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000",
            ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000",
            ProducerConfig.METADATA_MAX_AGE_CONFIG, "100",
            ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000",
            ProducerConfig.RETRIES_CONFIG, "1"
        ))) {
            producer.send(new ProducerRecord<>(topic, 0, null, "k", "v")).get(10, TimeUnit.SECONDS);
            TestUtils.waitForCondition(() -> producer.partitionsFor(topic).size() == targetCount,
                10000, "Producer metadata should exclude the draining tail before explicit rejection checks");

            for (int p = targetCount; p < originalCount; p++) {
                int partition = p;
                ExecutionException e = assertThrows(ExecutionException.class, () ->
                    producer.send(new ProducerRecord<>(topic, partition, null, "k", "v")).get(10, TimeUnit.SECONDS));
                assertTrue(e.getCause() instanceof NotLeaderOrFollowerException ||
                        e.getCause() instanceof TimeoutException,
                    "Partition " + partition + ": got " + e.getCause().getClass().getName());
            }
        }
    }

    private void verifyNonDrainingConsumable(ClusterInstance cluster, String topic, int count) throws Exception {
        List<Integer> partitions = new ArrayList<>();
        for (int i = 0; i < count; i++) partitions.add(i);
        Map<Integer, List<String>> consumed = consumeFromPartitions(cluster, topic, partitions);
        for (int p = 0; p < count; p++) {
            assertFalse(consumed.getOrDefault(p, List.of()).isEmpty(),
                "Should consume from non-draining partition " + p);
        }
    }

    private void waitForPartitionCount(Admin admin, String topic, int expected, long timeoutMs) throws Exception {
        TestUtils.waitForCondition(() -> {
            try {
                return admin.describeTopics(List.of(topic))
                    .topicNameValues().get(topic).get().partitions().size() == expected;
            } catch (Exception e) {
                return false;
            }
        }, timeoutMs, "Expected " + expected + " partitions for topic " + topic);
    }

    private void verifyLogDirsGone(ClusterInstance cluster, String topic, Set<Integer> partitions) throws Exception {
        TestUtils.waitForCondition(() ->
            cluster.brokers().values().stream().allMatch(broker ->
                broker.config().logDirs().stream().allMatch(logDir ->
                    partitions.stream().noneMatch(p ->
                        new File(logDir, topic + "-" + p).exists()))),
            30000, "Partition log directories should be deleted");
    }

    private void verifyLogDirsGoneAllBrokers(ClusterInstance cluster, String topic, Set<Integer> partitions)
        throws Exception {
        TestUtils.waitForCondition(() ->
            cluster.brokers().values().stream().allMatch(broker ->
                broker.config().logDirs().stream().allMatch(logDir ->
                    partitions.stream().noneMatch(p ->
                        new File(logDir, topic + "-" + p).exists()))),
            60000, "Partition log directories should be deleted on ALL brokers");
    }

    private void verifyPartitionIdReuseRejected(Admin admin, String topic, int requestedCount, int retainedCount)
        throws Exception {
        ExecutionException failure = assertThrows(ExecutionException.class,
            () -> admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(requestedCount)))
                .all().get(10, TimeUnit.SECONDS));
        assertInstanceOf(InvalidPartitionsException.class, failure.getCause());
        assertEquals(retainedCount, admin.describeTopics(List.of(topic))
            .topicNameValues().get(topic).get(10, TimeUnit.SECONDS).partitions().size());
    }

    private Map<Integer, List<String>> consumeFromPartitions(ClusterInstance cluster, String topic,
                                                              List<Integer> partitionIds) throws Exception {
        Map<Integer, List<String>> result = new HashMap<>();
        Set<TopicPartition> tps = new HashSet<>();
        for (int id : partitionIds) tps.add(new TopicPartition(topic, id));

        try (Consumer<String, String> consumer = createStringConsumer(cluster)) {
            consumer.assign(tps);
            consumer.seekToBeginning(tps);

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for (ConsumerRecord<String, String> r : records) {
                        result.computeIfAbsent(r.partition(), k -> new ArrayList<>()).add(r.value());
                    }
                }
            }
        }
        return result;
    }

    private Producer<String, String> createStringProducer(ClusterInstance cluster) {
        return cluster.producer(Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));
    }

    private Consumer<String, String> createStringConsumer(ClusterInstance cluster) {
        return cluster.consumer(Map.of(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ));
    }
}
