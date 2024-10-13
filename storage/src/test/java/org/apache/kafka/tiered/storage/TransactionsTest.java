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
package org.apache.kafka.tiered.storage;

import kafka.server.KafkaBroker;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG;
import static org.apache.kafka.coordinator.group.GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_MIN_ISR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.coordinator.transaction.TransactionStateManagerConfig.TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG;
import static org.apache.kafka.server.config.ReplicationConfigs.AUTO_LEADER_REBALANCE_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG;
import static org.apache.kafka.server.config.ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterTestDefaults(
        brokers = 3,
        serverProperties = {
            @ClusterConfigProperty(key = AUTO_CREATE_TOPICS_ENABLE_CONFIG, value = "false"),
            @ClusterConfigProperty(key = OFFSETS_TOPIC_PARTITIONS_CONFIG, value = "1"),
            @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_PARTITIONS_CONFIG, value = "3"),
            @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG, value = "2"),
            @ClusterConfigProperty(key = TRANSACTIONS_TOPIC_MIN_ISR_CONFIG, value = "2"),
            @ClusterConfigProperty(key = CONTROLLED_SHUTDOWN_ENABLE_CONFIG, value = "true"),
            @ClusterConfigProperty(key = "unclean.leader.election.enable", value = "false"),
            @ClusterConfigProperty(key = AUTO_LEADER_REBALANCE_ENABLE_CONFIG, value = "false"),
            @ClusterConfigProperty(key = GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, value = "0"),
            @ClusterConfigProperty(key = TRANSACTIONS_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS_CONFIG, value = "200"),
        }
)
@ExtendWith(ClusterTestExtensions.class)
public class TransactionsTest {

    private final int transactionalProducerCount = 2;
    private final int transactionalConsumerCount = 1;
    private final int nonTransactionalConsumerCount = 1;

    private final String topic1 = "topic1";
    private final String topic2 = "topic2";
    private final int numPartitions = 4;

    private final List<KafkaProducer<byte[], byte[]>> transactionalProducers = new ArrayList<>();
    private final List<Consumer<byte[], byte[]>> transactionalConsumers = new ArrayList<>();
    private final List<Consumer<byte[], byte[]>> nonTransactionalConsumers = new ArrayList<>();

    private final String transactionStatusKey = "transactionStatus";
    private final String committedKey = "committed";
    private final String noTransactionGroup = "non-transactional-group";
    
    private final ClusterInstance cluster;

    public TransactionsTest(ClusterInstance clusterInstance) {
        this.cluster = clusterInstance;
    }

    private Map<String, String> topicConfig() {
        return Collections.singletonMap(ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }

    @AfterEach
    public void tearDown() {
        transactionalProducers.forEach(KafkaProducer::close);
        transactionalConsumers.forEach(Consumer::close);
        nonTransactionalConsumers.forEach(Consumer::close);
    }

    @ClusterTest
    public void testBasicTransactions() throws Exception {
        createTopics();
        createTransactionProducers();
        createTransactionConsumers();
        createNonTransactionConsumers();
        
        KafkaProducer<byte[], byte[]> producer = transactionalProducers.get(0);
        Consumer<byte[], byte[]> consumer = transactionalConsumers.get(0);
        Consumer<byte[], byte[]> unCommittedConsumer = nonTransactionalConsumers.get(0);
        TopicPartition tp11 = new TopicPartition(topic1, 1);
        TopicPartition tp22 = new TopicPartition(topic2, 2);

        producer.initTransactions();
        producer.beginTransaction();
        producer.send(producerRecordWithExpectedTransactionStatus(topic2, 2, "2", "2", false));
        producer.send(producerRecordWithExpectedTransactionStatus(topic1, 1, "4", "4", false));
        producer.flush();

        // Since we haven't committed/aborted any records, the last stable offset is still 0,
        // no segments should be offloaded to remote storage
        Map<TopicPartition, Integer> topicPartition = new HashMap<>();
        topicPartition.put(tp11, 0);
        topicPartition.put(tp22, 0);
        verifyLogStartOffsets(topicPartition);
        producer.abortTransaction();

        // We've sent 1 record + 1 abort mark = 2 (segments) to each topic partition,
        // so 1 segment should be offloaded, the local log start offset should be 1
        // And log start offset is still 0
        verifyLogStartOffsets(topicPartition);

        producer.beginTransaction();
        producer.send(producerRecordWithExpectedTransactionStatus(topic1, 1, "1", "1", true));
        producer.send(producerRecordWithExpectedTransactionStatus(topic2, 2, "3", "3", true));

        // Before records are committed, these records won't be offloaded.
        verifyLogStartOffsets(topicPartition);

        producer.commitTransaction();

        // We've sent 2 records + 1 abort mark + 1 commit mark = 4 (segments) to each topic partition,
        // so 3 segments should be offloaded, the local log start offset should be 3
        // And log start offset is still 0
        verifyLogStartOffsets(topicPartition);

        List<String> partitions = new ArrayList<>();
        partitions.add(tp11.topic());
        partitions.add(tp22.topic());
        consumer.subscribe(partitions);
        unCommittedConsumer.subscribe(partitions);

        List<ConsumerRecord<byte[], byte[]>> records = consumeRecords(consumer, 2);
        records.forEach(this::assertCommittedAndGetValue);
        List<ConsumerRecord<byte[], byte[]>> allRecords = consumeRecords(unCommittedConsumer, 4);
        Set<String> expectedValues = new HashSet<>();
        expectedValues.add("1");
        expectedValues.add("2");
        expectedValues.add("3");
        expectedValues.add("4");
        allRecords.forEach(record -> assertTrue(expectedValues.contains(recordValueAsString(record))));
    }

    private void createNonTransactionConsumers() {
        for (int i = 0; i < nonTransactionalConsumerCount; i++) {
            nonTransactionalConsumers.add(createReadUncommittedConsumer(noTransactionGroup));
        }
    }

    private void createTransactionConsumers() {
        for (int i = 0; i < transactionalConsumerCount; i++) {
            transactionalConsumers.add(createReadCommittedConsumer(
                    "transactional-group", 
                    100, 
                    new Properties())
            );
        }
    }

    private void createTransactionProducers() {
        for (int i = 0; i < transactionalProducerCount; i++) {
            transactionalProducers.add(
                    createTransactionalProducer(
                            "transactional-producer",
                            2000,
                            2000,
                            4000,
                            1000)
            );
        }
    }

    private void createTopics() {
        try (Admin adminClient = cluster.createAdminClient()) {
            List<NewTopic> newTopics = new ArrayList<>();
            newTopics.add(new NewTopic(topic1, numPartitions, (short) 3).configs(topicConfig()));
            newTopics.add(new NewTopic(topic2, numPartitions, (short) 3).configs(topicConfig()));
            adminClient.createTopics(newTopics);
        }
    }

    private KafkaProducer<byte[], byte[]> createTransactionalProducer(String transactionalId,
                                                                      int transactionTimeoutMs,
                                                                      int maxBlockMs,
                                                                      int deliveryTimeoutMs,
                                                                      int requestTimeoutMs) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<byte[], byte[]> createReadCommittedConsumer(
            String group,
            int maxPollRecords,
            Properties props
    ) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProps.putAll(props);
        return new KafkaConsumer<>(consumerProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }

    private KafkaConsumer<byte[], byte[]> createReadUncommittedConsumer(String group) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CLASSIC.name);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_uncommitted");
        return new KafkaConsumer<>(consumerProps);
    }

    private void verifyLogStartOffsets(Map<TopicPartition, Integer> partitionStartOffsets) throws InterruptedException {
        Collection<KafkaBroker> brokers = cluster.brokers().values();
        Map<Integer, Long> offsets = new HashMap<>();
        TestUtils.waitForCondition(() -> {
            for (KafkaBroker broker : brokers) {
                for (Map.Entry<TopicPartition, Integer> entry : partitionStartOffsets.entrySet()) {
                    long offset = broker.replicaManager().localLog(entry.getKey()).get().localLogStartOffset();
                    offsets.put(broker.config().brokerId(), offset);
                    if (entry.getValue() != offset) {
                        return false;
                    }
                }
            }
            return true;
        }, "log start offset doesn't change to the expected position: " + partitionStartOffsets + ", current position: " + offsets);
    }

    private ProducerRecord<byte[], byte[]> producerRecordWithExpectedTransactionStatus(
            String topic,
            int partition,
            String key,
            String value,
            boolean expectedTransactionStatus
    ) {
        Header header = new Header() {
            @Override
            public String key() {
                return transactionStatusKey;
            }

            @Override
            public byte[] value() {
                return expectedTransactionStatus ? committedKey.getBytes(UTF_8) : "aborted".getBytes(UTF_8);
            }
        };
        return new ProducerRecord<>(topic, partition, key.getBytes(), value.getBytes(), singleton(header));
    }

    private String assertCommittedAndGetValue(ConsumerRecord<byte[], byte[]> record) {
        Header header = record.headers().headers(transactionStatusKey).iterator().next();
        if (header != null) {
            assertEquals(committedKey, new String(header.value(), UTF_8), "Got " + new String(header.value(), UTF_8) + " but expected the value to indicate committed status.");
        } else {
            throw new RuntimeException("expected the record header to include an expected transaction status, but received nothing.");
        }
        return recordValueAsString(record);
    }

    private <K, V> List<ConsumerRecord<K, V>> consumeRecords(
            Consumer<K, V> consumer, 
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = pollUntilAtLeastNumRecords(consumer, numRecords);
        assertEquals(numRecords, records.size(), "Consumed more records than expected");
        return records;
    }

    public static <K, V> List<ConsumerRecord<K, V>> pollUntilAtLeastNumRecords(
            Consumer<K, V> consumer, 
            int numRecords
    ) throws InterruptedException {
        List<ConsumerRecord<K, V>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return records.size() >= numRecords;
        }, DEFAULT_MAX_WAIT_MS, format("Consumed %d records before timeout instead of the expected %d records", records.size(), numRecords));
        return records;
    }

    private String recordValueAsString(ConsumerRecord<byte[], byte[]> record) {
        return new String(record.value(), UTF_8);
    }
}
