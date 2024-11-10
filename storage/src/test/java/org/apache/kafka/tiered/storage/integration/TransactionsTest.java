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
package org.apache.kafka.tiered.storage.integration;

import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.api.ClusterConfig;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTemplate;
import org.apache.kafka.common.test.api.ClusterTestExtensions;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.server.config.ServerLogConfigs.MIN_IN_SYNC_REPLICAS_CONFIG;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.TOPIC1;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.consumerPositions;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTopics;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.createTransactionalProducer;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.sendOffset;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testFailureToFenceEpoch;
import static org.apache.kafka.tiered.storage.integration.TransactionTestUtils.testTimeout;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ClusterTestExtensions.class)
public class TransactionsTest {
    
    private static List<ClusterConfig> generator() {
        return TransactionTestUtils.generator(new Properties());
    }

    private static List<ClusterConfig> generateTV2Disabled() {
        return TransactionTestUtils.generateTV2Disabled(new Properties());
    }

    private static List<ClusterConfig> generateTV2Enabled() {
        return TransactionTestUtils.generateTV2Enabled(new Properties());
    }

    private Map<String, String> topicConfig() {
        return Map.of(MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }

    @ClusterTemplate("generator")
    public void testBasicTransactionsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBasicTransactions(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testBasicTransactionsWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBasicTransactions(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testReadCommittedConsumerShouldNotSeeUndecidedDataWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testReadCommittedConsumerShouldNotSeeUndecidedData(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testReadCommittedConsumerShouldNotSeeUndecidedDataWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testReadCommittedConsumerShouldNotSeeUndecidedData(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testDelayedFetchIncludesAbortedTransactionWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testDelayedFetchIncludesAbortedTransaction(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testDelayedFetchIncludesAbortedTransactionWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testDelayedFetchIncludesAbortedTransaction(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @SuppressWarnings("deprecation")
    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupIdWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), groupId), true, GroupProtocol.CLASSIC);
    }

    @SuppressWarnings("deprecation")
    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupIdWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), groupId), true, GroupProtocol.CONSUMER);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupMetadataWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata()), true, GroupProtocol.CLASSIC);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsWithGroupMetadataWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        createTopics(cluster, topicConfig());
        sendOffset(cluster, (producer, groupId, consumer) ->
                producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata()), true, GroupProtocol.CONSUMER);
    }

    @ClusterTemplate("generator")
    public void testFencingOnCommitWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnCommit(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnCommitWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnCommit(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendOffsetsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSendOffsets(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendOffsetsWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSendOffsets(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testOffsetMetadataInSendOffsetsToTransactionWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testOffsetMetadataInSendOffsetsToTransaction(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testOffsetMetadataInSendOffsetsToTransactionWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testOffsetMetadataInSendOffsetsToTransaction(cluster, GroupProtocol.CONSUMER, topicConfig());
    }
    
    @ClusterTemplate("generator")
    public void testInitTransactionsTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, false, KafkaProducer::initTransactions);
    }

    @ClusterTemplate("generator")
    public void testSendOffsetsToTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, producer -> producer.sendOffsetsToTransaction(
                Map.of(new TopicPartition(TOPIC1, 0), new OffsetAndMetadata(0)),
                new ConsumerGroupMetadata("test-group")
        ));
    }

    @ClusterTemplate("generator")
    public void testCommitTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, KafkaProducer::commitTransaction);
    }

    @ClusterTemplate("generator")
    public void testAbortTransactionTimeout(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        testTimeout(cluster, true, KafkaProducer::abortTransaction);
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSend(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnSendWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnSend(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnAddPartitionsWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnAddPartitions(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnAddPartitionsWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnAddPartitions(cluster, GroupProtocol.CONSUMER, topicConfig());
    }
    
    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2EnabledWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Enabled(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2EnabledWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Enabled(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnTransactionExpirationWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnTransactionExpiration(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testFencingOnTransactionExpirationWithClassicConsumerProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testFencingOnTransactionExpiration(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testMultipleMarkersOneLeaderWithClassicGroupProtocol(ClusterInstance cluster) throws InterruptedException {
        TransactionTestUtils.testMultipleMarkersOneLeader(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testMultipleMarkersOneLeaderWithClassicConsumerProtocol(ClusterInstance cluster) throws InterruptedException {
        TransactionTestUtils.testMultipleMarkersOneLeader(cluster, GroupProtocol.CONSUMER, topicConfig());
    }

    @ClusterTemplate("generator")
    public void testConsecutivelyRunInitTransactions(ClusterInstance cluster) {
        createTopics(cluster, topicConfig());
        try (var producer = createTransactionalProducer(cluster, "normalProducer",
                100, 2000, 4000, 1000)) {
            producer.initTransactions();
            assertThrows(IllegalStateException.class, producer::initTransactions);
        }
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2DisabledWithClassicGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Disabled(cluster, GroupProtocol.CLASSIC, topicConfig());
    }

    // FIXME
    @ClusterTemplate("generateTV2Disabled")
    public void testBumpTransactionalEpochWithTV2DisabledWithConsumerGroupProtocol(ClusterInstance cluster) throws Exception {
        TransactionTestUtils.testBumpTransactionalEpochWithTV2Disabled(cluster, GroupProtocol.CONSUMER, topicConfig());
    }
    
    @ClusterTemplate("generator")
    public void testFailureToFenceEpochTV2Disable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, false, topicConfig());
    }

    @ClusterTemplate("generateTV2Enabled")
    public void testFailureToFenceEpochTV2Enable(ClusterInstance cluster) throws Exception {
        testFailureToFenceEpoch(cluster, true, topicConfig());
    }
}
