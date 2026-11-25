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
package org.apache.kafka.clients.producer;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.requests.ProduceRequest;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProducerRoutingDeletionReviewTest {
    private static final String TOPIC = "producer-routing-deletion-review";
    private static final Uuid TOPIC_ID = Uuid.randomUuid();
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private static final byte[] KEY = "abc".getBytes(StandardCharsets.UTF_8);
    private static final byte[] VALUE = new byte[] {1, 2, 3};

    @Test
    void testKeyedSendRehashesToActivePartitionAfterDrain() throws Exception {
        try (ProducerEnvironment env = new ProducerEnvironment()) {
            // Kafka's positive murmur2 hash of "abc" is 479470107: %4 = 3, %2 = 1.
            assertEquals(3, env.send(new ProducerRecord<>(TOPIC, KEY, VALUE)).partition());

            env.updateActiveCount(2);

            assertEquals(1, env.send(new ProducerRecord<>(TOPIC, KEY, VALUE)).partition());
        }
    }

    @Test
    void testUnkeyedSendsConvergeToTheOnlyActivePartition() throws Exception {
        try (ProducerEnvironment env = new ProducerEnvironment()) {
            env.send(new ProducerRecord<>(TOPIC, VALUE));
            env.updateActiveCount(1);

            for (int i = 0; i < 8; i++) {
                assertEquals(0, env.send(new ProducerRecord<>(TOPIC, VALUE)).partition());
            }
        }
    }

    @Test
    void testExplicitDrainingPartitionFailsWithoutPreventingActiveSend() throws Exception {
        try (ProducerEnvironment env = new ProducerEnvironment()) {
            env.updateActiveCount(2);

            ExecutionException failure = assertThrows(ExecutionException.class, () -> env.producer.send(
                new ProducerRecord<>(TOPIC, 3, KEY, VALUE)).get(5, TimeUnit.SECONDS));

            assertInstanceOf(TimeoutException.class, failure.getCause());
            assertEquals(0, env.send(new ProducerRecord<>(TOPIC, 0, KEY, VALUE)).partition());
        }
    }

    @Test
    void testReopenedPartitionAcceptsAnExplicitSendAgain() throws Exception {
        try (ProducerEnvironment env = new ProducerEnvironment()) {
            env.updateActiveCount(2);
            assertEquals(2, env.producer.partitionsFor(TOPIC).size());

            env.updateActiveCount(3);

            assertEquals(3, env.producer.partitionsFor(TOPIC).size());
            assertEquals(2, env.send(new ProducerRecord<>(TOPIC, 2, KEY, VALUE)).partition());
        }
    }

    private static final class ProducerEnvironment implements AutoCloseable {
        private final ProducerMetadata metadata;
        private final MockClient client;
        private final KafkaProducer<byte[], byte[]> producer;
        private int epoch = 1;

        private ProducerEnvironment() {
            metadata = new ProducerMetadata(0, 0, 60000, 60000,
                new LogContext(), new ClusterResourceListeners(), Time.SYSTEM);
            metadata.add(TOPIC, Time.SYSTEM.milliseconds());
            client = new MockClient(Time.SYSTEM, metadata);
            updateActiveCount(4);
            ByteArraySerializer keySerializer = new ByteArraySerializer();
            ByteArraySerializer valueSerializer = new ByteArraySerializer();
            Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 200,
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 3000,
                ProducerConfig.LINGER_MS_CONFIG, 0);
            producer = new KafkaProducer<>(new ProducerConfig(ProducerConfig.appendSerializerToConfig(
                config, keySerializer, valueSerializer)), keySerializer, valueSerializer, metadata, client,
                null, new ApiVersions(), Time.SYSTEM);
        }

        private void updateActiveCount(int activeCount) {
            int leaderEpoch = epoch++;
            List<MetadataResponse.PartitionMetadata> partitions = IntStream.range(0, 4)
                .mapToObj(partition -> new MetadataResponse.PartitionMetadata(Errors.NONE,
                    new TopicPartition(TOPIC, partition), Optional.of(0), Optional.of(leaderEpoch),
                    List.of(0), List.of(0), List.of(), partition >= activeCount))
                .collect(Collectors.toList());
            client.updateMetadata(RequestTestUtils.metadataResponse(List.of(BROKER), "review-cluster", 0, List.of(
                new MetadataResponse.TopicMetadata(Errors.NONE, TOPIC, TOPIC_ID, false, partitions,
                    MetadataResponse.AUTHORIZED_OPERATIONS_OMITTED))));
        }

        private RecordMetadata send(ProducerRecord<byte[], byte[]> record) throws Exception {
            // Only the network is simulated. KafkaProducer, metadata, accumulator, partitioner,
            // sender, callback completion, and RecordMetadata are real production components.
            ProduceResponseData response = new ProduceResponseData();
            client.prepareResponse(request -> {
                if (!(request instanceof ProduceRequest))
                    return false;
                ProduceRequest produce = (ProduceRequest) request;
                produce.data().topicData().forEach(topic -> {
                    ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse()
                        .setName(TOPIC).setTopicId(topic.topicId());
                    topic.partitionData().forEach(partition -> topicResponse.partitionResponses().add(
                        new ProduceResponseData.PartitionProduceResponse().setIndex(partition.index())
                            .setErrorCode(Errors.NONE.code()).setBaseOffset(0).setLogStartOffset(0)));
                    response.responses().add(topicResponse);
                });
                return true;
            }, new ProduceResponse(response));
            return producer.send(record).get(5, TimeUnit.SECONDS);
        }

        @Override
        public void close() {
            producer.close(Duration.ZERO);
        }
    }
}
