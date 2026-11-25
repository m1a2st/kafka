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

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.ThrottlingQuotaExceededException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.DeletePartitionsRequestData;
import org.apache.kafka.common.message.DeletePartitionsResponseData;
import org.apache.kafka.common.message.DeletePartitionsResponseData.DeletePartitionsTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.DeletePartitionsRequest;
import org.apache.kafka.common.requests.DeletePartitionsResponse;
import org.apache.kafka.common.requests.RequestTestUtils;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AdminRetriesDeletionReviewTest {
    private static final Node FIRST_CONTROLLER = new Node(0, "localhost", 9092);
    private static final Node SECOND_CONTROLLER = new Node(1, "localhost", 9093);
    private static final Cluster CLUSTER = new Cluster("deletion-review-cluster",
        List.of(FIRST_CONTROLLER, SECOND_CONTROLLER), List.of(), Set.of(), Set.of(), FIRST_CONTROLLER);

    @Test
    void testDisconnectDoesNotChangeCapturedQuotaAndForceOptions() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            List<DeletePartitionsRequestData> requests = new ArrayList<>();
            DeletePartitionsOptions options = new DeletePartitionsOptions().force(true).validateOnly(true)
                .retryOnQuotaViolation(false).timeoutMs(5000);
            env.kafkaClient().prepareResponse(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                options.force(false).validateOnly(false).retryOnQuotaViolation(true);
                return true;
            }, response(0, result("orders", Errors.NONE)), true);
            env.kafkaClient().prepareResponse(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(17, result("orders", Errors.THROTTLING_QUOTA_EXCEEDED)));

            DeletePartitionsResult deletion = env.adminClient().deletePartitions(Map.of("orders", 2), options);

            ThrottlingQuotaExceededException failure = TestUtils.assertFutureThrows(
                ThrottlingQuotaExceededException.class, deletion.values().get("orders"));
            assertEquals(17, failure.throttleTimeMs());
            assertEquals(2, requests.size());
            for (DeletePartitionsRequestData request : requests) {
                assertTrue(request.validateOnly());
                assertTrue(request.topics().find("orders").force());
                assertEquals(2, request.topics().find("orders").count());
            }

            env.kafkaClient().prepareResponse(response(0, result("next", Errors.NONE)));
            assertNull(env.adminClient().deletePartitions(Map.of("next", 1)).all().get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void testDisconnectWhileRetryingQuotaDoesNotResubmitOtherTopics() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            List<DeletePartitionsRequestData> requests = new ArrayList<>();
            env.kafkaClient().prepareResponse(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(0, result("accepted", Errors.NONE), result("throttled", Errors.THROTTLING_QUOTA_EXCEEDED),
                result("denied", Errors.TOPIC_AUTHORIZATION_FAILED)));
            env.kafkaClient().prepareResponse(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(0, result("throttled", Errors.NONE)), true);
            env.kafkaClient().prepareResponse(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(0, result("throttled", Errors.NONE)));

            DeletePartitionsResult deletion = env.adminClient().deletePartitions(
                Map.of("accepted", 2, "throttled", 3, "denied", 4),
                new DeletePartitionsOptions().force(true).validateOnly(true).timeoutMs(5000));

            assertNull(deletion.values().get("accepted").get(5, TimeUnit.SECONDS));
            assertNull(deletion.values().get("throttled").get(5, TimeUnit.SECONDS));
            TestUtils.assertFutureThrows(TopicAuthorizationException.class, deletion.values().get("denied"));
            assertEquals(3, requests.size());
            assertEquals(Set.of("accepted", "throttled", "denied"), requestedTopics(requests.get(0)));
            for (DeletePartitionsRequestData request : requests.subList(1, 3)) {
                assertEquals(Set.of("throttled"), requestedTopics(request));
                assertEquals(3, request.topics().find("throttled").count());
                assertTrue(request.topics().find("throttled").force());
                assertTrue(request.validateOnly());
            }
        }
    }

    @Test
    void testControllerRediscoveryPreservesTheDeletionRequest() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            List<DeletePartitionsRequestData> requests = new ArrayList<>();
            env.kafkaClient().prepareResponseFrom(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(0, result("orders", Errors.NOT_CONTROLLER)), FIRST_CONTROLLER);
            env.kafkaClient().prepareResponse(RequestTestUtils.metadataResponse(CLUSTER.nodes(),
                "deletion-review-cluster", SECOND_CONTROLLER.id(), List.of()));
            env.kafkaClient().prepareResponseFrom(request -> {
                requests.add(((DeletePartitionsRequest) request).data().duplicate());
                return true;
            }, response(0, result("orders", Errors.NONE)), SECOND_CONTROLLER);

            assertNull(env.adminClient().deletePartitions(Map.of("orders", 3),
                new DeletePartitionsOptions().force(true).validateOnly(true).timeoutMs(5000))
                .all().get(5, TimeUnit.SECONDS));

            assertEquals(2, requests.size());
            for (DeletePartitionsRequestData request : requests) {
                assertEquals(Set.of("orders"), requestedTopics(request));
                assertEquals(3, request.topics().find("orders").count());
                assertTrue(request.validateOnly());
                assertTrue(request.topics().find("orders").force());
            }
        }
    }

    @Test
    void testUnsupportedVersionAndInvalidNamesDoNotPoisonLaterRequests() throws Exception {
        try (AdminClientUnitTestEnv env = new AdminClientUnitTestEnv(CLUSTER)) {
            env.kafkaClient().prepareUnsupportedVersionResponse(request -> request instanceof DeletePartitionsRequest);
            Map<String, Integer> targets = new HashMap<>();
            targets.put(null, 1);
            targets.put("", 1);
            targets.put("orders", 2);

            DeletePartitionsResult deletion = env.adminClient().deletePartitions(targets,
                new DeletePartitionsOptions().timeoutMs(5000));

            TestUtils.assertFutureThrows(InvalidTopicException.class, deletion.values().get(null));
            TestUtils.assertFutureThrows(InvalidTopicException.class, deletion.values().get(""));
            TestUtils.assertFutureThrows(UnsupportedVersionException.class, deletion.values().get("orders"));
            env.kafkaClient().prepareResponse(response(0, result("next", Errors.NONE)));
            assertNull(env.adminClient().deletePartitions(Map.of("next", 1)).all().get(5, TimeUnit.SECONDS));
        }
    }

    private static Set<String> requestedTopics(DeletePartitionsRequestData request) {
        return request.topics().stream().map(DeletePartitionsRequestData.DeletePartitionsTopic::name)
            .collect(Collectors.toSet());
    }

    private static DeletePartitionsTopicResult result(String topic, Errors error) {
        return new DeletePartitionsTopicResult().setName(topic).setErrorCode(error.code());
    }

    private static DeletePartitionsResponse response(int throttleTimeMs, DeletePartitionsTopicResult... results) {
        return new DeletePartitionsResponse(new DeletePartitionsResponseData()
            .setThrottleTimeMs(throttleTimeMs).setResults(List.of(results)));
    }
}
