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
package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.DeletePartitionsOptions;
import org.apache.kafka.clients.admin.DeletePartitionsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicCommandDeletionReviewTest {
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private static final Node OTHER_BROKER = new Node(1, "localhost", 9093);

    @Test
    void testRelativeDeletionValidatesEveryMatchedTopicBeforeMutation() {
        try (MockAdminClient admin = admin()) {
            addTopic(admin, "review-a", 5, 5, -1);
            addTopic(admin, "review-b", 2, 2, -1);
            TopicCommand.TopicService service = new TopicCommand.TopicService(admin);

            assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
                options("--delete-partitions", "2")));

            verifyNoMutation(admin);
        }
    }

    @Test
    void testRelativeForceRejectsMixedExistingDrainBeforeMutation() {
        try (MockAdminClient admin = admin()) {
            addTopic(admin, "review-a", 5, 5, -1);
            addTopic(admin, "review-b", 5, 3, -1);
            TopicCommand.TopicService service = new TopicCommand.TopicService(admin);

            assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
                options("--delete-partitions", "1", "--force")));

            verifyNoMutation(admin);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testAbsoluteTargetCanReopenAndDecreaseDifferentMatchedTopics() throws Exception {
        try (MockAdminClient admin = admin()) {
            addTopic(admin, "review-a", 5, 3, -1);
            addTopic(admin, "review-b", 6, 6, -1);

            new TopicCommand.TopicService(admin).alterTopic(options("--partitions", "4"));

            ArgumentCaptor<Map<String, NewPartitions>> increases = ArgumentCaptor.forClass(Map.class);
            verify(admin).createPartitions(increases.capture(), any());
            assertEquals(1, increases.getValue().size());
            assertEquals(4, increases.getValue().get("review-a").totalCount());
            assertNull(increases.getValue().get("review-a").assignments());
            ArgumentCaptor<DeletePartitionsOptions> deletionOptions = ArgumentCaptor.forClass(DeletePartitionsOptions.class);
            verify(admin).deletePartitions(eq(Map.of("review-b", 4)), deletionOptions.capture());
            assertFalse(deletionOptions.getValue().force());
            assertFalse(deletionOptions.getValue().shouldRetryOnQuotaViolation());
        }
    }

    @Test
    void testChangedRetainedAssignmentOnOneTopicPreventsAllMutations() {
        try (MockAdminClient admin = admin()) {
            addTopic(admin, "review-a", 5, 3, -1);
            addTopic(admin, "review-b", 5, 3, 2);

            assertThrows(IllegalArgumentException.class, () -> new TopicCommand.TopicService(admin).alterTopic(
                options("--partitions", "4", "--replica-assignment", "0,0,0,0")));

            verifyNoMutation(admin);
        }
    }

    @Test
    void testAbsoluteForceUsesSameActiveTargetAfterDifferentPartialRemovals() throws Exception {
        try (MockAdminClient admin = admin()) {
            addTopic(admin, "review-a", 5, 3, -1);
            addTopic(admin, "review-b", 4, 3, -1);

            new TopicCommand.TopicService(admin).alterTopic(options("--partitions", "3", "--force"));

            ArgumentCaptor<DeletePartitionsOptions> deletionOptions = ArgumentCaptor.forClass(DeletePartitionsOptions.class);
            verify(admin).deletePartitions(eq(Map.of("review-a", 3, "review-b", 3)), deletionOptions.capture());
            assertTrue(deletionOptions.getValue().force());
            assertFalse(deletionOptions.getValue().shouldRetryOnQuotaViolation());
            verify(admin, never()).createPartitions(anyMap(), any());
        }
    }

    private static MockAdminClient admin() {
        MockAdminClient admin = spy(new MockAdminClient(List.of(BROKER, OTHER_BROKER), BROKER));
        // Admin is the network boundary of the command. Topic listing, descriptions,
        // option parsing, and all mutation planning are real code.
        CreatePartitionsResult creation = mock(CreatePartitionsResult.class);
        when(creation.all()).thenReturn(KafkaFuture.completedFuture(null));
        doReturn(creation).when(admin).createPartitions(anyMap(), any());
        DeletePartitionsResult deletion = mock(DeletePartitionsResult.class);
        when(deletion.all()).thenReturn(KafkaFuture.completedFuture(null));
        doReturn(deletion).when(admin).deletePartitions(anyMap(), any());
        return admin;
    }

    private static void addTopic(MockAdminClient admin, String name, int retained, int active, int differentReplicaPartition) {
        List<TopicPartitionInfo> partitions = IntStream.range(0, retained).mapToObj(partition -> {
            Node replica = partition == differentReplicaPartition ? OTHER_BROKER : BROKER;
            return new TopicPartitionInfo(partition, replica, List.of(replica), List.of(replica),
                List.of(), List.of(), partition >= active, partition >= active ? 12345L : -1L);
        }).collect(Collectors.toList());
        admin.addTopic(false, name, partitions, Map.of());
    }

    private static TopicCommand.TopicCommandOptions options(String... countOptions) {
        List<String> arguments = new ArrayList<>(List.of("--bootstrap-server", "localhost:9092",
            "--alter", "--topic", "review-.*"));
        arguments.addAll(List.of(countOptions));
        return new TopicCommand.TopicCommandOptions(arguments.toArray(new String[0]));
    }

    private static void verifyNoMutation(MockAdminClient admin) {
        verify(admin, never()).createPartitions(anyMap(), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }
}
