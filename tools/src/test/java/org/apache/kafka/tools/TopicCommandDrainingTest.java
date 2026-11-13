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
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.internals.Exit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TopicCommandDrainingTest {
    private static final String TOPIC = "draining-topic";
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private MockAdminClient admin;
    private TopicCommand.TopicService service;

    @BeforeEach
    void setup() {
        admin = spy(new MockAdminClient(List.of(BROKER), BROKER));
        admin.addTopic(false, TOPIC, IntStream.range(0, 5)
            .mapToObj(partition -> new TopicPartitionInfo(partition, BROKER, List.of(BROKER), List.of(BROKER),
                List.of(), List.of(), partition >= 3, partition >= 3 ? 12345L : -1L))
            .toList(), Map.of(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1"));
        service = new TopicCommand.TopicService(admin);
        allowCreatePartitions();
        allowDeletePartitions(KafkaFuture.completedFuture(null));
    }

    @AfterEach
    void tearDown() {
        admin.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 5})
    void testAlterReopensRetainedPartitions(final int targetCount) throws Exception {
        allowCreatePartitions();

        service.alterTopic(options("--alter", "--partitions", Integer.toString(targetCount)));

        verify(admin).createPartitions(argThat(request -> request.get(TOPIC).totalCount() == targetCount), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    @Test
    void testUnchangedActiveCountDoesNotStartAnotherDeletion() throws Exception {
        service.alterTopic(options("--alter", "--partitions", "3"));

        verifyNoPartitionMutation();
    }

    @Test
    void testForceUpgradesExistingActiveTarget() throws Exception {
        allowDeletePartitions(KafkaFuture.completedFuture(null));

        service.alterTopic(options("--alter", "--partitions", "3", "--force"));

        assertDeletePartitions(3, true);
    }

    @ParameterizedTest
    @ValueSource(ints = {4, 5})
    void testForceCannotReopenRetainedPartitions(final int targetCount) {
        assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
            options("--alter", "--partitions", Integer.toString(targetCount), "--force")));

        verifyNoPartitionMutation();
    }

    @Test
    void testRelativeDeletionUsesActiveCount() throws Exception {
        allowDeletePartitions(KafkaFuture.completedFuture(null));

        service.alterTopic(options("--alter", "--delete-partitions", "1"));

        assertDeletePartitions(2, false);
    }

    @Test
    void testRelativeForceRequiresAbsoluteTargetForExistingDrain() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> service.alterTopic(options("--alter", "--delete-partitions", "1", "--force")));

        assertTrue(exception.getMessage().contains("--partitions"));
        verifyNoPartitionMutation();
    }

    @Test
    void testReopenRejectsChangedRetainedReplicaAssignment() {
        assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
            options("--alter", "--partitions", "4", "--replica-assignment", "0,0,0,1")));

        verifyNoPartitionMutation();
    }

    @Test
    void testReopenRejectsChangedAssignmentForStillDrainingPartition() {
        assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
            options("--alter", "--partitions", "4", "--replica-assignment", "0,0,0,0,1")));

        verifyNoPartitionMutation();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0,0,0", "0,0,0,0,0"})
    void testReopenRequiresExactAssignmentCount(final String assignment) {
        assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
            options("--alter", "--partitions", "4", "--replica-assignment", assignment)));

        verifyNoPartitionMutation();
    }

    @Test
    void testReopenPreservesMatchingRetainedReplicaAssignment() throws Exception {
        allowCreatePartitions();

        service.alterTopic(options("--alter", "--partitions", "4", "--replica-assignment", "0,0,0,0"));

        verify(admin).createPartitions(argThat(request -> request.get(TOPIC).totalCount() == 4 &&
            request.get(TOPIC).assignments().isEmpty()), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    @Test
    void testExpansionSlicesNewReplicaAssignmentsAfterRetainedCount() throws Exception {
        allowCreatePartitions();

        service.alterTopic(options("--alter", "--partitions", "7", "--replica-assignment", "0,0,0,0,0,1,0"));

        verify(admin).createPartitions(argThat(request -> request.get(TOPIC).totalCount() == 7 &&
            request.get(TOPIC).assignments().equals(List.of(List.of(1), List.of(0)))), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    @Test
    void testDecreaseRejectsIgnoredReplicaAssignment() {
        assertThrows(IllegalArgumentException.class, () -> service.alterTopic(
            options("--alter", "--partitions", "2", "--replica-assignment", "0,0")));

        verifyNoPartitionMutation();
    }

    @Test
    void testRelativeDeleteRejectsReplicaAssignmentOption() {
        Exit.setExitProcedure((status, message) -> {
            assertEquals(1, status);
            throw new IllegalArgumentException(message);
        });
        try {
            assertThrows(IllegalArgumentException.class,
                () -> options("--alter", "--delete-partitions", "1", "--replica-assignment", "0,0"));
        } finally {
            Exit.resetExitProcedure();
        }
        verifyNoPartitionMutation();
    }

    @Test
    void testAcceptedDeletionOutputDoesNotClaimRemoval() {
        allowDeletePartitions(KafkaFuture.completedFuture(null));

        String output = captureAlterOutput("--alter", "--partitions", "2");

        assertTrue(output.contains("Initiated partition deletion"));
        assertTrue(output.contains(TOPIC));
        assertFalse(output.contains("removed"));
    }

    @Test
    void testFailedDeletionDoesNotPrintAcceptance() {
        KafkaFutureImpl<Void> failure = new KafkaFutureImpl<>();
        failure.completeExceptionally(new IllegalArgumentException("deletion rejected"));
        allowDeletePartitions(failure);

        String output = ToolsTestUtils.captureStandardOut(() -> assertThrows(ExecutionException.class,
            () -> service.alterTopic(options("--alter", "--partitions", "2"))));

        assertFalse(output.contains("Initiated partition deletion"));
    }

    @Test
    void testDescribeShowsRetainedActiveAndDrainingCounts() {
        String output = ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.describeTopic(options("--describe"));
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        assertTrue(output.contains("PartitionCount: 5"), output);
        assertTrue(output.contains("ActivePartitionCount: 3"), output);
        assertTrue(output.contains("DrainingPartitionCount: 2"), output);
        assertEquals(2, output.lines().filter(line -> line.contains("Draining: true") &&
            line.contains("DrainDeadlineMs: 12345")).count(), output);
    }

    @Test
    void testDescribeUnknownDrainingDeadline() {
        TopicPartitionInfo partition = new TopicPartitionInfo(0, BROKER, List.of(BROKER), List.of(BROKER),
            List.of(), List.of(), true, -1L);
        TopicCommand.PartitionDescription description = new TopicCommand.PartitionDescription(TOPIC, partition,
            new org.apache.kafka.clients.admin.Config(List.of()), null);

        String output = ToolsTestUtils.captureStandardOut(description::printDescription);

        assertTrue(output.contains("Draining: true"), output);
        assertTrue(output.contains("DrainDeadlineMs: unknown"), output);
    }

    private void allowCreatePartitions() {
        CreatePartitionsResult result = mock(CreatePartitionsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        doReturn(result).when(admin).createPartitions(anyMap(), any());
    }

    private void allowDeletePartitions(final KafkaFuture<Void> completion) {
        DeletePartitionsResult result = mock(DeletePartitionsResult.class);
        when(result.all()).thenReturn(completion);
        doReturn(result).when(admin).deletePartitions(anyMap(), any());
    }

    private void assertDeletePartitions(final int targetCount, final boolean force) {
        ArgumentCaptor<DeletePartitionsOptions> options = ArgumentCaptor.forClass(DeletePartitionsOptions.class);
        verify(admin).deletePartitions(eq(Map.of(TOPIC, targetCount)), options.capture());
        assertEquals(force, options.getValue().force());
        assertFalse(options.getValue().shouldRetryOnQuotaViolation());
        verify(admin, never()).createPartitions(anyMap(), any());
    }

    private void verifyNoPartitionMutation() {
        verify(admin, never()).createPartitions(anyMap(), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    private String captureAlterOutput(final String... args) {
        return ToolsTestUtils.captureStandardOut(() -> {
            try {
                service.alterTopic(options(args));
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static TopicCommand.TopicCommandOptions options(final String... args) {
        List<String> options = new ArrayList<>(List.of(args));
        options.addAll(List.of("--bootstrap-server", "localhost:9092", "--topic", TOPIC));
        return new TopicCommand.TopicCommandOptions(options.toArray(new String[0]));
    }
}
