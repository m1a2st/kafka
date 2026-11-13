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

import org.apache.kafka.clients.admin.DeletePartitionsOptions;
import org.apache.kafka.clients.admin.DeletePartitionsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.internals.Exit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class TopicCommandForceTest {
    private static final String TOPIC = "force-topic";
    private static final Node BROKER = new Node(0, "localhost", 9092);
    private MockAdminClient admin;

    @BeforeEach
    void setup() {
        admin = spy(new MockAdminClient(List.of(BROKER), BROKER));
        addTopic(TOPIC, 3);
    }

    @AfterEach
    void tearDown() {
        admin.close();
    }

    @ParameterizedTest
    @CsvSource({"--partitions,false", "--partitions,true", "--delete-partitions,false", "--delete-partitions,true"})
    void testForceFlagForPartitionDecreases(final String countOption, final boolean force) throws Exception {
        DeletePartitionsResult result = mock(DeletePartitionsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        doReturn(result).when(admin).deletePartitions(anyMap(), any());
        List<String> args = new ArrayList<>(List.of("--alter", "--topic", TOPIC,
            countOption, countOption.equals("--partitions") ? "2" : "1"));
        if (force) {
            args.add("--force");
        }

        new TopicCommand.TopicService(admin).alterTopic(options(args));

        ArgumentCaptor<DeletePartitionsOptions> options = ArgumentCaptor.forClass(DeletePartitionsOptions.class);
        verify(admin).deletePartitions(eq(Map.of(TOPIC, 2)), options.capture());
        assertEquals(force, options.getValue().force());
        verify(admin, never()).createPartitions(anyMap(), any());
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 4})
    void testForceRejectsUnchangedOrIncreasedCount(final int count) {
        TopicCommand.TopicCommandOptions options = options(List.of("--alter", "--topic", TOPIC,
            "--partitions", Integer.toString(count), "--force"));

        assertThrows(IllegalArgumentException.class, () -> new TopicCommand.TopicService(admin).alterTopic(options));

        verify(admin, never()).createPartitions(anyMap(), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    @Test
    void testForceRejectsMixedIncreaseAndDecreaseBeforeMutation() {
        addTopic("force-other-topic", 1);
        TopicCommand.TopicCommandOptions options = options(List.of("--alter", "--topic", "force-.*",
            "--partitions", "2", "--force"));

        assertThrows(IllegalArgumentException.class, () -> new TopicCommand.TopicService(admin).alterTopic(options));

        verify(admin, never()).createPartitions(anyMap(), any());
        verify(admin, never()).deletePartitions(anyMap(), any());
    }

    @ParameterizedTest
    @ValueSource(strings = {"--create", "--delete", "--describe", "--list"})
    void testForceRejectsOtherActions(final String action) {
        assertInvalidOptions(List.of(action, "--topic", TOPIC, "--force"));
    }

    @Test
    void testForceRejectsReplicaAssignment() {
        assertInvalidOptions(List.of("--alter", "--topic", TOPIC, "--partitions", "2",
            "--replica-assignment", "0,0", "--force"));
    }

    private void addTopic(final String topic, final int count) {
        admin.addTopic(false, topic, IntStream.range(0, count)
            .mapToObj(partition -> new TopicPartitionInfo(partition, BROKER, List.of(BROKER), List.of(BROKER)))
            .collect(Collectors.toList()), Map.of());
    }

    private static TopicCommand.TopicCommandOptions options(final List<String> args) {
        List<String> options = new ArrayList<>(args);
        options.addAll(List.of("--bootstrap-server", "localhost:9092"));
        return new TopicCommand.TopicCommandOptions(options.toArray(new String[0]));
    }

    private static void assertInvalidOptions(final List<String> args) {
        AtomicBoolean exited = new AtomicBoolean();
        Exit.setExitProcedure((status, message) -> {
            assertEquals(1, status);
            exited.set(true);
            throw new IllegalArgumentException(message);
        });
        try {
            assertThrows(RuntimeException.class, () -> options(args));
            assertTrue(exited.get(), "The recognized --force option must be rejected by argument validation");
        } finally {
            Exit.resetExitProcedure();
        }
    }
}
