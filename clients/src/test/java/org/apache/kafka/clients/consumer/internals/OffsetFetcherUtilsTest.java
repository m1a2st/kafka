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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.NodeApiVersions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetFetcherUtilsTest {

    private MockTime time;
    private ConsumerMetadata metadata;
    private SubscriptionState subscriptionState;
    private ApiVersions apiVersions;
    private PositionsValidator positionsValidator;
    private TopicPartition tp;

    @BeforeEach
    public void setUp() {
        time = new MockTime();
        metadata = mock(ConsumerMetadata.class);
        subscriptionState = mock(SubscriptionState.class);
        apiVersions = mock(ApiVersions.class);
        positionsValidator = mock(PositionsValidator.class);
        tp = new TopicPartition("test-topic", 0);
    }

    private OffsetFetcherUtils buildUtils(long maxAgeMs) {
        return new OffsetFetcherUtils(
            new LogContext(),
            metadata,
            subscriptionState,
            time,
            100L,           // retryBackoffMs
            apiVersions,
            positionsValidator,
            maxAgeMs
        );
    }

    @Test
    public void testHotPartitionResetsToEarliest() {
        // Feature enabled: maxAgeMs = 5_000L
        // partitionAgeMs = 1_000L (≤ threshold)
        // Assert returned strategy is AutoOffsetResetStrategy.EARLIEST
        OffsetFetcherUtils utils = buildUtils(5_000L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        when(metadata.partitionAgeMs(tp)).thenReturn(1_000L);
        when(apiVersions.all()).thenReturn(createBrokerVersions(14));

        Map<TopicPartition, AutoOffsetResetStrategy> result = utils.getOffsetResetStrategyForPartitions();

        assertEquals(AutoOffsetResetStrategy.EARLIEST, result.get(tp));
    }

    @Test
    public void testColdPartitionKeepsLatest() {
        // Feature enabled: maxAgeMs = 5_000L
        // partitionAgeMs = 10_000L (> threshold)
        // Assert strategy is AutoOffsetResetStrategy.LATEST
        OffsetFetcherUtils utils = buildUtils(5_000L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        when(metadata.partitionAgeMs(tp)).thenReturn(10_000L);
        when(apiVersions.all()).thenReturn(createBrokerVersions(14));

        Map<TopicPartition, AutoOffsetResetStrategy> result = utils.getOffsetResetStrategyForPartitions();

        assertEquals(AutoOffsetResetStrategy.LATEST, result.get(tp));
    }

    @Test
    public void testUnknownAgeDefersReset() {
        // Feature enabled: maxAgeMs = 5_000L
        // partitionAgeMs = -1L (metadata not yet populated)
        // Partition must be absent from result (deferred) and a metadata update must be requested.
        OffsetFetcherUtils utils = buildUtils(5_000L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        when(metadata.partitionAgeMs(tp)).thenReturn(-1L);
        when(apiVersions.all()).thenReturn(createBrokerVersions(14));

        Map<TopicPartition, AutoOffsetResetStrategy> result = utils.getOffsetResetStrategyForPartitions();

        assertTrue(result.isEmpty(), "Partition with unknown age must be deferred (not present in result)");
        verify(metadata).requestUpdate(false);
    }

    @Test
    public void testFeatureDisabledKeepsLatest() {
        // maxAgeMs = -1L (feature off)
        // partitionAgeMs = 100L (would be "hot" if enabled)
        // Assert strategy is AutoOffsetResetStrategy.LATEST
        OffsetFetcherUtils utils = buildUtils(-1L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        // partitionAgeMs won't be called, and apiVersions.all() won't be called either

        Map<TopicPartition, AutoOffsetResetStrategy> result = utils.getOffsetResetStrategyForPartitions();

        assertEquals(AutoOffsetResetStrategy.LATEST, result.get(tp));
    }

    @Test
    public void testBrokerVersionCheckThrowsForOldBroker() {
        // Feature enabled: maxAgeMs = 5_000L
        // apiVersions.all() returns a map with one broker having METADATA maxVersion = 13
        // Assert getOffsetResetStrategyForPartitions() throws UnsupportedVersionException
        OffsetFetcherUtils utils = buildUtils(5_000L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        when(apiVersions.all()).thenReturn(createBrokerVersions(13));

        assertThrows(UnsupportedVersionException.class, () -> utils.getOffsetResetStrategyForPartitions());
    }

    @Test
    public void testBrokerVersionCheckPassesForBrokerSupportingV14() {
        // Feature enabled: maxAgeMs = 5_000L
        // Broker METADATA maxVersion = 14
        // Set partitionAgeMs = 1_000L (hot partition) and strategy = LATEST
        // Assert no exception and strategy is EARLIEST (hot partition logic proceeds)
        OffsetFetcherUtils utils = buildUtils(5_000L);

        when(subscriptionState.partitionsNeedingReset(anyLong())).thenReturn(Set.of(tp));
        when(subscriptionState.resetStrategy(tp)).thenReturn(AutoOffsetResetStrategy.LATEST);
        when(metadata.partitionAgeMs(tp)).thenReturn(1_000L);
        when(apiVersions.all()).thenReturn(createBrokerVersions(14));

        Map<TopicPartition, AutoOffsetResetStrategy> result = utils.getOffsetResetStrategyForPartitions();

        assertEquals(AutoOffsetResetStrategy.EARLIEST, result.get(tp));
    }

    private Map<String, NodeApiVersions> createBrokerVersions(int metadataMaxVersion) {
        ApiVersionsResponseData.ApiVersion metadataVersion = new ApiVersionsResponseData.ApiVersion()
            .setApiKey(ApiKeys.METADATA.id)
            .setMinVersion((short) 0)
            .setMaxVersion((short) metadataMaxVersion);

        NodeApiVersions nodeApiVersions = NodeApiVersions.create(Set.of(metadataVersion));
        Map<String, NodeApiVersions> brokerVersions = new HashMap<>();
        brokerVersions.put("broker-1", nodeApiVersions);
        return brokerVersions;
    }
}
