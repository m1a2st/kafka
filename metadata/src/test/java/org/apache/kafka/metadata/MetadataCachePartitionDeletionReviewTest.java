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

package org.apache.kafka.metadata;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.metadata.PartitionDrainingRecord;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpoint;
import org.apache.kafka.common.metadata.RegisterBrokerRecord.BrokerEndpointCollection;
import org.apache.kafka.common.metadata.RemovePartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.server.common.KRaftVersion;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.metadata.MetadataCacheFixtures.updateCache;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetadataCachePartitionDeletionReviewTest {
    private static final String NAME = "cache-deletion-review";
    private static final ListenerName LISTENER = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT);
    private final Uuid topicId = Uuid.randomUuid();
    private final KRaftMetadataCache cache = new KRaftMetadataCache(0, () -> KRaftVersion.KRAFT_VERSION_0);

    MetadataCachePartitionDeletionReviewTest() {
        List<ApiMessage> records = new ArrayList<>();
        records.add(new RegisterBrokerRecord().setBrokerId(0).setFenced(false)
            .setEndPoints(new BrokerEndpointCollection(List.of(new BrokerEndpoint().setHost("localhost").setPort(9092)
                .setName(LISTENER.value()).setSecurityProtocol(SecurityProtocol.PLAINTEXT.id)))));
        records.add(new TopicRecord().setName(NAME).setTopicId(topicId));
        for (int id = 0; id < 4; id++) {
            records.add(new PartitionRecord().setTopicId(topicId).setPartitionId(id).setLeader(0)
                .setReplicas(List.of(0)).setIsr(List.of(0)));
        }
        records.add(drain(2, 100L));
        records.add(drain(3, 100L));
        updateCache(cache, records);
    }

    private PartitionDrainingRecord drain(int partitionId, long deadline) {
        return new PartitionDrainingRecord().setTopicId(topicId).setPartitionId(partitionId).setDeadlineMs(deadline);
    }

    private List<MetadataResponsePartition> metadata(short version) {
        var response = cache.getTopicMetadata(Set.of(NAME), LISTENER, false, false, version);
        assertEquals(1, response.size());
        assertEquals(Errors.NONE.code(), response.get(0).errorCode());
        return response.get(0).partitions();
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 9, 13})
    void testLegacyConsumerMetadataRetainsReadableDrainingPartitions(short version) {
        // Metadata requests have no producer/consumer selector. A subscribing legacy consumer
        // needs the retained tail in this response to keep its assignment and drain the data.
        assertEquals(Set.of(0, 1, 2, 3), metadata(version).stream()
            .map(MetadataResponsePartition::partitionIndex).collect(Collectors.toSet()),
            "Readable draining partitions must remain discoverable to legacy consumers until removal");
    }

    @Test
    void testModernMetadataDistinguishesCancellationFromPhysicalRemoval() {
        assertEquals(Set.of(2, 3), metadata((short) 14).stream().filter(MetadataResponsePartition::isDraining)
            .map(MetadataResponsePartition::partitionIndex).collect(Collectors.toSet()));

        updateCache(cache, List.of(drain(2, -1L), new RemovePartitionRecord().setTopicId(topicId).setPartitionId(3)));

        List<MetadataResponsePartition> after = metadata((short) 14);
        assertEquals(Set.of(0, 1, 2), after.stream().map(MetadataResponsePartition::partitionIndex).collect(Collectors.toSet()));
        assertTrue(after.stream().noneMatch(MetadataResponsePartition::isDraining));
        assertEquals(4, cache.currentImage().topics().getTopic(topicId).partitionIdHighWatermark());
    }

    @Test
    void testDescribePaginationKeepsRetainedDrainingTailAfterPartialRemoval() {
        updateCache(cache, List.of(new RemovePartitionRecord().setTopicId(topicId).setPartitionId(3)));
        var first = cache.describeTopicResponse(List.of(NAME).iterator(), LISTENER, ignored -> 1, 1, false);
        var firstPartition = first.topics().find(NAME).partitions().get(0);
        assertEquals(1, firstPartition.partitionIndex());
        assertFalse(firstPartition.isDraining());
        assertEquals(2, first.nextCursor().partitionIndex());

        var last = cache.describeTopicResponse(List.of(NAME).iterator(), LISTENER,
            ignored -> first.nextCursor().partitionIndex(), 1, false);
        var lastPartition = last.topics().find(NAME).partitions().get(0);
        assertEquals(2, lastPartition.partitionIndex());
        assertTrue(lastPartition.isDraining());
        assertEquals(100L, lastPartition.drainDeadlineMs());
        assertNull(last.nextCursor());
    }
}
