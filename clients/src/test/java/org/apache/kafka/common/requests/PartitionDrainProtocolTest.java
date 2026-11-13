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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData.PartitionDrainStatus;
import org.apache.kafka.common.message.DeletePartitionsRequestData;
import org.apache.kafka.common.message.DeletePartitionsRequestData.DeletePartitionsTopic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartitionDrainProtocolTest {
    @Test
    void testDeletePartitionsForceTagRoundTrip() {
        DeletePartitionsRequestData data = new DeletePartitionsRequestData().setValidateOnly(true);
        data.topics().add(new DeletePartitionsTopic().setName("safe-topic").setCount(2));
        data.topics().add(new DeletePartitionsTopic().setName("forced-topic").setCount(1).setForce(true));
        DeletePartitionsRequest request = new DeletePartitionsRequest.Builder(data).build((short) 0);

        DeletePartitionsRequest parsed = DeletePartitionsRequest.parse(request.serialize(), (short) 0);

        assertTrue(parsed.data().validateOnly());
        assertFalse(parsed.data().topics().find("safe-topic").force());
        assertTrue(parsed.data().topics().find("forced-topic").force());
        assertEquals(data, parsed.data());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    void testHeartbeatDrainingStatusRoundTrip(final short version) {
        Uuid topicId = Uuid.randomUuid();
        PartitionDrainStatus blocked = new PartitionDrainStatus().setTopicId(topicId).setPartitionId(2)
            .setLeaderEpoch(3).setPartitionEpoch(4).setMetadataOffset(100).setBlockedReason("pending transactions");
        PartitionDrainStatus ready = new PartitionDrainStatus().setTopicId(topicId).setPartitionId(3)
            .setLeaderEpoch(5).setPartitionEpoch(6).setMetadataOffset(101).setReady(true);
        BrokerHeartbeatRequestData data = new BrokerHeartbeatRequestData().setBrokerId(1).setBrokerEpoch(2)
            .setCurrentMetadataOffset(101).setDrainingPartitions(List.of(blocked, ready));
        BrokerHeartbeatRequest request = new BrokerHeartbeatRequest.Builder(data).build(version);

        BrokerHeartbeatRequest parsed = BrokerHeartbeatRequest.parse(request.serialize(), version);

        assertEquals(List.of(blocked, ready), parsed.data().drainingPartitions());
        assertFalse(parsed.data().drainingPartitions().get(0).ready());
        assertTrue(parsed.data().drainingPartitions().get(1).ready());
        assertEquals(data, parsed.data());
    }

    @ParameterizedTest
    @ValueSource(shorts = {0, 1, 2})
    void testHeartbeatWithoutDrainStatusRemainsCompatible(final short version) {
        BrokerHeartbeatRequest request = new BrokerHeartbeatRequest.Builder(new BrokerHeartbeatRequestData()
            .setBrokerId(1).setBrokerEpoch(2).setCurrentMetadataOffset(100)).build(version);

        BrokerHeartbeatRequest parsed = BrokerHeartbeatRequest.parse(request.serialize(), version);

        assertTrue(parsed.data().drainingPartitions().isEmpty());
        assertEquals(-1, new PartitionDrainStatus().metadataOffset());
        assertFalse(new PartitionDrainStatus().ready());
        assertEquals("", new PartitionDrainStatus().blockedReason());
    }
}
