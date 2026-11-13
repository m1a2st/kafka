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
package org.apache.kafka.common;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TopicPartitionInfoTest {
    private static final Node LEADER = new Node(0, "localhost", 9092);

    @Test
    void testLegacyConstructorsDefaultToActiveWithUnknownDeadline() {
        TopicPartitionInfo fourArguments = new TopicPartitionInfo(0, LEADER, List.of(LEADER), List.of(LEADER));
        TopicPartitionInfo sixArguments = new TopicPartitionInfo(0, LEADER, List.of(LEADER), List.of(LEADER), List.of(), List.of());
        assertNull(fourArguments.elr());
        assertNull(fourArguments.lastKnownElr());
        assertFalse(fourArguments.isDraining());
        assertFalse(sixArguments.isDraining());
        assertEquals(OptionalLong.empty(), fourArguments.drainDeadlineMs());
        assertEquals(OptionalLong.empty(), sixArguments.drainDeadlineMs());
    }

    @Test
    void testDrainingDescriptionIncludesEarliestRemovalTime() {
        TopicPartitionInfo info = drainingInfo(true, 123456789L);
        assertTrue(info.isDraining());
        assertEquals(OptionalLong.of(123456789L), info.drainDeadlineMs());
        assertTrue(info.toString().contains("isDraining=true"));
        assertTrue(info.toString().contains("123456789"));
    }

    @Test
    void testUnavailableDeadlineRemainsEmpty() {
        assertTrue(drainingInfo(true, -1L).isDraining());
        assertEquals(OptionalLong.empty(), drainingInfo(true, -1L).drainDeadlineMs());
        assertEquals(OptionalLong.empty(), drainingInfo(false, -1L).drainDeadlineMs());
    }

    @Test
    void testDrainStateAndDeadlineParticipateInEquality() {
        TopicPartitionInfo active = drainingInfo(false, -1L);
        TopicPartitionInfo draining = drainingInfo(true, 100L);
        TopicPartitionInfo same = drainingInfo(true, 100L);
        TopicPartitionInfo later = drainingInfo(true, 200L);
        assertNotEquals(active, draining);
        assertNotEquals(draining, later);
        assertEquals(draining, same);
        assertEquals(draining.hashCode(), same.hashCode());
    }

    private static TopicPartitionInfo drainingInfo(boolean draining, long deadlineMs) {
        return new TopicPartitionInfo(0, LEADER, List.of(LEADER), List.of(LEADER), List.of(), List.of(), draining, deadlineMs);
    }
}
