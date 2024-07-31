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
package org.apache.kafka.tools.consumer;

import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKey;
import org.apache.kafka.coordinator.group.generated.GroupMetadataKeyJsonConverter;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValue;
import org.apache.kafka.coordinator.group.generated.GroupMetadataValueJsonConverter;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.nio.ByteBuffer;
import java.util.Optional;

public class GroupMetadataMessageFormatter extends AbstractGroupMetadataFormatter {

    JsonNode transferMetadataToJsonNode(ApiMessage logKey, short keyVersion) {
        if (logKey instanceof OffsetCommitKey) {
            return NullNode.getInstance();
        } else if (logKey instanceof GroupMetadataKey) {
            return GroupMetadataKeyJsonConverter.write((GroupMetadataKey) logKey, keyVersion);
        } else {
            return new TextNode(UNKNOWN);
        }
    }

    @Override
    JsonNode readValueNode(ByteBuffer byteBuffer, short version) {
        return readGroupMetaValue(byteBuffer)
                .map(logValue -> GroupMetadataValueJsonConverter.write(logValue, version))
                .orElseGet(() -> new TextNode(UNKNOWN));
    }

    private Optional<GroupMetadataValue> readGroupMetaValue(ByteBuffer byteBuffer) {
        short version = byteBuffer.getShort();
        if (version >= GroupMetadataValue.LOWEST_SUPPORTED_VERSION
                && version <= GroupMetadataValue.HIGHEST_SUPPORTED_VERSION) {
            return Optional.of(new GroupMetadataValue(new ByteBufferAccessor(byteBuffer), version));
        } else {
            return Optional.empty();
        }
    }
}
