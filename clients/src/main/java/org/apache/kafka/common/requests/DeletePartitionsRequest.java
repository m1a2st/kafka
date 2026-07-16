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

import org.apache.kafka.common.message.DeletePartitionsRequestData;
import org.apache.kafka.common.message.DeletePartitionsRequestData.DeletePartitionsTopic;
import org.apache.kafka.common.message.DeletePartitionsResponseData;
import org.apache.kafka.common.message.DeletePartitionsResponseData.DeletePartitionsTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Readable;

public class DeletePartitionsRequest extends AbstractRequest {

    private final DeletePartitionsRequestData data;

    public static class Builder extends AbstractRequest.Builder<DeletePartitionsRequest> {

        private final DeletePartitionsRequestData data;

        public Builder(DeletePartitionsRequestData data) {
            super(ApiKeys.DELETE_PARTITIONS);
            this.data = data;
        }

        @Override
        public DeletePartitionsRequest build(short version) {
            return new DeletePartitionsRequest(data, version);
        }

        @Override
        public String toString() {
            return data.toString();
        }
    }

    DeletePartitionsRequest(DeletePartitionsRequestData data, short apiVersion) {
        super(ApiKeys.DELETE_PARTITIONS, apiVersion);
        this.data = data;
    }

    @Override
    public DeletePartitionsRequestData data() {
        return data;
    }

    @Override
    public AbstractResponse getErrorResponse(int throttleTimeMs, Throwable e) {
        DeletePartitionsResponseData response = new DeletePartitionsResponseData();
        response.setThrottleTimeMs(throttleTimeMs);

        ApiError apiError = ApiError.fromThrowable(e);
        for (DeletePartitionsTopic topic : data.topics()) {
            response.results().add(new DeletePartitionsTopicResult()
                    .setName(topic.name())
                    .setErrorCode(apiError.error().code())
                    .setErrorMessage(apiError.message())
            );
        }
        return new DeletePartitionsResponse(response);
    }

    public static DeletePartitionsRequest parse(Readable readable, short version) {
        return new DeletePartitionsRequest(new DeletePartitionsRequestData(readable, version), version);
    }
}
