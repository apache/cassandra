/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.replication;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

/**
 * {@code NoPayload} cannot be used here because it is special-cased in
 * {@code Message.Serializer.payloadSize}, which would clash with the embedded
 * version byte written by the {@code mtEmbedded} wrapper.
 */
public class TransferFailedResponse
{
    public static final TransferFailedResponse instance = new TransferFailedResponse();

    private TransferFailedResponse() {}

    public static final VersionedSerializer<TransferFailedResponse> serializer = new VersionedSerializer<>()
    {
        @Override
        public void serialize(TransferFailedResponse t, DataOutputPlus out, Version version)
        {
        }

        @Override
        public TransferFailedResponse deserialize(DataInputPlus in, Version version)
        {
            return instance;
        }

        @Override
        public long serializedSize(TransferFailedResponse t, Version version)
        {
            return 0;
        }
    };

    @Override
    public String toString()
    {
        return "TransferFailedResponse{}";
    }
}
