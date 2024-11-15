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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.messages.CalculateDeps;
import accord.messages.CalculateDeps.CalculateDepsOk;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class CalculateDepsSerializers
{
    public static final IVersionedSerializer<CalculateDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<>()
    {
        @Override
        public void serializeBody(CalculateDeps msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
        }

        @Override
        public CalculateDeps deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            return CalculateDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, executeAt);
        }

        @Override
        public long serializedBodySize(CalculateDeps msg, int version)
        {
            return CommandSerializers.timestamp.serializedSize(msg.executeAt, version);
        }
    };

    public static final IVersionedSerializer<CalculateDepsOk> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(CalculateDepsOk reply, DataOutputPlus out, int version) throws IOException
        {
            DepsSerializers.deps.serialize(reply.deps, out, version);
        }

        @Override
        public CalculateDepsOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new CalculateDepsOk(DepsSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedSize(CalculateDepsOk reply, int version)
        {
            return DepsSerializers.deps.serializedSize(reply.deps, version);
        }
    };
}
