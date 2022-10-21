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

import accord.messages.GetDeps;
import accord.messages.GetDeps.GetDepsOk;
import accord.primitives.PartialRoute;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetDepsSerializers
{
    public static final IVersionedSerializer<GetDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<GetDeps>()
    {
        @Override
        public void serializeBody(GetDeps msg, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.seekables.serialize(msg.keys, out, version);
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            CommandSerializers.kind.serialize(msg.kind, out, version);
        }

        @Override
        public GetDeps deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch, long minEpoch, boolean doNotComputeProgressKey) throws IOException
        {
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in, version);
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            Txn.Kind kind = CommandSerializers.kind.deserialize(in, version);
            return GetDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, doNotComputeProgressKey, keys, executeAt, kind);
        }

        @Override
        public long serializedBodySize(GetDeps msg, int version)
        {
            return KeySerializers.seekables.serializedSize(msg.keys, version)
                   + CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
                   + CommandSerializers.kind.serializedSize(msg.kind, version);
        }
    };

    public static final IVersionedSerializer<GetDepsOk> reply = new IVersionedSerializer<GetDepsOk>()
    {
        @Override
        public void serialize(GetDepsOk reply, DataOutputPlus out, int version) throws IOException
        {
            DepsSerializer.partialDeps.serialize(reply.deps, out, version);
        }

        @Override
        public GetDepsOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new GetDepsOk(DepsSerializer.partialDeps.deserialize(in, version));
        }

        @Override
        public long serializedSize(GetDepsOk reply, int version)
        {
            return DepsSerializer.partialDeps.serializedSize(reply.deps, version);
        }
    };
}
