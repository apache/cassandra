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

import accord.messages.GetEphemeralReadDeps;
import accord.messages.GetEphemeralReadDeps.GetEphemeralReadDepsOk;
import accord.primitives.Deps;
import accord.primitives.Route;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetEphmrlReadDepsSerializers
{
    public static final IVersionedSerializer<GetEphemeralReadDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<GetEphemeralReadDeps>()
    {
        @Override
        public void serializeBody(GetEphemeralReadDeps msg, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(msg.executionEpoch);
        }

        @Override
        public GetEphemeralReadDeps deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            long executionEpoch = in.readUnsignedVInt();
            return GetEphemeralReadDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, executionEpoch);
        }

        @Override
        public long serializedBodySize(GetEphemeralReadDeps msg, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(msg.executionEpoch);
        }
    };

    public static final IVersionedSerializer<GetEphemeralReadDepsOk> reply = new IVersionedSerializer<GetEphemeralReadDepsOk>()
    {
        @Override
        public void serialize(GetEphemeralReadDepsOk reply, DataOutputPlus out, int version) throws IOException
        {
            DepsSerializers.deps.serialize(reply.deps, out, version);
            out.writeUnsignedVInt(reply.latestEpoch);
        }

        @Override
        public GetEphemeralReadDepsOk deserialize(DataInputPlus in, int version) throws IOException
        {
            Deps deps = DepsSerializers.deps.deserialize(in, version);
            long latestEpoch = in.readUnsignedVInt();
            return new GetEphemeralReadDepsOk(deps, latestEpoch);
        }

        @Override
        public long serializedSize(GetEphemeralReadDepsOk reply, int version)
        {
            return DepsSerializers.deps.serializedSize(reply.deps, version)
                   + TypeSizes.sizeofUnsignedVInt(reply.latestEpoch);
        }
    };
}
