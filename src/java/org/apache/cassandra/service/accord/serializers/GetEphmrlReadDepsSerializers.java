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

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.messages.GetEphemeralReadDeps;
import accord.messages.GetEphemeralReadDeps.GetEphemeralReadDepsOk;
import accord.primitives.Deps;
import accord.primitives.Route;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetEphmrlReadDepsSerializers
{
    public static final IVersionedSerializer<GetEphemeralReadDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<GetEphemeralReadDeps>()
    {
        @Override
        public void serializeBody(GetEphemeralReadDeps msg, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUnsignedVInt(msg.executionEpoch);
        }

        @Override
        public GetEphemeralReadDeps deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            long executionEpoch = in.readUnsignedVInt();
            return GetEphemeralReadDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, executionEpoch);
        }

        @Override
        public long serializedBodySize(GetEphemeralReadDeps msg, Version version)
        {
            return TypeSizes.sizeofUnsignedVInt(msg.executionEpoch);
        }
    };

    public static final UnversionedSerializer<GetEphemeralReadDepsOk> reply = new UnversionedSerializer<GetEphemeralReadDepsOk>()
    {
        @Override
        public void serialize(GetEphemeralReadDepsOk reply, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(reply.latestEpoch);
            out.writeBoolean(reply.deps != null);
            if (reply.deps == null)
                return;
            DepsSerializers.deps.serialize(reply.deps, out);
            out.writeUnsignedVInt32(reply.flags.bits());
        }

        @Override
        public GetEphemeralReadDepsOk deserialize(DataInputPlus in) throws IOException
        {
            long latestEpoch = in.readUnsignedVInt();
            if (!in.readBoolean())
                return new GetEphemeralReadDepsOk(latestEpoch);
            Deps deps = DepsSerializers.deps.deserialize(in);
            ExecuteFlags flags = ExecuteFlags.get(in.readUnsignedVInt32());
            return new GetEphemeralReadDepsOk(deps, latestEpoch, flags);
        }

        @Override
        public long serializedSize(GetEphemeralReadDepsOk reply)
        {
            long size = 1 + TypeSizes.sizeofUnsignedVInt(reply.latestEpoch);
            if (reply.deps != null)
            {
                size += DepsSerializers.deps.serializedSize(reply.deps)
                        + TypeSizes.sizeofUnsignedVInt(reply.flags.bits());
            }
            return size;
        }
    };
}
