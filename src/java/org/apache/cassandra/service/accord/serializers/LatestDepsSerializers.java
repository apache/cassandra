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

import accord.api.RoutingKey;
import accord.messages.GetLatestDeps;
import accord.messages.GetLatestDeps.GetLatestDepsOk;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Known;
import accord.primitives.LatestDeps;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class LatestDepsSerializers
{
    public static final IVersionedSerializer<LatestDeps> latestDeps = new IVersionedSerializer<LatestDeps>()
    {
        @Override
        public void serialize(LatestDeps t, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(t.size());
            for (int i = 0 ; i < t.size() ; ++i)
            {
                RoutingKey start = t.startAt(i);
                KeySerializers.routingKey.serialize(start, out, version);
                LatestDeps.LatestEntry e = t.valueAt(i);
                if (e == null)
                {
                    CommandSerializers.nullableKnownDeps.serialize(null, out, version);
                }
                else
                {
                    CommandSerializers.nullableKnownDeps.serialize(e.known, out, version);
                    CommandSerializers.ballot.serialize(e.ballot, out, version);
                    DepsSerializers.nullableDeps.serialize(e.coordinatedDeps, out, version);
                    DepsSerializers.nullableDeps.serialize(e.localDeps, out, version);
                }
            }
            KeySerializers.routingKey.serialize(t.startAt(t.size()), out, version);
        }

        @Override
        public LatestDeps deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            RoutingKey[] starts = new RoutingKey[size + 1];
            LatestDeps.LatestEntry[] values = new LatestDeps.LatestEntry[size];
            for (int i = 0 ; i < size ; ++i)
            {
                starts[i] = KeySerializers.routingKey.deserialize(in, version);
                Known.KnownDeps knownDeps = CommandSerializers.nullableKnownDeps.deserialize(in, version);
                if (knownDeps == null)
                    continue;

                Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
                Deps coordinatedDeps = DepsSerializers.nullableDeps.deserialize(in, version);
                Deps localDeps = DepsSerializers.nullableDeps.deserialize(in, version);
                values[i] = new LatestDeps.LatestEntry(knownDeps, ballot, coordinatedDeps, localDeps);
            }
            starts[size] = KeySerializers.routingKey.deserialize(in, version);

            return LatestDeps.SerializerSupport.create(true, starts, values);
        }

        @Override
        public long serializedSize(LatestDeps t, int version)
        {
            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(t.size());
            for (int i = 0 ; i < t.size() ; ++i)
            {
                RoutingKey start = t.startAt(i);
                size += KeySerializers.routingKey.serializedSize(start, version);
                LatestDeps.LatestEntry e = t.valueAt(i);
                if (e == null)
                {
                    size += CommandSerializers.nullableKnownDeps.serializedSize(null, version);
                }
                else
                {
                    size += CommandSerializers.nullableKnownDeps.serializedSize(e.known, version);
                    size += CommandSerializers.ballot.serializedSize(e.ballot, version);
                    size += DepsSerializers.nullableDeps.serializedSize(e.coordinatedDeps, version);
                    size += DepsSerializers.nullableDeps.serializedSize(e.localDeps, version);
                }
            }
            size += KeySerializers.routingKey.serializedSize(t.startAt(t.size()), version);
            return size;
        }
    };

    public static final IVersionedSerializer<GetLatestDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<>()
    {
        @Override
        public void serializeBody(GetLatestDeps msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
        }

        @Override
        public GetLatestDeps deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            return GetLatestDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, executeAt);
        }

        @Override
        public long serializedBodySize(GetLatestDeps msg, int version)
        {
            return CommandSerializers.timestamp.serializedSize(msg.executeAt, version);
        }
    };

    public static final IVersionedSerializer<GetLatestDepsOk> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(GetLatestDepsOk reply, DataOutputPlus out, int version) throws IOException
        {
            latestDeps.serialize(reply.deps, out, version);
        }

        @Override
        public GetLatestDepsOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new GetLatestDepsOk(latestDeps.deserialize(in, version));
        }

        @Override
        public long serializedSize(GetLatestDepsOk reply, int version)
        {
            return latestDeps.serializedSize(reply.deps, version);
        }
    };
}
