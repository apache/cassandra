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
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;

public class LatestDepsSerializers
{
    public static final UnversionedSerializer<LatestDeps> latestDeps = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(LatestDeps t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.size());
            if (t.size() == 0)
                return;

            for (int i = 0 ; i < t.size() ; ++i)
            {
                RoutingKey start = t.startAt(i);
                KeySerializers.routingKey.serialize(start, out);
                LatestDeps.LatestEntry e = t.valueAt(i);
                if (e == null)
                {
                    CommandSerializers.knownDeps.serialize(null, out);
                }
                else
                {
                    CommandSerializers.knownDeps.serialize(e.known, out);
                    CommandSerializers.ballot.serialize(e.ballot, out);
                    DepsSerializers.nullableDeps.serialize(e.coordinatedDeps, out);
                    DepsSerializers.nullableDeps.serialize(e.localDeps, out);
                }
            }
            KeySerializers.routingKey.serialize(t.startAt(t.size()), out);
        }

        @Override
        public LatestDeps deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            if (size == 0)
                return LatestDeps.EMPTY;

            RoutingKey[] starts = new RoutingKey[size + 1];
            LatestDeps.LatestEntry[] values = new LatestDeps.LatestEntry[size];
            for (int i = 0 ; i < size ; ++i)
            {
                starts[i] = KeySerializers.routingKey.deserialize(in);
                Known.KnownDeps knownDeps = CommandSerializers.knownDeps.deserialize(in);
                if (knownDeps == null)
                    continue;

                Ballot ballot = CommandSerializers.ballot.deserialize(in);
                Deps coordinatedDeps = DepsSerializers.nullableDeps.deserialize(in);
                Deps localDeps = DepsSerializers.nullableDeps.deserialize(in);
                values[i] = new LatestDeps.LatestEntry(knownDeps, ballot, coordinatedDeps, localDeps);
            }
            starts[size] = KeySerializers.routingKey.deserialize(in);

            return LatestDeps.SerializerSupport.create(true, starts, values);
        }

        @Override
        public long serializedSize(LatestDeps t)
        {
            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(t.size());
            if (t.size() == 0)
                return size;
            for (int i = 0 ; i < t.size() ; ++i)
            {
                RoutingKey start = t.startAt(i);
                size += KeySerializers.routingKey.serializedSize(start);
                LatestDeps.LatestEntry e = t.valueAt(i);
                if (e == null)
                {
                    size += CommandSerializers.knownDeps.serializedSize(null);
                }
                else
                {
                    size += CommandSerializers.knownDeps.serializedSize(e.known);
                    size += CommandSerializers.ballot.serializedSize(e.ballot);
                    size += DepsSerializers.nullableDeps.serializedSize(e.coordinatedDeps);
                    size += DepsSerializers.nullableDeps.serializedSize(e.localDeps);
                }
            }
            size += KeySerializers.routingKey.serializedSize(t.startAt(t.size()));
            return size;
        }
    };

    public static final IVersionedSerializer<GetLatestDeps> request = new TxnRequestSerializer.WithUnsyncedSerializer<>()
    {
        @Override
        public void serializeBody(GetLatestDeps msg, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.ballot.serialize(msg.ballot, out);
            ExecuteAtSerializer.serialize(msg.executeAt, out);
        }

        @Override
        public GetLatestDeps deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            Timestamp executeAt = ExecuteAtSerializer.deserialize(in);
            return GetLatestDeps.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, ballot, executeAt);
        }

        @Override
        public long serializedBodySize(GetLatestDeps msg, Version version)
        {
            return CommandSerializers.ballot.serializedSize(msg.ballot)
                   + ExecuteAtSerializer.serializedSize(msg.executeAt);
        }
    };

    public static final UnversionedSerializer<GetLatestDepsOk> reply = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(GetLatestDepsOk reply, DataOutputPlus out) throws IOException
        {
            latestDeps.serialize(reply.deps, out);
        }

        @Override
        public GetLatestDepsOk deserialize(DataInputPlus in) throws IOException
        {
            return new GetLatestDepsOk(latestDeps.deserialize(in));
        }

        @Override
        public long serializedSize(GetLatestDepsOk reply)
        {
            return latestDeps.serializedSize(reply.deps);
        }
    };
}
