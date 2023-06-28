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

import accord.api.Data;
import accord.api.Result;
import accord.api.RoutingKey;
import accord.impl.AbstractFetchCoordinator.FetchRequest;
import accord.impl.AbstractFetchCoordinator.FetchResponse;
import accord.local.SaveStatus;
import accord.local.Status.Durability;
import accord.local.Status.Known;
import accord.messages.Propagate;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadReply;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamData;
import org.apache.cassandra.service.accord.AccordFetchCoordinator.StreamingTxn;
import org.apache.cassandra.utils.CastingSerializer;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class FetchSerializers
{
    public static final IVersionedSerializer<FetchRequest> request = new IVersionedSerializer<FetchRequest>()
    {
        @Override
        public void serialize(FetchRequest request, DataOutputPlus out, int version) throws IOException
        {
            Invariants.checkArgument(request.txnId.epoch() == request.executeAt.epoch());

            out.writeUnsignedVInt(request.waitForEpoch());
            CommandSerializers.txnId.serialize(request.txnId, out, version);
            KeySerializers.ranges.serialize((Ranges) request.readScope, out, version);
            DepsSerializer.partialDeps.serialize(request.partialDeps, out, version);
            StreamingTxn.serializer.serialize(request.read, out, version);
            out.writeBoolean(request.collectMaxApplied);
        }

        @Override
        public FetchRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FetchRequest(in.readUnsignedVInt(),
                                    CommandSerializers.txnId.deserialize(in, version),
                                    KeySerializers.ranges.deserialize(in, version),
                                    DepsSerializer.partialDeps.deserialize(in, version),
                                    StreamingTxn.serializer.deserialize(in, version),
                                    in.readBoolean());
        }

        @Override
        public long serializedSize(FetchRequest request, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(request.waitForEpoch())
                   + CommandSerializers.txnId.serializedSize(request.txnId, version)
                   + KeySerializers.ranges.serializedSize((Ranges) request.readScope, version)
                   + DepsSerializer.partialDeps.serializedSize(request.partialDeps, version)
                   + StreamingTxn.serializer.serializedSize(request.read, version)
                   + TypeSizes.BYTE_SIZE;
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<ReadReply>()
    {
        final ReadData.ReadNack[] nacks = ReadData.ReadNack.values();
        final IVersionedSerializer<Data> streamDataSerializer = new CastingSerializer<>(StreamData.class, StreamData.serializer);

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((ReadData.ReadNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            FetchResponse response = (FetchResponse) reply;
            serializeNullable(response.unavailable, out, version, KeySerializers.ranges);
            serializeNullable(response.data, out, version, streamDataSerializer);
            CommandSerializers.nullableTimestamp.serialize(response.maxApplied, out, version);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            return new FetchResponse(deserializeNullable(in, version, KeySerializers.ranges),
                                     deserializeNullable(in, version, streamDataSerializer),
                                     CommandSerializers.nullableTimestamp.deserialize(in, version));
        }

        @Override
        public long serializedSize(ReadReply reply, int version)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            FetchResponse response = (FetchResponse) reply;
            return TypeSizes.BYTE_SIZE
                   + serializedNullableSize(response.unavailable, version, KeySerializers.ranges)
                   + serializedNullableSize(response.data, version, streamDataSerializer)
                   + CommandSerializers.nullableTimestamp.serializedSize(response.maxApplied, version);
        }
    };

    public static final IVersionedSerializer<Propagate> propagate = new IVersionedSerializer<Propagate>()
    {
        @Override
        public void serialize(Propagate p, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(p.txnId, out, version);
            KeySerializers.route.serialize(p.route, out, version);
            CommandSerializers.saveStatus.serialize(p.saveStatus, out, version);
            CommandSerializers.saveStatus.serialize(p.maxSaveStatus, out, version);
            CommandSerializers.durability.serialize(p.durability, out, version);
            KeySerializers.nullableRoutingKey.serialize(p.homeKey, out, version);
            KeySerializers.nullableRoutingKey.serialize(p.progressKey, out, version);
            CommandSerializers.known.serialize(p.achieved, out, version);
            CommandSerializers.nullablePartialTxn.serialize(p.partialTxn, out, version);
            DepsSerializer.nullablePartialDeps.serialize(p.partialDeps, out, version);
            out.writeLong(p.toEpoch);
            CommandSerializers.nullableTimestamp.serialize(p.executeAt, out, version);
            CommandSerializers.nullableWrites.serialize(p.writes, out, version);
        }

        @Override
        public Propagate deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Route<?> route = KeySerializers.route.deserialize(in, version);
            SaveStatus saveStatus = CommandSerializers.saveStatus.deserialize(in, version);
            SaveStatus maxSaveStatus = CommandSerializers.saveStatus.deserialize(in, version);
            Durability durability = CommandSerializers.durability.deserialize(in, version);
            RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
            RoutingKey progressKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
            Known achieved = CommandSerializers.known.deserialize(in, version);
            PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps partialDeps = DepsSerializer.nullablePartialDeps.deserialize(in, version);
            long toEpoch = in.readLong();
            Timestamp executeAt = CommandSerializers.nullableTimestamp.deserialize(in, version);
            Writes writes = CommandSerializers.nullableWrites.deserialize(in, version);

            Result result = null;
            switch (saveStatus)
            {
                case PreApplied:
                case Applying:
                case Applied:
                case TruncatedApply:
                case TruncatedApplyWithOutcome:
                case TruncatedApplyWithDeps:
                    result = Result.APPLIED;
                    break;
                case Invalidated:
                    result = Result.INVALIDATED;
                    break;
            }

            return Propagate.SerializerSupport.create(txnId, route, saveStatus, maxSaveStatus, durability, homeKey, progressKey, achieved, partialTxn, partialDeps, toEpoch, executeAt, writes, result);
        }

        @Override
        public long serializedSize(Propagate p, int version)
        {
            return CommandSerializers.txnId.serializedSize(p.txnId, version)
                 + KeySerializers.route.serializedSize(p.route, version)
                 + CommandSerializers.saveStatus.serializedSize(p.saveStatus, version)
                 + CommandSerializers.saveStatus.serializedSize(p.maxSaveStatus, version)
                 + CommandSerializers.durability.serializedSize(p.durability, version)
                 + KeySerializers.nullableRoutingKey.serializedSize(p.homeKey, version)
                 + KeySerializers.nullableRoutingKey.serializedSize(p.progressKey, version)
                 + CommandSerializers.known.serializedSize(p.achieved, version)
                 + CommandSerializers.nullablePartialTxn.serializedSize(p.partialTxn, version)
                 + DepsSerializer.nullablePartialDeps.serializedSize(p.partialDeps, version)
                 + TypeSizes.sizeof(p.toEpoch)
                 + CommandSerializers.nullableTimestamp.serializedSize(p.executeAt, version)
                 + CommandSerializers.nullableWrites.serializedSize(p.writes, version)
            ;
        }
    };
}
