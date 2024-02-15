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
import accord.messages.CheckStatus;
import accord.messages.Propagate;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Ballot;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Ranges;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
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
            out.writeUnsignedVInt(request.executeAtEpoch);
            CommandSerializers.txnId.serialize(request.txnId, out, version);
            KeySerializers.ranges.serialize((Ranges) request.readScope, out, version);
            DepsSerializer.partialDeps.serialize(request.partialDeps, out, version);
            StreamingTxn.serializer.serialize(request.read, out, version);
        }

        @Override
        public FetchRequest deserialize(DataInputPlus in, int version) throws IOException
        {
            return new FetchRequest(in.readUnsignedVInt(),
                                    CommandSerializers.txnId.deserialize(in, version),
                                    KeySerializers.ranges.deserialize(in, version),
                                    DepsSerializer.partialDeps.deserialize(in, version),
                                    StreamingTxn.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(FetchRequest request, int version)
        {
            return TypeSizes.sizeofUnsignedVInt(request.executeAtEpoch)
                   + CommandSerializers.txnId.serializedSize(request.txnId, version)
                   + KeySerializers.ranges.serializedSize((Ranges) request.readScope, version)
                   + DepsSerializer.partialDeps.serializedSize(request.partialDeps, version)
                   + StreamingTxn.serializer.serializedSize(request.read, version);
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<ReadReply>()
    {
        final CommitOrReadNack[] nacks = CommitOrReadNack.values();
        final IVersionedSerializer<Data> streamDataSerializer = new CastingSerializer<>(StreamData.class, StreamData.serializer);

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((CommitOrReadNack) reply).ordinal());
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
            CommandSerializers.saveStatus.serialize(p.maxKnowledgeSaveStatus, out, version);
            CommandSerializers.saveStatus.serialize(p.maxSaveStatus, out, version);
            CommandSerializers.ballot.serialize(p.ballot, out, version);
            CommandSerializers.durability.serialize(p.durability, out, version);
            KeySerializers.nullableRoutingKey.serialize(p.homeKey, out, version);
            KeySerializers.nullableRoutingKey.serialize(p.progressKey, out, version);
            CommandSerializers.known.serialize(p.achieved, out, version);
            CheckStatusSerializers.foundKnownMap.serialize(p.known, out, version);
            out.writeBoolean(p.isShardTruncated);
            CommandSerializers.nullablePartialTxn.serialize(p.partialTxn, out, version);
            DepsSerializer.nullablePartialDeps.serialize(p.stableDeps, out, version);
            out.writeLong(p.toEpoch);
            CommandSerializers.nullableTimestamp.serialize(p.committedExecuteAt, out, version);
            CommandSerializers.nullableWrites.serialize(p.writes, out, version);
        }

        @Override
        public Propagate deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Route<?> route = KeySerializers.route.deserialize(in, version);
            SaveStatus maxKnowledgeSaveStatus = CommandSerializers.saveStatus.deserialize(in, version);
            SaveStatus maxSaveStatus = CommandSerializers.saveStatus.deserialize(in, version);
            Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
            Durability durability = CommandSerializers.durability.deserialize(in, version);
            RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
            RoutingKey progressKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
            Known achieved = CommandSerializers.known.deserialize(in, version);
            CheckStatus.FoundKnownMap known = CheckStatusSerializers.foundKnownMap.deserialize(in, version);
            boolean isTruncated = in.readBoolean();
            PartialTxn partialTxn = CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps committedDeps = DepsSerializer.nullablePartialDeps.deserialize(in, version);
            long toEpoch = in.readLong();
            Timestamp committedExecuteAt = CommandSerializers.nullableTimestamp.deserialize(in, version);
            Writes writes = CommandSerializers.nullableWrites.deserialize(in, version);

            Result result = null;
            switch (maxSaveStatus)
            {
                case PreApplied:
                case Applying:
                case Applied:
                case TruncatedApply:
                case TruncatedApplyWithOutcome:
                case TruncatedApplyWithDeps:
                    result = CommandSerializers.APPLIED;
                    break;
            }

            return Propagate.SerializerSupport.create(txnId,
                                                      route,
                                                      maxKnowledgeSaveStatus,
                                                      maxSaveStatus,
                                                      ballot,
                                                      durability,
                                                      homeKey,
                                                      progressKey,
                                                      achieved,
                                                      known,
                                                      isTruncated,
                                                      partialTxn,
                                                      committedDeps,
                                                      toEpoch,
                                                      committedExecuteAt,
                                                      writes,
                                                      result);
        }

        @Override
        public long serializedSize(Propagate p, int version)
        {
            return CommandSerializers.txnId.serializedSize(p.txnId, version)
                 + KeySerializers.route.serializedSize(p.route, version)
                 + CommandSerializers.saveStatus.serializedSize(p.maxKnowledgeSaveStatus, version)
                 + CommandSerializers.saveStatus.serializedSize(p.maxSaveStatus, version)
                 + CommandSerializers.ballot.serializedSize(p.ballot, version)
                 + CommandSerializers.durability.serializedSize(p.durability, version)
                 + KeySerializers.nullableRoutingKey.serializedSize(p.homeKey, version)
                 + KeySerializers.nullableRoutingKey.serializedSize(p.progressKey, version)
                 + CommandSerializers.known.serializedSize(p.achieved, version)
                 + CheckStatusSerializers.foundKnownMap.serializedSize(p.known, version)
                 + TypeSizes.BOOL_SIZE
                 + CommandSerializers.nullablePartialTxn.serializedSize(p.partialTxn, version)
                 + DepsSerializer.nullablePartialDeps.serializedSize(p.stableDeps, version)
                 + TypeSizes.sizeof(p.toEpoch)
                 + CommandSerializers.nullableTimestamp.serializedSize(p.committedExecuteAt, version)
                 + CommandSerializers.nullableWrites.serializedSize(p.writes, version)
            ;
        }
    };
}
