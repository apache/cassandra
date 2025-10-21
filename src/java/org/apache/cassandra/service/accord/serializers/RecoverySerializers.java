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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import accord.api.Result;
import accord.api.RoutingKey;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Known.KnownDeps;
import accord.primitives.LatestDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.serializers.TxnRequestSerializer.WithUnsyncedSerializer;
import org.apache.cassandra.utils.vint.VIntCoding;

import static accord.messages.BeginRecovery.RecoverReply.Kind.Ok;

public class RecoverySerializers
{
    static final int HAS_ROUTE = 0x1;
    static final int HAS_EXECUTE_AT_EPOCH = 0x2;
    static final int IS_FAST_PATH_DECIDED = 0x4;
    static final int SIZE_OF_FLAGS = VIntCoding.computeUnsignedVIntSize(HAS_ROUTE | HAS_EXECUTE_AT_EPOCH | IS_FAST_PATH_DECIDED);
    public static final IVersionedSerializer<BeginRecovery> request = new WithUnsyncedSerializer<BeginRecovery>()
    {
        @Override
        public void serializeBody(BeginRecovery recover, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.partialTxn.serialize(recover.partialTxn, out, version);
            int flags =   (recover.route != null ? HAS_ROUTE : 0)
                        | (recover.executeAtOrTxnIdEpoch != recover.txnId.epoch() ? HAS_EXECUTE_AT_EPOCH : 0)
                        | (recover.isFastPathDecided ? IS_FAST_PATH_DECIDED : 0);
            CommandSerializers.ballot.serialize(recover.ballot, out);
            out.writeUnsignedVInt32(flags);
            if (recover.route != null)
                KeySerializers.fullRoute.serialize(recover.route, out);
            if (0 != (flags & HAS_EXECUTE_AT_EPOCH))
                out.writeUnsignedVInt(recover.executeAtOrTxnIdEpoch - recover.txnId.epoch());
        }

        @Override
        public BeginRecovery deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            Ballot ballot = CommandSerializers.ballot.deserialize(in);
            int flags = in.readUnsignedVInt32();
            FullRoute<?> route = null;
            if (0 != (flags & HAS_ROUTE))
                route = KeySerializers.fullRoute.deserialize(in);
            long executeAtOrTxnIdEpoch = txnId.epoch();
            if (0 != (flags & HAS_EXECUTE_AT_EPOCH))
                executeAtOrTxnIdEpoch += in.readUnsignedVInt32();
            boolean isFastPathDecided = 0 != (flags & IS_FAST_PATH_DECIDED);
            return BeginRecovery.SerializationSupport.create(txnId, scope, waitForEpoch, minEpoch, partialTxn, ballot, route, executeAtOrTxnIdEpoch, isFastPathDecided);
        }

        @Override
        public long serializedBodySize(BeginRecovery recover, Version version)
        {
            return CommandSerializers.partialTxn.serializedSize(recover.partialTxn, version)
                   + CommandSerializers.ballot.serializedSize(recover.ballot)
                   + SIZE_OF_FLAGS
                   + (recover.route == null ? 0 : KeySerializers.fullRoute.serializedSize(recover.route))
                   + (recover.executeAtOrTxnIdEpoch == recover.txnId.epoch() ? 0 : TypeSizes.sizeofUnsignedVInt(recover.executeAtOrTxnIdEpoch - recover.txnId.epoch()));
        }
    };

    public static final IVersionedSerializer<RecoverReply> reply = new IVersionedSerializer<RecoverReply>()
    {
        final RecoverReply.Kind[] kinds = RecoverReply.Kind.values();
        void serializeNack(RecoverNack recoverNack, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.ballot.serialize(recoverNack.supersededBy, out);
        }

        void serializeOk(RecoverOk recoverOk, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(recoverOk.txnId, out);
            CommandSerializers.status.serialize(recoverOk.status, out);
            CommandSerializers.ballot.serialize(recoverOk.accepted, out);
            ExecuteAtSerializer.serializeNullable(recoverOk.executeAt, out);
            latestDeps.serialize(recoverOk.deps, out);
            DepsSerializers.deps.serialize(recoverOk.earlierWait, out);
            DepsSerializers.deps.serialize(recoverOk.earlierNoWait, out);
            DepsSerializers.deps.serialize(recoverOk.laterCoordRejects, out);
            out.writeBoolean(recoverOk.selfAcceptsFastPath);
            KeySerializers.nullableParticipants.serialize(recoverOk.coordinatorAcceptsFastPath, out);
            out.writeBoolean(recoverOk.supersedingRejects);
            CommandSerializers.nullableWrites.serialize(recoverOk.writes, out, version);
        }

        @Override
        public void serialize(RecoverReply reply, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(reply.kind().ordinal());
            if (reply.kind() == Ok) serializeOk((RecoverOk) reply, out, version);
            else serializeNack((RecoverNack) reply, out, version);
        }

        RecoverNack deserializeNack(RecoverReply.Kind kind, Ballot supersededBy, DataInputPlus in, Version version)
        {
            return new RecoverNack(kind, supersededBy);
        }

        RecoverOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, @Nonnull LatestDeps deps, Deps earlierWait, Deps earlierNoWait, Deps laterCoordRejects, boolean acceptsFastPath, @Nullable Participants<?> coordinatorAcceptsFastPath, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, Version version)
        {
            return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierWait, earlierNoWait, laterCoordRejects, acceptsFastPath, coordinatorAcceptsFastPath, rejectsFastPath, writes, result);
        }

        @Override
        public RecoverReply deserialize(DataInputPlus in, Version version) throws IOException
        {
            RecoverReply.Kind kind = kinds[in.readByte()];
            if (kind != Ok)
                return deserializeNack(kind, CommandSerializers.ballot.deserialize(in), in, version);

            TxnId id = CommandSerializers.txnId.deserialize(in);
            Status status = CommandSerializers.status.deserialize(in);

            Result result = null;
            if (status == Status.PreApplied || status == Status.Applied || status == Status.Truncated)
                result = ResultSerializers.APPLIED;

            return deserializeOk(id,
                                 status,
                                 CommandSerializers.ballot.deserialize(in),
                                 ExecuteAtSerializer.deserializeNullable(in),
                                 latestDeps.deserialize(in),
                                 DepsSerializers.deps.deserialize(in),
                                 DepsSerializers.deps.deserialize(in),
                                 DepsSerializers.deps.deserialize(in),
                                 in.readBoolean(),
                                 KeySerializers.nullableParticipants.deserialize(in),
                                 in.readBoolean(),
                                 CommandSerializers.nullableWrites.deserialize(in, version),
                                 result,
                                 in,
                                 version);
        }

        long serializedNackSize(RecoverNack recoverNack, Version version)
        {
            return CommandSerializers.ballot.serializedSize(recoverNack.supersededBy);
        }

        long serializedOkSize(RecoverOk recoverOk, Version version)
        {
            long size = CommandSerializers.txnId.serializedSize(recoverOk.txnId);
            size += CommandSerializers.status.serializedSize(recoverOk.status);
            size += CommandSerializers.ballot.serializedSize(recoverOk.accepted);
            size += ExecuteAtSerializer.serializedNullableSize(recoverOk.executeAt);
            size += latestDeps.serializedSize(recoverOk.deps);
            size += DepsSerializers.deps.serializedSize(recoverOk.earlierWait);
            size += DepsSerializers.deps.serializedSize(recoverOk.earlierNoWait);
            size += DepsSerializers.deps.serializedSize(recoverOk.laterCoordRejects);
            size += TypeSizes.sizeof(recoverOk.selfAcceptsFastPath);
            size += KeySerializers.nullableParticipants.serializedSize(recoverOk.coordinatorAcceptsFastPath);
            size += TypeSizes.sizeof(recoverOk.supersedingRejects);
            size += CommandSerializers.nullableWrites.serializedSize(recoverOk.writes, version);
            return size;
        }

        @Override
        public long serializedSize(RecoverReply reply, Version version)
        {
            return TypeSizes.BYTE_SIZE
                   + (reply.kind() == Ok ? serializedOkSize((RecoverOk) reply, version) : serializedNackSize((RecoverNack) reply, version));
        }
    };

    public static final UnversionedSerializer<LatestDeps> latestDeps = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(LatestDeps t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(t.size());
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
            RoutingKey[] starts = new RoutingKey[size + 1];
            LatestDeps.LatestEntry[] values = new LatestDeps.LatestEntry[size];
            for (int i = 0 ; i < size ; ++i)
            {
                starts[i] = KeySerializers.routingKey.deserialize(in);
                KnownDeps knownDeps = CommandSerializers.knownDeps.deserialize(in);
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
}
