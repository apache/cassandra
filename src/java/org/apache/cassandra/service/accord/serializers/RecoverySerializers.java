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
import accord.local.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.LatestDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class RecoverySerializers
{
    public static final IVersionedSerializer<BeginRecovery> request = new TxnRequestSerializer<BeginRecovery>()
    {
        @Override
        public void serializeBody(BeginRecovery recover, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.partialTxn.serialize(recover.partialTxn, out, version);
            CommandSerializers.ballot.serialize(recover.ballot, out, version);
            serializeNullable(recover.route, out, version, KeySerializers.fullRoute);
        }

        @Override
        public BeginRecovery deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute<?> scope, long waitForEpoch) throws IOException
        {
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
            @Nullable FullRoute<?> route = deserializeNullable(in, version, KeySerializers.fullRoute);
            return BeginRecovery.SerializationSupport.create(txnId, scope, waitForEpoch, partialTxn, ballot, route);
        }

        @Override
        public long serializedBodySize(BeginRecovery recover, int version)
        {
            return CommandSerializers.partialTxn.serializedSize(recover.partialTxn, version)
                   + CommandSerializers.ballot.serializedSize(recover.ballot, version)
                   + serializedNullableSize(recover.route, version, KeySerializers.fullRoute);
        }
    };

    public static final IVersionedSerializer<RecoverReply> reply = new IVersionedSerializer<RecoverReply>()
    {
        void serializeNack(RecoverNack recoverNack, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.nullableBallot.serialize(recoverNack.supersededBy, out, version);
        }

        void serializeOk(RecoverOk recoverOk, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(recoverOk.txnId, out, version);
            CommandSerializers.status.serialize(recoverOk.status, out, version);
            CommandSerializers.ballot.serialize(recoverOk.accepted, out, version);
            CommandSerializers.nullableTimestamp.serialize(recoverOk.executeAt, out, version);
            latestDeps.serialize(recoverOk.deps, out, version);
            DepsSerializer.deps.serialize(recoverOk.earlierCommittedWitness, out, version);
            DepsSerializer.deps.serialize(recoverOk.earlierAcceptedNoWitness, out, version);
            out.writeBoolean(recoverOk.rejectsFastPath);
            CommandSerializers.nullableWrites.serialize(recoverOk.writes, out, version);
        }

        @Override
        public void serialize(RecoverReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOk());
            if (!reply.isOk())
                serializeNack((RecoverNack) reply, out, version);
            else
                serializeOk((RecoverOk) reply, out, version);
        }

        RecoverNack deserializeNack(Ballot supersededBy, DataInputPlus in, int version)
        {
            return new RecoverNack(supersededBy);
        }

        RecoverOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, @Nonnull LatestDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, int version)
        {
            return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, writes, result);
        }

        @Override
        public RecoverReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOk = in.readBoolean();
            if (!isOk)
                return deserializeNack(CommandSerializers.nullableBallot.deserialize(in, version), in, version);

            TxnId id = CommandSerializers.txnId.deserialize(in, version);
            Status status = CommandSerializers.status.deserialize(in, version);

            Result result = null;
            if (status == Status.PreApplied || status == Status.Applied || status == Status.Truncated)
                result = CommandSerializers.APPLIED;

            return deserializeOk(id,
                                 status,
                                 CommandSerializers.ballot.deserialize(in, version),
                                 CommandSerializers.nullableTimestamp.deserialize(in, version),
                                 latestDeps.deserialize(in, version),
                                 DepsSerializer.deps.deserialize(in, version),
                                 DepsSerializer.deps.deserialize(in, version),
                                 in.readBoolean(),
                                 CommandSerializers.nullableWrites.deserialize(in, version),
                                 result,
                                 in,
                                 version);
        }

        long serializedNackSize(RecoverNack recoverNack, int version)
        {
            return CommandSerializers.nullableBallot.serializedSize(recoverNack.supersededBy, version);
        }

        long serializedOkSize(RecoverOk recoverOk, int version)
        {
            long size = CommandSerializers.txnId.serializedSize(recoverOk.txnId, version);
            size += CommandSerializers.status.serializedSize(recoverOk.status, version);
            size += CommandSerializers.ballot.serializedSize(recoverOk.accepted, version);
            size += CommandSerializers.nullableTimestamp.serializedSize(recoverOk.executeAt, version);
            size += latestDeps.serializedSize(recoverOk.deps, version);
            size += DepsSerializer.deps.serializedSize(recoverOk.earlierCommittedWitness, version);
            size += DepsSerializer.deps.serializedSize(recoverOk.earlierAcceptedNoWitness, version);
            size += TypeSizes.sizeof(recoverOk.rejectsFastPath);
            size += CommandSerializers.nullableWrites.serializedSize(recoverOk.writes, version);
            return size;
        }

        @Override
        public long serializedSize(RecoverReply reply, int version)
        {
            return TypeSizes.sizeof(reply.isOk())
                   + (reply.isOk() ? serializedOkSize((RecoverOk) reply, version) : serializedNackSize((RecoverNack) reply, version));
        }
    };

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
                    DepsSerializer.nullableDeps.serialize(e.coordinatedDeps, out, version);
                    DepsSerializer.nullableDeps.serialize(e.localDeps, out, version);
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
                Status.KnownDeps knownDeps = CommandSerializers.nullableKnownDeps.deserialize(in, version);
                if (knownDeps == null)
                    continue;

                Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
                Deps coordinatedDeps = DepsSerializer.nullableDeps.deserialize(in, version);
                Deps localDeps = DepsSerializer.nullableDeps.deserialize(in, version);
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
                    size += DepsSerializer.nullableDeps.serializedSize(e.coordinatedDeps, version);
                    size += DepsSerializer.nullableDeps.serializedSize(e.localDeps, version);
                }
            }
            size += KeySerializers.routingKey.serializedSize(t.startAt(t.size()), version);
            return size;
        }
    };
}
