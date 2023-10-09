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
import accord.local.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
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
            DepsSerializer.partialDeps.serialize(recoverOk.deps, out, version);
            DepsSerializer.nullablePartialDeps.serialize(recoverOk.acceptedDeps, out, version);
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

        RecoverOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, @Nonnull PartialDeps deps, PartialDeps acceptedDeps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, int version)
        {
            return new RecoverOk(txnId, status, accepted, executeAt, deps, acceptedDeps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, writes, result);
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
                                 DepsSerializer.partialDeps.deserialize(in, version),
                                 DepsSerializer.nullablePartialDeps.deserialize(in, version),
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
            size += DepsSerializer.partialDeps.serializedSize(recoverOk.deps, version);
            size += DepsSerializer.nullablePartialDeps.serializedSize(recoverOk.acceptedDeps, version);
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
}
