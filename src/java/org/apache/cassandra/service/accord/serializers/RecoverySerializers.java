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

import javax.annotation.Nullable;

import accord.api.Result;
import accord.local.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.PartialDeps;
import accord.primitives.PartialRoute;
import accord.primitives.PartialTxn;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

public class RecoverySerializers
{
    public static final IVersionedSerializer<BeginRecovery> request = new TxnRequestSerializer<BeginRecovery>()
    {
        @Override
        public void serializeBody(BeginRecovery recover, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.partialTxn.serialize(recover.partialTxn, out, version);
            CommandSerializers.ballot.serialize(recover.ballot, out, version);
            serializeNullable(KeySerializers.route, recover.route, out, version);
        }

        @Override
        public BeginRecovery deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute scope, long waitForEpoch) throws IOException
        {
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            Ballot ballot = CommandSerializers.ballot.deserialize(in, version);
            @Nullable Route route = deserializeNullable(KeySerializers.route, in, version);
            return BeginRecovery.SerializationSupport.create(txnId, scope, waitForEpoch, partialTxn, ballot, route);
        }

        @Override
        public long serializedBodySize(BeginRecovery recover, int version)
        {
            return CommandSerializers.partialTxn.serializedSize(recover.partialTxn, version)
                   + CommandSerializers.ballot.serializedSize(recover.ballot, version)
                   + serializedSizeNullable(KeySerializers.route, recover.route, version);
        }
    };

    public static final IVersionedSerializer<RecoverReply> reply = new IVersionedSerializer<RecoverReply>()
    {
        void serializeNack(RecoverNack recoverNack, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(recoverNack.supersededBy, out, version);
        }

        void serializeOk(RecoverOk recoverOk, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(recoverOk.txnId, out, version);
            CommandSerializers.status.serialize(recoverOk.status, out, version);
            CommandSerializers.ballot.serialize(recoverOk.accepted, out, version);
            serializeNullable(CommandSerializers.timestamp, recoverOk.executeAt, out, version);
            DepsSerializer.partialDeps.serialize(recoverOk.deps, out, version);
            DepsSerializer.deps.serialize(recoverOk.earlierCommittedWitness, out, version);
            DepsSerializer.deps.serialize(recoverOk.earlierAcceptedNoWitness, out, version);
            out.writeBoolean(recoverOk.rejectsFastPath);
            serializeNullable(CommandSerializers.writes, recoverOk.writes, out, version);
            serializeNullable(AccordData.serializer, (AccordData) recoverOk.result, out, version);
        }

        @Override
        public final void serialize(RecoverReply reply, DataOutputPlus out, int version) throws IOException
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

        RecoverOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, PartialDeps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, int version)
        {
            return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, writes, result);
        }

        @Override
        public final RecoverReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOk = in.readBoolean();
            if (!isOk)
                return deserializeNack(CommandSerializers.ballot.deserialize(in, version), in, version);

            return deserializeOk(CommandSerializers.txnId.deserialize(in, version),
                                 CommandSerializers.status.deserialize(in, version),
                                 CommandSerializers.ballot.deserialize(in, version),
                                 deserializeNullable(CommandSerializers.timestamp, in, version),
                                 DepsSerializer.partialDeps.deserialize(in, version),
                                 DepsSerializer.deps.deserialize(in, version),
                                 DepsSerializer.deps.deserialize(in, version),
                                 in.readBoolean(),
                                 deserializeNullable(CommandSerializers.writes, in, version),
                                 deserializeNullable(AccordData.serializer, in, version),
                                 in,
                                 version);
        }

        long serializedNackSize(RecoverNack recoverNack, int version)
        {
            return CommandSerializers.ballot.serializedSize(recoverNack.supersededBy, version);
        }

        long serializedOkSize(RecoverOk recoverOk, int version)
        {
            long size = CommandSerializers.txnId.serializedSize(recoverOk.txnId, version);
            size += CommandSerializers.status.serializedSize(recoverOk.status, version);
            size += CommandSerializers.ballot.serializedSize(recoverOk.accepted, version);
            size += serializedSizeNullable(CommandSerializers.timestamp, recoverOk.executeAt, version);
            size += DepsSerializer.partialDeps.serializedSize(recoverOk.deps, version);
            size += DepsSerializer.deps.serializedSize(recoverOk.earlierCommittedWitness, version);
            size += DepsSerializer.deps.serializedSize(recoverOk.earlierAcceptedNoWitness, version);
            size += TypeSizes.sizeof(recoverOk.rejectsFastPath);
            size += serializedSizeNullable(CommandSerializers.writes, recoverOk.writes, version);
            size += serializedSizeNullable(AccordData.serializer, (AccordData) recoverOk.result, version);
            return size;
        }

        @Override
        public final long serializedSize(RecoverReply reply, int version)
        {
            return TypeSizes.sizeof(reply.isOk())
                   + (reply.isOk() ? serializedOkSize((RecoverOk) reply, version) : serializedNackSize((RecoverNack) reply, version));
        }
    };
}
