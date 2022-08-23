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

import accord.api.Result;
import accord.local.Status;
import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;
import org.apache.cassandra.utils.CounterId;

public class RecoverySerializers
{
    public static final IVersionedSerializer<BeginRecovery> request = new TxnRequestSerializer<BeginRecovery>()
    {
        @Override
        public void serializeBody(BeginRecovery recover, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(recover.txnId, out, version);
            CommandSerializers.txn.serialize(recover.txn, out, version);
            KeySerializers.key.serialize(recover.homeKey, out, version);
            CommandSerializers.ballot.serialize(recover.ballot, out, version);
        }

        @Override
        public BeginRecovery deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return new BeginRecovery(scope, waitForEpoch,
                                     CommandSerializers.txnId.deserialize(in, version),
                                     CommandSerializers.txn.deserialize(in, version),
                                     KeySerializers.key.deserialize(in, version),
                                     CommandSerializers.ballot.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(BeginRecovery recover, int version)
        {
            return CommandSerializers.txnId.serializedSize(recover.txnId, version)
                   + CommandSerializers.txn.serializedSize(recover.txn, version)
                   + KeySerializers.key.serializedSize(recover.homeKey, version)
                   + CommandSerializers.ballot.serializedSize(recover.ballot, version);
        }
    };

    static abstract class RecoverReplySerializer<O extends RecoverOk, N extends RecoverNack> implements IVersionedSerializer<RecoverReply>
    {

        void serializeNack(N recoverNack, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(recoverNack.supersededBy, out, version);
        }


        void serializeOk(O recoverOk, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(recoverOk.txnId, out, version);
            CommandSerializers.status.serialize(recoverOk.status, out, version);
            CommandSerializers.ballot.serialize(recoverOk.accepted, out, version);
            CommandSerializers.timestamp.serialize(recoverOk.executeAt, out, version);
            CommandSerializers.deps.serialize(recoverOk.deps, out, version);
            CommandSerializers.deps.serialize(recoverOk.earlierCommittedWitness, out, version);
            CommandSerializers.deps.serialize(recoverOk.earlierAcceptedNoWitness, out, version);
            out.writeBoolean(recoverOk.rejectsFastPath);
            out.writeBoolean(recoverOk.writes != null);
            if (recoverOk.writes != null)
                CommandSerializers.writes.serialize(recoverOk.writes, out, version);
            out.writeBoolean(recoverOk.result != null);
            if (recoverOk.result != null)
                AccordData.serializer.serialize((AccordData) recoverOk.result, out, version);
        }

        @Override
        public final void serialize(RecoverReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOK());
            if (!reply.isOK())
                serializeNack((N) reply, out, version);
            else
                serializeOk((O) reply, out, version);
        }

        abstract N deserializeNack(Ballot supersededBy,
                                   DataInputPlus in,
                                   int version) throws IOException;

        abstract O deserializeOk(TxnId txnId,
                                 Status status,
                                 Ballot accepted,
                                 Timestamp executeAt,
                                 Deps deps,
                                 Deps earlierCommittedWitness,
                                 Deps earlierAcceptedNoWitness,
                                 boolean rejectsFastPath,
                                 Writes writes,
                                 Result result,
                                 DataInputPlus in,
                                 int version) throws IOException;

        @Override
        public final RecoverReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOk = in.readBoolean();
            if (!isOk)
                return deserializeNack(CommandSerializers.ballot.deserialize(in, version), in, version);

            return deserializeOk(CommandSerializers.txnId.deserialize(in, version),
                                 CommandSerializers.status.deserialize(in, version),
                                 CommandSerializers.ballot.deserialize(in, version),
                                 CommandSerializers.timestamp.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 in.readBoolean(),
                                 in.readBoolean() ? CommandSerializers.writes.deserialize(in, version) : null,
                                 in.readBoolean() ? AccordData.serializer.deserialize(in, version) : null,
                                 in,
                                 version);
        }

        long serializedNackSize(N recoverNack, int version)
        {
            return CommandSerializers.ballot.serializedSize(recoverNack.supersededBy, version);
        }

        long serializedOkSize(O recoverOk, int version)
        {
            long size = CommandSerializers.txnId.serializedSize(recoverOk.txnId, version);
            size += CommandSerializers.status.serializedSize(recoverOk.status, version);
            size += CommandSerializers.ballot.serializedSize(recoverOk.accepted, version);
            size += CommandSerializers.timestamp.serializedSize(recoverOk.executeAt, version);
            size += CommandSerializers.deps.serializedSize(recoverOk.deps, version);
            size += CommandSerializers.deps.serializedSize(recoverOk.earlierCommittedWitness, version);
            size += CommandSerializers.deps.serializedSize(recoverOk.earlierAcceptedNoWitness, version);
            size += TypeSizes.sizeof(recoverOk.rejectsFastPath);
            size += TypeSizes.sizeof(recoverOk.writes != null);
            if (recoverOk.writes != null)
                size += CommandSerializers.writes.serializedSize(recoverOk.writes, version);
            size += TypeSizes.sizeof(recoverOk.result != null);
            if (recoverOk.result != null)
                size += AccordData.serializer.serializedSize((AccordData) recoverOk.result, version);
            return size;
        }

        @Override
        public final long serializedSize(RecoverReply reply, int version)
        {
            return TypeSizes.sizeof(reply.isOK())
                   + (reply.isOK() ? serializedOkSize((O) reply, version) : serializedNackSize((N) reply, version));
        }
    }

    public static final IVersionedSerializer<RecoverReply> reply = new RecoverReplySerializer()
    {
        @Override
        RecoverNack deserializeNack(Ballot supersededBy, DataInputPlus in, int version)
        {
            return new RecoverNack(supersededBy);
        }

        @Override
        RecoverOk deserializeOk(TxnId txnId, Status status, Ballot accepted, Timestamp executeAt, Deps deps, Deps earlierCommittedWitness, Deps earlierAcceptedNoWitness, boolean rejectsFastPath, Writes writes, Result result, DataInputPlus in, int version)
        {
            return new RecoverOk(txnId, status, accepted, executeAt, deps, earlierCommittedWitness, earlierAcceptedNoWitness, rejectsFastPath, writes, result);
        }
    };
}
