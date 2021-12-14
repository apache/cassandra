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

import accord.messages.BeginRecovery;
import accord.messages.BeginRecovery.RecoverNack;
import accord.messages.BeginRecovery.RecoverOk;
import accord.messages.BeginRecovery.RecoverReply;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

public class RecoverySerializers
{
    public static final IVersionedSerializer<BeginRecovery> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(BeginRecovery recover, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.requestScope.serialize(recover.scope(), out, version);
            CommandSerializers.txnId.serialize(recover.txnId, out, version);
            CommandSerializers.txn.serialize(recover.txn, out, version);
            CommandSerializers.ballot.serialize(recover.ballot, out, version);

        }

        @Override
        public BeginRecovery deserialize(DataInputPlus in, int version) throws IOException
        {
            return new BeginRecovery(TopologySerializers.requestScope.deserialize(in, version),
                                     CommandSerializers.txnId.deserialize(in, version),
                                     CommandSerializers.txn.deserialize(in, version),
                                     CommandSerializers.ballot.deserialize(in, version));
        }

        @Override
        public long serializedSize(BeginRecovery recover, int version)
        {
            long size = TopologySerializers.requestScope.serializedSize(recover.scope(), version);
            size += CommandSerializers.txnId.serializedSize(recover.txnId, version);
            size += CommandSerializers.txn.serializedSize(recover.txn, version);
            size += CommandSerializers.ballot.serializedSize(recover.ballot, version);
            return size;
        }
    };

    public static final IVersionedSerializer<RecoverReply> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(RecoverReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOK());
            if (!reply.isOK())
            {
                RecoverNack recoverNack = (RecoverNack) reply;
                CommandSerializers.ballot.serialize(recoverNack.supersededBy, out, version);
            }
            else
            {
                RecoverOk recoverOk = (RecoverOk) reply;
                CommandSerializers.txnId.serialize(recoverOk.txnId, out, version);
                CommandSerializers.status.serialize(recoverOk.status, out, version);
                CommandSerializers.ballot.serialize(recoverOk.accepted, out, version);
                CommandSerializers.timestamp.serialize(recoverOk.executeAt, out, version);
                CommandSerializers.deps.serialize(recoverOk.deps, out, version);
                CommandSerializers.deps.serialize(recoverOk.earlierCommittedWitness, out, version);
                CommandSerializers.deps.serialize(recoverOk.earlierAcceptedNoWitness, out, version);
                out.writeBoolean(recoverOk.rejectsFastPath);
                CommandSerializers.writes.serialize(recoverOk.writes, out, version);
                AccordData.serializer.serialize((AccordData) recoverOk.result, out, version);
            }
        }

        @Override
        public RecoverReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOk = in.readBoolean();
            if (!isOk)
                return new RecoverNack(CommandSerializers.ballot.deserialize(in, version));

            return new RecoverOk(CommandSerializers.txnId.deserialize(in, version),
                                 CommandSerializers.status.deserialize(in, version),
                                 CommandSerializers.ballot.deserialize(in, version),
                                 CommandSerializers.timestamp.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 CommandSerializers.deps.deserialize(in, version),
                                 in.readBoolean(),
                                 CommandSerializers.writes.deserialize(in, version),
                                 AccordData.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(RecoverReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOK());

            if (!reply.isOK())
            {
                RecoverNack recoverNack = (RecoverNack) reply;
                size += CommandSerializers.ballot.serializedSize(recoverNack.supersededBy, version);
            }
            else
            {
                RecoverOk recoverOk = (RecoverOk) reply;
                size += CommandSerializers.txnId.serializedSize(recoverOk.txnId, version);
                size += CommandSerializers.status.serializedSize(recoverOk.status, version);
                size += CommandSerializers.ballot.serializedSize(recoverOk.accepted, version);
                size += CommandSerializers.timestamp.serializedSize(recoverOk.executeAt, version);
                size += CommandSerializers.deps.serializedSize(recoverOk.deps, version);
                size += CommandSerializers.deps.serializedSize(recoverOk.earlierCommittedWitness, version);
                size += CommandSerializers.deps.serializedSize(recoverOk.earlierAcceptedNoWitness, version);
                size += TypeSizes.sizeof(recoverOk.rejectsFastPath);
                size += CommandSerializers.writes.serializedSize(recoverOk.writes, version);
                size += AccordData.serializer.serializedSize((AccordData) recoverOk.result, version);
            }
            return size;
        }
    };
}
