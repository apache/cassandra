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

import accord.messages.PreAccept;
import accord.messages.PreAccept.PreAcceptOk;
import accord.messages.PreAccept.PreAcceptReply;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.TxnRequestSerializer.WithUnsyncedSerializer;

public class PreacceptSerializers
{
    private PreacceptSerializers() {}

    public static final IVersionedSerializer<PreAccept> request = new WithUnsyncedSerializer<PreAccept>()
    {
        @Override
        public void serializeBody(PreAccept msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txn.serialize(msg.txn, out, version);
            KeySerializers.key.serialize(msg.homeKey, out, version);
        }

        @Override
        public PreAccept deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch, TxnId txnId, long minEpoch) throws IOException
        {
            return new PreAccept(scope, waitForEpoch, txnId,
                                 CommandSerializers.txn.deserialize(in, version),
                                 KeySerializers.key.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(PreAccept msg, int version)
        {
            return CommandSerializers.txn.serializedSize(msg.txn, version)
                   + KeySerializers.key.serializedSize(msg.homeKey, version);
        }
    };

    public static final IVersionedSerializer<PreAcceptReply> reply = new IVersionedSerializer<PreAcceptReply>()
    {
        @Override
        public void serialize(PreAcceptReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOK());
            if (!reply.isOK())
                return;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            CommandSerializers.txnId.serialize(preAcceptOk.txnId, out, version);
            CommandSerializers.timestamp.serialize(preAcceptOk.witnessedAt, out, version);
            CommandSerializers.deps.serialize(preAcceptOk.deps, out, version);
        }

        @Override
        public PreAcceptReply deserialize(DataInputPlus in, int version) throws IOException
        {
            if (!in.readBoolean())
                return PreAccept.PreAcceptNack.INSTANCE;

            return new PreAcceptOk(CommandSerializers.txnId.deserialize(in, version),
                                   CommandSerializers.timestamp.deserialize(in, version),
                                   CommandSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedSize(PreAcceptReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOK());
            if (!reply.isOK())
                return size;

            PreAcceptOk preAcceptOk = (PreAcceptOk) reply;
            size += CommandSerializers.txnId.serializedSize(preAcceptOk.txnId, version);
            size += CommandSerializers.timestamp.serializedSize(preAcceptOk.witnessedAt, version);
            size += CommandSerializers.deps.serializedSize(preAcceptOk.deps, version);

            return size;
        }
    };
}
