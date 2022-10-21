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

import accord.messages.Accept;
import accord.messages.Accept.AcceptNack;
import accord.messages.Accept.AcceptOk;
import accord.messages.Accept.AcceptReply;
import accord.primitives.Keys;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.messages.Accept.SerializerSupport.create;

public class AcceptSerializers
{
    private AcceptSerializers() {}

    public static final IVersionedSerializer<Accept> request = new TxnRequestSerializer.WithUnsyncedSerializer<Accept>()
    {
        @Override
        public void serializeBody(Accept accept, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(accept.ballot, out, version);
            KeySerializers.key.serialize(accept.homeKey, out, version);
            CommandSerializers.txn.serialize(accept.txn, out, version);
            CommandSerializers.timestamp.serialize(accept.executeAt, out, version);
            CommandSerializers.deps.serialize(accept.deps, out, version);
        }

        @Override
        public Accept deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch, TxnId txnId, long minEpoch) throws IOException
        {
            return create(scope, waitForEpoch, txnId,
                          CommandSerializers.ballot.deserialize(in, version),
                          KeySerializers.key.deserialize(in, version),
                          CommandSerializers.txn.deserialize(in, version),
                          CommandSerializers.timestamp.deserialize(in, version),
                          CommandSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(Accept accept, int version)
        {
            return CommandSerializers.ballot.serializedSize(accept.ballot, version)
                   + KeySerializers.key.serializedSize(accept.homeKey, version)
                   + CommandSerializers.txn.serializedSize(accept.txn, version)
                   + CommandSerializers.timestamp.serializedSize(accept.executeAt, version)
                   + CommandSerializers.deps.serializedSize(accept.deps, version);
        }
    };

    public static final IVersionedSerializer<Accept.Invalidate> invalidate = new IVersionedSerializer<Accept.Invalidate>()
    {
        @Override
        public void serialize(Accept.Invalidate invalidate, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(invalidate.ballot, out, version);
            CommandSerializers.txnId.serialize(invalidate.txnId, out, version);
            KeySerializers.key.serialize(invalidate.someKey, out, version);
        }

        @Override
        public Accept.Invalidate deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Accept.Invalidate(CommandSerializers.ballot.deserialize(in, version),
                                         CommandSerializers.txnId.deserialize(in, version),
                                         KeySerializers.key.deserialize(in, version));
        }

        @Override
        public long serializedSize(Accept.Invalidate invalidate, int version)
        {
            return CommandSerializers.ballot.serializedSize(invalidate.ballot, version)
                   + CommandSerializers.txnId.serializedSize(invalidate.txnId, version)
                   + KeySerializers.key.serializedSize(invalidate.someKey, version);
        }
    };

    private static final IVersionedSerializer<AcceptOk> acceptOk = new IVersionedSerializer<AcceptOk>()
    {
        @Override
        public void serialize(AcceptOk acceptOk, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(acceptOk.txnId, out, version);
            NullableSerializer.serializeNullable(acceptOk.deps, out, version, CommandSerializers.deps);

        }

        @Override
        public AcceptOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new AcceptOk(CommandSerializers.txnId.deserialize(in, version),
                                NullableSerializer.deserializeNullable(in, version, CommandSerializers.deps));
        }

        @Override
        public long serializedSize(AcceptOk acceptOk, int version)
        {
            return CommandSerializers.txnId.serializedSize(acceptOk.txnId, version)
                   + NullableSerializer.serializedSizeNullable(acceptOk.deps, version, CommandSerializers.deps);
        }
    };

    private static final IVersionedSerializer<AcceptNack> acceptNack = new IVersionedSerializer<AcceptNack>()
    {
        @Override
        public void serialize(AcceptNack acceptNack, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(acceptNack.txnId, out, version);
            CommandSerializers.timestamp.serialize(acceptNack.reject, out, version);
        }

        @Override
        public AcceptNack deserialize(DataInputPlus in, int version) throws IOException
        {
            return new AcceptNack(CommandSerializers.txnId.deserialize(in, version),
                                  CommandSerializers.timestamp.deserialize(in, version));
        }

        @Override
        public long serializedSize(AcceptNack acceptNack, int version)
        {
            return CommandSerializers.txnId.serializedSize(acceptNack.txnId, version)
                 + CommandSerializers.timestamp.serializedSize(acceptNack.reject, version);
        }
    };

    public static final IVersionedSerializer<AcceptReply> reply = new IVersionedSerializer<AcceptReply>()
    {
        @Override
        public void serialize(AcceptReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOK());
            if (reply.isOK())
                acceptOk.serialize((AcceptOk) reply, out, version);
            else
                acceptNack.serialize((AcceptNack) reply, out, version);
        }

        @Override
        public AcceptReply deserialize(DataInputPlus in, int version) throws IOException
        {
            return in.readBoolean() ? acceptOk.deserialize(in, version) : acceptNack.deserialize(in, version);
        }

        @Override
        public long serializedSize(AcceptReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOK());
            if (reply.isOK())
                size += acceptOk.serializedSize((AcceptOk) reply, version);
            else
                size += acceptNack.serializedSize((AcceptNack) reply, version);
            return size;
        }
    };
}
