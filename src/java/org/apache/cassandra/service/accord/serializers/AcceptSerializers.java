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
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class AcceptSerializers
{
    private AcceptSerializers() {}

    public static final IVersionedSerializer<Accept> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Accept accept, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.requestScope.serialize(accept.scope(), out, version);
            CommandSerializers.ballot.serialize(accept.ballot, out, version);
            CommandSerializers.txnId.serialize(accept.txnId, out, version);
            CommandSerializers.txn.serialize(accept.txn, out, version);
            CommandSerializers.timestamp.serialize(accept.executeAt, out, version);
            CommandSerializers.deps.serialize(accept.deps, out, version);
        }

        @Override
        public Accept deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Accept(TopologySerializers.requestScope.deserialize(in, version),
                              CommandSerializers.ballot.deserialize(in, version),
                              CommandSerializers.txnId.deserialize(in, version),
                              CommandSerializers.txn.deserialize(in, version),
                              CommandSerializers.timestamp.deserialize(in, version),
                              CommandSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedSize(Accept accept, int version)
        {
            long size = TopologySerializers.requestScope.serializedSize(accept.scope(), version);
            size += CommandSerializers.ballot.serializedSize(accept.ballot, version);
            size += CommandSerializers.txnId.serializedSize(accept.txnId, version);
            size += CommandSerializers.txn.serializedSize(accept.txn, version);
            size += CommandSerializers.timestamp.serializedSize(accept.executeAt, version);
            size += CommandSerializers.deps.serializedSize(accept.deps, version);
            return size;
        }
    };

    private static final IVersionedSerializer<AcceptOk> acceptOk = new IVersionedSerializer<AcceptOk>()
    {
        @Override
        public void serialize(AcceptOk acceptOk, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(acceptOk.txnId, out, version);
            CommandSerializers.deps.serialize(acceptOk.deps, out, version);

        }

        @Override
        public AcceptOk deserialize(DataInputPlus in, int version) throws IOException
        {
            return new AcceptOk(CommandSerializers.txnId.deserialize(in, version),
                                CommandSerializers.deps.deserialize(in, version));
        }

        @Override
        public long serializedSize(AcceptOk acceptOk, int version)
        {
            return CommandSerializers.txnId.serializedSize(acceptOk.txnId, version)
                 + CommandSerializers.deps.serializedSize(acceptOk.deps, version);
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

    public static final IVersionedSerializer<AcceptReply> reply = new IVersionedSerializer<>()
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
