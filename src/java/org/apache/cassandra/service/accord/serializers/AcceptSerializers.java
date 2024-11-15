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
import accord.messages.Accept.AcceptReply;
import accord.primitives.Ballot;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.messages.Accept.SerializerSupport.create;

public class AcceptSerializers
{
    private AcceptSerializers() {}

    public static final IVersionedSerializer<Accept> request = new TxnRequestSerializer.WithUnsyncedSerializer<>()
    {
        @Override
        public void serializeBody(Accept accept, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(accept.ballot, out, version);
            CommandSerializers.timestamp.serialize(accept.executeAt, out, version);
            DepsSerializers.partialDeps.serialize(accept.partialDeps, out, version);
        }

        @Override
        public Accept deserializeBody(DataInputPlus in, int version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            return create(txnId, scope, waitForEpoch, minEpoch,
                          CommandSerializers.ballot.deserialize(in, version),
                          CommandSerializers.timestamp.deserialize(in, version),
                          DepsSerializers.partialDeps.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(Accept accept, int version)
        {
            return CommandSerializers.ballot.serializedSize(accept.ballot, version)
                   + CommandSerializers.timestamp.serializedSize(accept.executeAt, version)
                   + DepsSerializers.partialDeps.serializedSize(accept.partialDeps, version);
        }
    };

    public static final IVersionedSerializer<Accept.Invalidate> invalidate = new IVersionedSerializer<Accept.Invalidate>()
    {
        @Override
        public void serialize(Accept.Invalidate invalidate, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(invalidate.ballot, out, version);
            CommandSerializers.txnId.serialize(invalidate.txnId, out, version);
            KeySerializers.routingKey.serialize(invalidate.someKey, out, version);
        }

        @Override
        public Accept.Invalidate deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Accept.Invalidate(CommandSerializers.ballot.deserialize(in, version),
                                         CommandSerializers.txnId.deserialize(in, version),
                                         KeySerializers.routingKey.deserialize(in, version));
        }

        @Override
        public long serializedSize(Accept.Invalidate invalidate, int version)
        {
            return CommandSerializers.ballot.serializedSize(invalidate.ballot, version)
                   + CommandSerializers.txnId.serializedSize(invalidate.txnId, version)
                   + KeySerializers.routingKey.serializedSize(invalidate.someKey, version);
        }
    };

    public static final IVersionedSerializer<AcceptReply> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AcceptReply reply, DataOutputPlus out, int version) throws IOException
        {
            switch (reply.outcome())
            {
                default: throw new AssertionError();
                case Success:
                    if (reply.deps != null)
                    {
                        out.writeByte(1);
                        DepsSerializers.deps.serialize(reply.deps, out, version);
                    }
                    else
                    {
                        Invariants.checkState(reply == AcceptReply.ACCEPT_INVALIDATE);
                        out.writeByte(2);
                    }
                    break;
                case Truncated:
                    out.writeByte(3);
                    break;
                case RejectedBallot:
                    out.writeByte(4);
                    CommandSerializers.ballot.serialize(reply.supersededBy, out, version);
                    break;
                case Redundant:
                    int flags = 5 | (reply.supersededBy != null ? 0x8 : 0) | (reply.committedExecuteAt != null ? 0x10 : 0);
                    out.writeByte(flags);
                    if (reply.supersededBy != null)
                        CommandSerializers.ballot.serialize(reply.supersededBy, out, version);
                    if (reply.committedExecuteAt != null)
                        CommandSerializers.timestamp.serialize(reply.committedExecuteAt, out, version);
            }
        }

        @Override
        public AcceptReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readByte();
            switch (flags & 0x7)
            {
                default: throw new IllegalStateException("Unexpected AcceptNack type: " + (flags & 0x7));
                case 1:
                    return new AcceptReply(DepsSerializers.deps.deserialize(in, version));
                case 2:
                    return AcceptReply.ACCEPT_INVALIDATE;
                case 3:
                    return AcceptReply.TRUNCATED;
                case 4:
                    return new AcceptReply(CommandSerializers.ballot.deserialize(in, version));
                case 5:
                    Ballot supersededBy = (flags & 0x8) == 0 ? null : CommandSerializers.ballot.deserialize(in, version);
                    Timestamp committedExecuteAt = (flags & 0x10) == 0 ? null : CommandSerializers.timestamp.deserialize(in, version);
                    return new AcceptReply(supersededBy, committedExecuteAt);
            }
        }

        @Override
        public long serializedSize(AcceptReply reply, int version)
        {
            long size = TypeSizes.BYTE_SIZE;
            switch (reply.outcome())
            {
                default: throw new AssertionError();
                case Success:
                    if (reply.deps != null)
                        size += DepsSerializers.deps.serializedSize(reply.deps, version);
                    break;
                case Truncated:
                    break;
                case RejectedBallot:
                    size += CommandSerializers.ballot.serializedSize(reply.supersededBy, version);
                    break;
                case Redundant:
                    if (reply.supersededBy != null) size += CommandSerializers.ballot.serializedSize(reply.supersededBy, version);
                    if (reply.committedExecuteAt != null) size += CommandSerializers.timestamp.serializedSize(reply.committedExecuteAt, version);
            }
            return size;
        }
    };
}
