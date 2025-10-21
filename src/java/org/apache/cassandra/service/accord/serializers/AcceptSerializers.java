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

import accord.coordinate.ExecuteFlag.ExecuteFlags;
import accord.local.Commands.AcceptOutcome;
import accord.messages.Accept;
import accord.messages.Accept.AcceptReply;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;

import static accord.messages.Accept.SerializerSupport.create;

public class AcceptSerializers
{
    private AcceptSerializers() {}

    public static final IVersionedSerializer<Accept> request = new RequestSerializer();
    public static class RequestSerializer extends TxnRequestSerializer.WithUnsyncedSerializer<Accept>
    {
        private static final Accept.Kind[] kinds = Accept.Kind.values();
        private static final int IS_PARTIAL = 1;

        @Override
        public void serializeBody(Accept accept, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte((accept.kind.ordinal() << 1) | (accept.isPartialAccept ? IS_PARTIAL : 0));
            CommandSerializers.ballot.serialize(accept.ballot, out);
            ExecuteAtSerializer.serialize(accept.txnId, accept.executeAt, out);
            DepsSerializers.partialDeps.serialize(accept.partialDeps(), out);
        }

        @Override
        public Accept deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch, long minEpoch) throws IOException
        {
            int flags = in.readByte();
            Accept.Kind kind = kinds[(flags >>> 1) & 1];
            return create(txnId, scope, waitForEpoch, minEpoch,
                          kind,
                          CommandSerializers.ballot.deserialize(in),
                          ExecuteAtSerializer.deserialize(txnId, in),
                          DepsSerializers.partialDeps.deserialize(in),
                          (flags & IS_PARTIAL) != 0);
        }

        @Override
        public long serializedBodySize(Accept accept, Version version)
        {
            return 1
                   + CommandSerializers.ballot.serializedSize(accept.ballot)
                   + ExecuteAtSerializer.serializedSize(accept.txnId, accept.executeAt)
                   + DepsSerializers.partialDeps.serializedSize(accept.partialDeps());
        }
    }

    public static final UnversionedSerializer<Accept.NotAccept> notAccept = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Accept.NotAccept invalidate, DataOutputPlus out) throws IOException
        {
            CommandSerializers.status.serialize(invalidate.status, out);
            CommandSerializers.ballot.serialize(invalidate.ballot, out);
            CommandSerializers.txnId.serialize(invalidate.txnId, out);
            KeySerializers.participants.serialize(invalidate.participants, out);
        }

        @Override
        public Accept.NotAccept deserialize(DataInputPlus in) throws IOException
        {
            return new Accept.NotAccept(CommandSerializers.status.deserialize(in),
                                        CommandSerializers.ballot.deserialize(in),
                                        CommandSerializers.txnId.deserialize(in),
                                        KeySerializers.participants.deserialize(in));
        }

        @Override
        public long serializedSize(Accept.NotAccept invalidate)
        {
            return CommandSerializers.status.serializedSize(invalidate.status)
                   + CommandSerializers.ballot.serializedSize(invalidate.ballot)
                   + CommandSerializers.txnId.serializedSize(invalidate.txnId)
                   + KeySerializers.participants.serializedSize(invalidate.participants);
        }
    };

    public static final UnversionedSerializer<AcceptReply> reply = new ReplySerializer();
    public static class ReplySerializer implements UnversionedSerializer<AcceptReply>
    {
        // we have one spare bit at 0x04 for either another flag or more AcceptOutcome variants
        private static final int SUPERSEDED_BY        = 0x08;
        private static final int COMMITTED_EXECUTE_AT = 0x10;
        private static final int SUCCESSFUL           = 0x20;
        private static final int DEPS                 = 0x40;
        private static final int FLAGS                = 0x80;
        @Override
        public void serialize(AcceptReply reply, DataOutputPlus out) throws IOException
        {
            int flags =  reply.outcome.ordinal()
                      | (reply.supersededBy != null       ? SUPERSEDED_BY        : 0)
                      | (reply.committedExecuteAt != null ? COMMITTED_EXECUTE_AT : 0)
                      | (reply.successful != null         ? SUCCESSFUL           : 0)
                      | (reply.deps != null               ? DEPS                 : 0)
                      | (!reply.flags.isEmpty()           ? FLAGS                : 0);

            out.writeByte(flags);
            if (reply.supersededBy != null)
                CommandSerializers.ballot.serialize(reply.supersededBy, out);
            if (reply.committedExecuteAt != null)
                ExecuteAtSerializer.serialize(reply.committedExecuteAt, out);
            if (reply.successful != null)
                KeySerializers.participants.serialize(reply.successful, out);
            if (reply.deps != null)
                DepsSerializers.deps.serialize(reply.deps, out);
            if (!reply.flags.isEmpty())
                out.writeUnsignedVInt32(reply.flags.bits());
        }

        private final AcceptOutcome[] outcomes = AcceptOutcome.values();
        @Override
        public AcceptReply deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readByte();
            AcceptOutcome outcome = outcomes[flags & 3];
            Ballot supersededBy = (flags & SUPERSEDED_BY) == 0 ? null : CommandSerializers.ballot.deserialize(in);
            Timestamp committedExecuteAt = (flags & COMMITTED_EXECUTE_AT) == 0 ? null : ExecuteAtSerializer.deserialize(in);
            Participants<?> successful = (flags & SUCCESSFUL) == 0 ? null : KeySerializers.participants.deserialize(in);
            Deps deps = (flags & DEPS) == 0 ? null : DepsSerializers.deps.deserialize(in);
            ExecuteFlags executeFlags = (flags & FLAGS) == 0 ? ExecuteFlags.none() : ExecuteFlags.get(in.readUnsignedVInt32());
            return new AcceptReply(outcome, supersededBy, successful, deps, committedExecuteAt, executeFlags);
        }

        @Override
        public long serializedSize(AcceptReply reply)
        {
            long size = TypeSizes.BYTE_SIZE;
            if (reply.supersededBy != null)
                size += CommandSerializers.ballot.serializedSize(reply.supersededBy);
            if (reply.committedExecuteAt != null)
                size += ExecuteAtSerializer.serializedSize(reply.committedExecuteAt);
            if (reply.successful != null)
                size += KeySerializers.participants.serializedSize(reply.successful);
            if (reply.deps != null)
                size += DepsSerializers.deps.serializedSize(reply.deps);
            if (!reply.flags.isEmpty())
                size += TypeSizes.sizeofUnsignedVInt(reply.flags.bits());
            return size;
        }
    }
}
