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

import accord.api.RoutingKey;
import accord.local.Status;
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.primitives.Ballot;
import accord.primitives.Route;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class BeginInvalidationSerializers
{
    public static final IVersionedSerializer<BeginInvalidation> request = new IVersionedSerializer<BeginInvalidation>()
    {
        @Override
        public void serialize(BeginInvalidation begin, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(begin.txnId, out, version);
            KeySerializers.unseekables.serialize(begin.someUnseekables, out, version);
            CommandSerializers.ballot.serialize(begin.ballot, out, version);
        }

        @Override
        public BeginInvalidation deserialize(DataInputPlus in, int version) throws IOException
        {
            return new BeginInvalidation(CommandSerializers.txnId.deserialize(in, version),
                                       KeySerializers.unseekables.deserialize(in, version),
                                       CommandSerializers.ballot.deserialize(in, version));
        }

        @Override
        public long serializedSize(BeginInvalidation begin, int version)
        {
            return CommandSerializers.txnId.serializedSize(begin.txnId, version)
                   + KeySerializers.unseekables.serializedSize(begin.someUnseekables, version)
                   + CommandSerializers.ballot.serializedSize(begin.ballot, version);
        }
    };

    public static final IVersionedSerializer<InvalidateReply> reply = new IVersionedSerializer<InvalidateReply>()
    {
        @Override
        public void serialize(InvalidateReply reply, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.nullableBallot.serialize(reply.supersededBy, out, version);
            CommandSerializers.ballot.serialize(reply.accepted, out, version);
            CommandSerializers.status.serialize(reply.status, out, version);
            out.writeBoolean(reply.acceptedFastPath);
            KeySerializers.nullableRoute.serialize(reply.route, out, version);
            KeySerializers.nullableRoutingKey.serialize(reply.homeKey, out, version);
        }

        @Override
        public InvalidateReply deserialize(DataInputPlus in, int version) throws IOException
        {
            Ballot supersededBy = CommandSerializers.nullableBallot.deserialize(in, version);
            Ballot accepted = CommandSerializers.ballot.deserialize(in, version);
            Status status = CommandSerializers.status.deserialize(in, version);
            boolean acceptedFastPath = in.readBoolean();
            Route<?> route = KeySerializers.nullableRoute.deserialize(in, version);
            RoutingKey homeKey = KeySerializers.nullableRoutingKey.deserialize(in, version);
            return new InvalidateReply(supersededBy, accepted, status, acceptedFastPath, route, homeKey);
        }

        @Override
        public long serializedSize(InvalidateReply reply, int version)
        {
            return CommandSerializers.nullableBallot.serializedSize(reply.supersededBy, version)
                 + CommandSerializers.ballot.serializedSize(reply.accepted, version)
                 + CommandSerializers.status.serializedSize(reply.status, version)
                 + TypeSizes.BOOL_SIZE
                 + KeySerializers.nullableRoute.serializedSize(reply.route, version)
                 + KeySerializers.nullableRoutingKey.serializedSize(reply.homeKey, version);
        }
    };
}
