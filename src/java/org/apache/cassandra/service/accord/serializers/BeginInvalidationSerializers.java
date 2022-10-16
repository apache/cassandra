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
import accord.messages.BeginInvalidation.InvalidateNack;
import accord.messages.BeginInvalidation.InvalidateOk;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.primitives.AbstractRoute;
import accord.primitives.Ballot;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedSizeNullable;

public class BeginInvalidationSerializers
{
    public static final IVersionedSerializer<BeginInvalidation> request = new IVersionedSerializer<BeginInvalidation>()
    {
        @Override
        public void serialize(BeginInvalidation begin, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(begin.txnId, out, version);
            KeySerializers.routingKey.serialize(begin.someKey, out, version);
            CommandSerializers.ballot.serialize(begin.ballot, out, version);
        }

        @Override
        public BeginInvalidation deserialize(DataInputPlus in, int version) throws IOException
        {
            return new BeginInvalidation(CommandSerializers.txnId.deserialize(in, version),
                                       KeySerializers.routingKey.deserialize(in, version),
                                       CommandSerializers.ballot.deserialize(in, version));
        }

        @Override
        public long serializedSize(BeginInvalidation begin, int version)
        {
            return CommandSerializers.txnId.serializedSize(begin.txnId, version)
                   + KeySerializers.routingKey.serializedSize(begin.someKey, version)
                   + CommandSerializers.ballot.serializedSize(begin.ballot, version);
        }
    };

    public static final IVersionedSerializer<InvalidateReply> reply = new IVersionedSerializer<InvalidateReply>()
    {
        void serializeOk(InvalidateOk ok, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.status.serialize(ok.status, out, version);
            serializeNullable(KeySerializers.abstractRoute, ok.route, out, version);
            serializeNullable(KeySerializers.routingKey, ok.homeKey, out, version);
        }

        InvalidateOk deserializeOk(DataInputPlus in, int version) throws IOException
        {
            Status status = CommandSerializers.status.deserialize(in, version);
            AbstractRoute route = deserializeNullable(KeySerializers.abstractRoute, in, version);
            RoutingKey homeKey = deserializeNullable(KeySerializers.routingKey, in, version);
            return new InvalidateOk(status, route, homeKey);
        }

        long serializedOkSize(InvalidateOk ok, int version)
        {
            return CommandSerializers.status.serializedSize(ok.status, version)
                   + serializedSizeNullable(KeySerializers.abstractRoute, ok.route, version)
                   + serializedSizeNullable(KeySerializers.routingKey, ok.homeKey, version);
        }

        void serializeNack(InvalidateNack nack, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.ballot.serialize(nack.supersededBy, out, version);
            serializeNullable(KeySerializers.routingKey, nack.homeKey, out, version);
        }

        InvalidateNack deserializeNack(DataInputPlus in, int version) throws IOException
        {
            Ballot supersededBy = CommandSerializers.ballot.deserialize(in, version);
            RoutingKey homeKey = deserializeNullable(KeySerializers.routingKey, in, version);
            return new InvalidateNack(supersededBy, homeKey);
        }

        long serializedNackSize(InvalidateNack nack, int version)
        {
            return CommandSerializers.ballot.serializedSize(nack.supersededBy, version)
                   + serializedSizeNullable(KeySerializers.routingKey, nack.homeKey, version);
        }

        @Override
        public void serialize(InvalidateReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOk());
            if (!reply.isOk())
                serializeNack((InvalidateNack) reply, out, version);
            else
                serializeOk((InvalidateOk) reply, out, version);
        }

        @Override
        public InvalidateReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOk = in.readBoolean();
            if (!isOk)
                return deserializeNack(in, version);

            return deserializeOk(in, version);
        }

        @Override
        public long serializedSize(InvalidateReply reply, int version)
        {
            return TypeSizes.sizeof(reply.isOk())
                   + (reply.isOk() ? serializedOkSize((InvalidateOk) reply, version)
                                   : serializedNackSize((InvalidateNack) reply, version));
        }
    };
}
