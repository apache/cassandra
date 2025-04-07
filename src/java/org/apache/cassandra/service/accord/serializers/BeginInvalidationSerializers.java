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
import accord.messages.BeginInvalidation;
import accord.messages.BeginInvalidation.InvalidateReply;
import accord.primitives.Ballot;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class BeginInvalidationSerializers
{
    public static final UnversionedSerializer<BeginInvalidation> request = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(BeginInvalidation begin, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(begin.txnId, out);
            KeySerializers.participants.serialize(begin.participants, out);
            CommandSerializers.ballot.serialize(begin.ballot, out);
        }

        @Override
        public BeginInvalidation deserialize(DataInputPlus in) throws IOException
        {
            return new BeginInvalidation(CommandSerializers.txnId.deserialize(in),
                                       KeySerializers.participants.deserialize(in),
                                       CommandSerializers.ballot.deserialize(in));
        }

        @Override
        public long serializedSize(BeginInvalidation begin)
        {
            return CommandSerializers.txnId.serializedSize(begin.txnId)
                   + KeySerializers.participants.serializedSize(begin.participants)
                   + CommandSerializers.ballot.serializedSize(begin.ballot);
        }
    };

    public static final UnversionedSerializer<InvalidateReply> reply = new UnversionedSerializer<>()
    {
        private static final int ACCEPTED_FAST_PATH = 0x1;
        private static final int HAS_TRUNCATED = 0x2;
        private static final int HAS_ROUTE = 0x4;
        private static final int HAS_HOME_KEY = 0x8;

        @Override
        public void serialize(InvalidateReply reply, DataOutputPlus out) throws IOException
        {
            CommandSerializers.ballot.serialize(reply.supersededBy, out);
            CommandSerializers.ballot.serialize(reply.accepted, out);
            CommandSerializers.saveStatus.serialize(reply.maxStatus, out);
            CommandSerializers.saveStatus.serialize(reply.maxKnowledgeStatus, out);
            int flags =   (reply.acceptedFastPath ? ACCEPTED_FAST_PATH : 0)
                        | (reply.truncated != null ? HAS_TRUNCATED : 0)
                        | (reply.route != null ? HAS_ROUTE : 0)
                        | (reply.homeKey != null && reply.route == null ? HAS_HOME_KEY : 0);
            out.writeByte(flags);
            if (reply.truncated != null) KeySerializers.participants.serialize(reply.truncated, out);
            if (reply.route != null) KeySerializers.route.serialize(reply.route, out);
            else if (reply.homeKey != null) KeySerializers.routingKey.serialize(reply.homeKey, out);
        }

        @Override
        public InvalidateReply deserialize(DataInputPlus in) throws IOException
        {
            Ballot supersededBy = CommandSerializers.ballot.deserialize(in);
            Ballot accepted = CommandSerializers.ballot.deserialize(in);
            SaveStatus maxStatus = CommandSerializers.saveStatus.deserialize(in);
            SaveStatus maxKnowledgeStatus = CommandSerializers.saveStatus.deserialize(in);
            byte flags = in.readByte();
            boolean acceptedFastPath = (flags & ACCEPTED_FAST_PATH) != 0;
            Participants<?> truncated = (flags & HAS_TRUNCATED) != 0 ? KeySerializers.participants.deserialize(in) : null;
            Route<?> route = (flags & HAS_ROUTE) != 0 ? KeySerializers.route.deserialize(in) : null;
            RoutingKey homeKey = (flags & HAS_HOME_KEY) != 0 ? KeySerializers.routingKey.deserialize(in) : route != null ? route.homeKey() : null;
            return new InvalidateReply(supersededBy, accepted, maxStatus, maxKnowledgeStatus, acceptedFastPath, truncated, route, homeKey);
        }

        @Override
        public long serializedSize(InvalidateReply reply)
        {
            return CommandSerializers.ballot.serializedSize(reply.supersededBy)
                 + CommandSerializers.ballot.serializedSize(reply.accepted)
                 + CommandSerializers.saveStatus.serializedSize(reply.maxStatus)
                 + CommandSerializers.saveStatus.serializedSize(reply.maxKnowledgeStatus)
                 + 1
                 + (reply.truncated != null ? KeySerializers.participants.serializedSize(reply.truncated) : 0)
                 + (reply.route != null ? KeySerializers.route.serializedSize(reply.route) : 0)
                 + (reply.homeKey != null && reply.route == null ? KeySerializers.routingKey.serializedSize(reply.homeKey) : 0);
        }
    };
}
