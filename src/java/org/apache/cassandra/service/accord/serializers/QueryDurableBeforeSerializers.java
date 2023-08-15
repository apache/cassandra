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

import accord.messages.QueryDurableBefore;
import accord.messages.QueryDurableBefore.DurableBeforeReply;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class QueryDurableBeforeSerializers
{
    public static final IVersionedSerializer<QueryDurableBefore> request = new IVersionedSerializer<QueryDurableBefore>()
    {
        @Override
        public void serialize(QueryDurableBefore msg, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(msg.waitForEpoch());
        }

        @Override
        public QueryDurableBefore deserialize(DataInputPlus in, int version) throws IOException
        {
            return new QueryDurableBefore(in.readLong());
        }

        @Override
        public long serializedSize(QueryDurableBefore msg, int version)
        {
            return TypeSizes.LONG_SIZE;
        }
    };

    public static final IVersionedSerializer<DurableBeforeReply> reply = new IVersionedSerializer<DurableBeforeReply>()
    {
        @Override
        public void serialize(DurableBeforeReply msg, DataOutputPlus out, int version) throws IOException
        {
            CommandStoreSerializers.durableBefore.serialize(msg.durableBeforeMap, out, version);
        }

        @Override
        public DurableBeforeReply deserialize(DataInputPlus in, int version) throws IOException
        {
            return new DurableBeforeReply(CommandStoreSerializers.durableBefore.deserialize(in, version));
        }

        @Override
        public long serializedSize(DurableBeforeReply msg, int version)
        {
            return CommandStoreSerializers.durableBefore.serializedSize(msg.durableBeforeMap, version);
        }
    };
}
