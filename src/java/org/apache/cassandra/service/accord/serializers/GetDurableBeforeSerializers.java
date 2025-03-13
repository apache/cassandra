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

import accord.messages.GetDurableBefore;
import accord.messages.GetDurableBefore.DurableBeforeReply;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GetDurableBeforeSerializers
{
    public static final UnversionedSerializer<GetDurableBefore> request = new UnversionedSerializer<GetDurableBefore>()
    {
        @Override
        public void serialize(GetDurableBefore msg, DataOutputPlus out) throws IOException
        {
        }

        @Override
        public GetDurableBefore deserialize(DataInputPlus in) throws IOException
        {
            return new GetDurableBefore();
        }

        @Override
        public long serializedSize(GetDurableBefore msg)
        {
            return 0;
        }
    };

    public static final UnversionedSerializer<DurableBeforeReply> reply = new UnversionedSerializer<DurableBeforeReply>()
    {
        @Override
        public void serialize(DurableBeforeReply msg, DataOutputPlus out) throws IOException
        {
            CommandStoreSerializers.durableBefore.serialize(msg.durableBeforeMap, out);
        }

        @Override
        public DurableBeforeReply deserialize(DataInputPlus in) throws IOException
        {
            return new DurableBeforeReply(CommandStoreSerializers.durableBefore.deserialize(in));
        }

        @Override
        public long serializedSize(DurableBeforeReply msg)
        {
            return CommandStoreSerializers.durableBefore.serializedSize(msg.durableBeforeMap);
        }
    };
}
