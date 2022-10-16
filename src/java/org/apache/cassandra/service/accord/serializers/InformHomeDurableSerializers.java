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

import com.google.common.collect.Sets;

import accord.messages.InformDurable;
import accord.messages.InformHomeDurable;
import accord.primitives.PartialRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializer;

public class InformHomeDurableSerializers
{
    public static final IVersionedSerializer<InformHomeDurable> request = new IVersionedSerializer<InformHomeDurable>()
    {
        @Override
        public void serialize(InformHomeDurable inform, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(inform.txnId, out, version);
            KeySerializers.routingKey.serialize(inform.homeKey, out, version);
            CommandSerializers.timestamp.serialize(inform.executeAt, out, version);
            CommandSerializers.durability.serialize(inform.durability, out, version);
            CollectionSerializer.serializeCollection(TopologySerializers.nodeId, inform.persistedOn, out, version);

        }

        @Override
        public InformHomeDurable deserialize(DataInputPlus in, int version) throws IOException
        {
            return new InformHomeDurable(CommandSerializers.txnId.deserialize(in, version),
                                         KeySerializers.routingKey.deserialize(in, version),
                                         CommandSerializers.timestamp.deserialize(in, version),
                                         CommandSerializers.durability.deserialize(in, version),
                                         CollectionSerializer.deserializeCollection(TopologySerializers.nodeId, Sets::newHashSetWithExpectedSize, in, version));
        }

        @Override
        public long serializedSize(InformHomeDurable inform, int version)
        {
            return CommandSerializers.txnId.serializedSize(inform.txnId, version)
                   + KeySerializers.routingKey.serializedSize(inform.homeKey, version)
                   + CommandSerializers.timestamp.serializedSize(inform.executeAt, version)
                   + CommandSerializers.durability.serializedSize(inform.durability, version)
                   + CollectionSerializer.serializedSizeCollection(TopologySerializers.nodeId, inform.persistedOn, version);
        }

    };
}
