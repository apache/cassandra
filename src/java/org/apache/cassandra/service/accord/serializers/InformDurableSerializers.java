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

import accord.local.Status;
import accord.messages.InformDurable;
import accord.primitives.PartialRoute;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class InformDurableSerializers
{
    public static final IVersionedSerializer<InformDurable> request = new TxnRequestSerializer<InformDurable>()
    {
        @Override
        public void serializeBody(InformDurable msg, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.timestamp.serialize(msg.executeAt, out, version);
            CommandSerializers.durability.serialize(msg.durability, out, version);
        }

        @Override
        public InformDurable deserializeBody(DataInputPlus in, int version, TxnId txnId, PartialRoute scope, long waitForEpoch) throws IOException
        {
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            Status.Durability durability = CommandSerializers.durability.deserialize(in, version);
            return InformDurable.SerializationSupport.create(txnId, scope, waitForEpoch, executeAt, durability);
        }

        @Override
        public long serializedBodySize(InformDurable msg, int version)
        {
            return CommandSerializers.timestamp.serializedSize(msg.executeAt, version)
            + CommandSerializers.durability.serializedSize(msg.durability, version);
        }
    };
}
