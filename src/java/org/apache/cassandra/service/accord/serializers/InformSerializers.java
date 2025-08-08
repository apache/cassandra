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

import accord.messages.InformDurable;
import accord.primitives.Route;
import accord.primitives.Status;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class InformSerializers
{
    public static final IVersionedSerializer<InformDurable> durable = new TxnRequestSerializer<>()
    {
        @Override
        public void serializeBody(InformDurable msg, DataOutputPlus out, Version version) throws IOException
        {
            out.writeVInt(msg.minEpoch - msg.waitForEpoch);
            out.writeVInt(msg.maxEpoch - msg.waitForEpoch);
            CommandSerializers.timestamp.serialize(msg.executeAt, out);
            CommandSerializers.durability.serialize(msg.durability, out);
        }

        @Override
        public InformDurable deserializeBody(DataInputPlus in, Version version, TxnId txnId, Route<?> scope, long waitForEpoch) throws IOException
        {
            long minEpoch = waitForEpoch + in.readVInt();
            long maxEpoch = waitForEpoch + in.readVInt();
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in);
            Status.Durability durability = CommandSerializers.durability.deserialize(in);
            return InformDurable.SerializationSupport.create(txnId, scope, executeAt, minEpoch, waitForEpoch, maxEpoch, durability);
        }

        @Override
        public long serializedBodySize(InformDurable msg, Version version)
        {
            return   TypeSizes.sizeofVInt(msg.minEpoch - msg.waitForEpoch)
                   + TypeSizes.sizeofVInt(msg.maxEpoch - msg.waitForEpoch)
                   + CommandSerializers.timestamp.serializedSize(msg.executeAt)
                   + CommandSerializers.durability.serializedSize(msg.durability);
        }
    };
}
