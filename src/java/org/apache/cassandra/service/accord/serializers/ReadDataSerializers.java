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

import accord.messages.ReadData;
import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.primitives.Keys;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

public class ReadDataSerializers
{
    public static final IVersionedSerializer<ReadData> request = new TxnRequestSerializer<ReadData>()
    {
        @Override
        public void serializeBody(ReadData read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            CommandSerializers.txn.serialize(read.txn, out, version);
            CommandSerializers.deps.serialize(read.deps, out, version);
            KeySerializers.key.serialize(read.homeKey, out, version);
            CommandSerializers.timestamp.serialize(read.executeAt, out, version);
        }

        @Override
        public ReadData deserializeBody(DataInputPlus in, int version, Keys scope, long waitForEpoch) throws IOException
        {
            return ReadData.SerializerSupport.create(scope, waitForEpoch,
                                                     CommandSerializers.txnId.deserialize(in, version),
                                                     CommandSerializers.txn.deserialize(in, version),
                                                     CommandSerializers.deps.deserialize(in, version),
                                                     KeySerializers.key.deserialize(in, version),
                                                     CommandSerializers.timestamp.deserialize(in, version));
        }

        @Override
        public long serializedBodySize(ReadData read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + CommandSerializers.txn.serializedSize(read.txn, version)
                   + CommandSerializers.deps.serializedSize(read.deps, version)
                   + KeySerializers.key.serializedSize(read.homeKey, version)
                   + CommandSerializers.timestamp.serializedSize(read.executeAt, version);
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<ReadReply>()
    {
        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isOK());

            // ReadNack
            if (!reply.isOK())
                return;

            ReadOk readOk = (ReadOk) reply;
            AccordData.serializer.serialize((AccordData) readOk.data, out, version);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isOK = in.readBoolean();

            if (!isOK)
                return new ReadNack();

            return new ReadOk(AccordData.serializer.deserialize(in, version));
        }

        @Override
        public long serializedSize(ReadReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isOK());

            if (!reply.isOK())
                return size;

            ReadOk readOk = (ReadOk) reply;
            return size + AccordData.serializer.serializedSize((AccordData) readOk.data, version);
        }
    };
}
