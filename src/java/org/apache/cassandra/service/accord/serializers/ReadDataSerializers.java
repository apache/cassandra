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
import accord.messages.ReadData.ReadWaiting;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.db.AccordData;

public class ReadDataSerializers
{
    public static final IVersionedSerializer<ReadData> request = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ReadData read, DataOutputPlus out, int version) throws IOException
        {
            TopologySerializers.requestScope.serialize(read.scope(), out, version);
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            CommandSerializers.txn.serialize(read.txn, out, version);
            CommandSerializers.deps.serialize(read.deps, out, version);
            CommandSerializers.timestamp.serialize(read.executeAt, out, version);
        }

        @Override
        public ReadData deserialize(DataInputPlus in, int version) throws IOException
        {
            return new ReadData(TopologySerializers.requestScope.deserialize(in, version),
                                CommandSerializers.txnId.deserialize(in, version),
                                CommandSerializers.txn.deserialize(in, version),
                                CommandSerializers.deps.deserialize(in, version),
                                CommandSerializers.timestamp.deserialize(in, version));
        }

        @Override
        public long serializedSize(ReadData read, int version)
        {
            long size = TopologySerializers.requestScope.serializedSize(read.scope(), version);
            size += CommandSerializers.txnId.serializedSize(read.txnId, version);
            size += CommandSerializers.txn.serializedSize(read.txn, version);
            size += CommandSerializers.deps.serializedSize(read.deps, version);
            size += CommandSerializers.timestamp.serializedSize(read.executeAt, version);
            return size;
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(reply.isFinal());
            out.writeBoolean(reply.isOK());

            // ReadNack
            if (!reply.isOK())
                return;

            if (reply.isFinal())
            {
                ReadOk readOk = (ReadOk) reply;
                AccordData.serializer.serialize((AccordData) readOk.data, out, version);
            }
            else
            {
                ReadWaiting readWaiting = (ReadWaiting) reply;
                CommandSerializers.txnId.serialize(readWaiting.forTxn, out, version);
                CommandSerializers.txnId.serialize(readWaiting.txnId, out, version);
                CommandSerializers.txn.serialize(readWaiting.txn, out, version);
                CommandSerializers.timestamp.serialize(readWaiting.executeAt, out, version);
                CommandSerializers.status.serialize(readWaiting.status, out, version);
            }
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isFinal = in.readBoolean();
            boolean isOK = in.readBoolean();

            if (!isOK)
                return new ReadNack();

            if (isFinal)
            {
                return new ReadOk(AccordData.serializer.deserialize(in, version));
            }
            else
            {
                return new ReadWaiting(CommandSerializers.txnId.deserialize(in, version),
                                       CommandSerializers.txnId.deserialize(in, version),
                                       CommandSerializers.txn.deserialize(in, version),
                                       CommandSerializers.timestamp.deserialize(in, version),
                                       CommandSerializers.status.deserialize(in, version));
            }
        }

        @Override
        public long serializedSize(ReadReply reply, int version)
        {
            long size = TypeSizes.sizeof(reply.isFinal());
            size += TypeSizes.sizeof(reply.isOK());

            if (!reply.isOK())
                return size;

            if (reply.isFinal())
            {
                ReadOk readOk = (ReadOk) reply;
                return size + AccordData.serializer.serializedSize((AccordData) readOk.data, version);
            }
            else
            {
                ReadWaiting readWaiting = (ReadWaiting) reply;
                size += CommandSerializers.txnId.serializedSize(readWaiting.forTxn, version);
                size += CommandSerializers.txnId.serializedSize(readWaiting.txnId, version);
                size += CommandSerializers.txn.serializedSize(readWaiting.txn, version);
                size += CommandSerializers.timestamp.serializedSize(readWaiting.executeAt, version);
                size += CommandSerializers.status.serializedSize(readWaiting.status, version);
                return size;
            }
        }
    };
}
