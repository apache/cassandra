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

import accord.messages.ReadData.ReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadTxnData;
import accord.primitives.Ranges;
import accord.primitives.Seekables;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnData;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class ReadDataSerializers
{
    public static final IVersionedSerializer<ReadTxnData> request = new IVersionedSerializer<ReadTxnData>()
    {
        @Override
        public void serialize(ReadTxnData read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            KeySerializers.seekables.serialize(read.readScope, out, version);
            out.writeUnsignedVInt(read.waitForEpoch());
            out.writeUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
        }

        @Override
        public ReadTxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Seekables<?, ?> readScope = KeySerializers.seekables.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long executeAtEpoch = in.readUnsignedVInt() + waitForEpoch;
            return ReadTxnData.SerializerSupport.create(txnId, readScope, executeAtEpoch, waitForEpoch);
        }

        @Override
        public long serializedSize(ReadTxnData read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + KeySerializers.seekables.serializedSize(read.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(read.waitForEpoch())
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
        }
    };

    public static final IVersionedSerializer<ReadReply> reply = new IVersionedSerializer<ReadReply>()
    {
        // TODO (now): use something other than ordinal
        final ReadNack[] nacks = ReadNack.values();

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((ReadNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            ReadOk readOk = (ReadOk) reply;
            serializeNullable(readOk.unavailable, out, version, KeySerializers.ranges);
            TxnData.nullableSerializer.serialize((TxnData) readOk.data, out, version);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            Ranges ranges = deserializeNullable(in, version, KeySerializers.ranges);
            TxnData data = TxnData.nullableSerializer.deserialize(in, version);
            return new ReadOk(ranges, data);
        }

        @Override
        public long serializedSize(ReadReply reply, int version)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            ReadOk readOk = (ReadOk) reply;
            return TypeSizes.BYTE_SIZE
                   + serializedNullableSize(readOk.unavailable, version, KeySerializers.ranges)
                   + TxnData.nullableSerializer.serializedSize((TxnData) readOk.data, version);
        }
    };
}
