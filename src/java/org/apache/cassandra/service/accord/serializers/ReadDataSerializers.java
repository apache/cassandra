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

import accord.api.Data;
import accord.messages.ApplyThenWaitUntilApplied;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadType;
import accord.messages.ReadTxnData;
import accord.messages.WaitUntilApplied;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnResult;

import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class ReadDataSerializers
{
    public static final IVersionedSerializer<ReadData> readData = new IVersionedSerializer<ReadData>()
    {
        @Override
        public void serialize(ReadData t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t.kind().val);
            serializerFor(t).serialize(t, out, version);
        }

        @Override
        public ReadData deserialize(DataInputPlus in, int version) throws IOException
        {
            return serializerFor(ReadType.valueOf(in.readByte())).deserialize(in, version);
        }

        @Override
        public long serializedSize(ReadData t, int version)
        {
            return sizeof(t.kind().val) + serializerFor(t).serializedSize(t, version);
        }
    };

    public static final ApplyThenWaitUntilAppliedSerializer applyThenWaitUntilApplied = new ApplyThenWaitUntilAppliedSerializer();

    public static class ApplyThenWaitUntilAppliedSerializer implements ReadDataSerializer<ApplyThenWaitUntilApplied>
    {
        @Override
        public void serialize(ApplyThenWaitUntilApplied applyThenWaitUntilApplied, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(applyThenWaitUntilApplied.txnId, out, version);
            KeySerializers.partialRoute.serialize(applyThenWaitUntilApplied.route, out, version);
            DepsSerializer.partialDeps.serialize(applyThenWaitUntilApplied.deps, out, version);
            KeySerializers.seekables.serialize(applyThenWaitUntilApplied.partialTxnKeys, out, version);
            CommandSerializers.writes.serialize(applyThenWaitUntilApplied.writes, out, version);
            TxnResult.serializer.serialize((TxnResult) applyThenWaitUntilApplied.txnResult, out, version);
            out.writeBoolean(applyThenWaitUntilApplied.notifyAgent);
        }

        @Override
        public ApplyThenWaitUntilApplied deserialize(DataInputPlus in, int version) throws IOException
        {
            return ApplyThenWaitUntilApplied.SerializerSupport.create(
            CommandSerializers.txnId.deserialize(in, version),
            KeySerializers.partialRoute.deserialize(in, version),
            DepsSerializer.partialDeps.deserialize(in, version),
            KeySerializers.seekables.deserialize(in, version),
            CommandSerializers.writes.deserialize(in, version),
            TxnResult.serializer.deserialize(in, version),
            in.readBoolean());
        }

        @Override
        public long serializedSize(ApplyThenWaitUntilApplied applyThenWaitUntilApplied, int version)
        {
            return CommandSerializers.txnId.serializedSize(applyThenWaitUntilApplied.txnId, version)
                   + KeySerializers.partialRoute.serializedSize(applyThenWaitUntilApplied.route, version)
                   + DepsSerializer.partialDeps.serializedSize(applyThenWaitUntilApplied.deps, version)
                   + KeySerializers.seekables.serializedSize(applyThenWaitUntilApplied.partialTxnKeys, version)
                   + CommandSerializers.writes.serializedSize(applyThenWaitUntilApplied.writes, version)
                   + TxnResult.serializer.serializedSize((TxnData)applyThenWaitUntilApplied.txnResult, version)
                   + sizeof(applyThenWaitUntilApplied.notifyAgent);
        }
    }

    private static final ReadDataSerializer<ReadTxnData> readTxnData = new ReadDataSerializer<ReadTxnData>()
    {
        @Override
        public void serialize(ReadTxnData read, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out, version);
            KeySerializers.participants.serialize(read.readScope, out, version);
            out.writeUnsignedVInt(read.waitForEpoch());
            out.writeUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
        }

        @Override
        public ReadTxnData deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> readScope = KeySerializers.participants.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            long executeAtEpoch = in.readUnsignedVInt() + waitForEpoch;
            return ReadTxnData.SerializerSupport.create(txnId, readScope, executeAtEpoch, waitForEpoch);
        }

        @Override
        public long serializedSize(ReadTxnData read, int version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId, version)
                   + KeySerializers.participants.serializedSize(read.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(read.waitForEpoch())
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch - read.waitForEpoch());
        }
    };

    public interface ReadDataSerializer<T extends ReadData> extends IVersionedSerializer<T>
    {
        void serialize(T bound, DataOutputPlus out, int version) throws IOException;
        T deserialize(DataInputPlus in, int version) throws IOException;
        long serializedSize(T condition, int version);
    }

    private static ReadDataSerializer serializerFor(ReadData toSerialize)
    {
        return serializerFor(toSerialize.kind());
    }

    private static ReadDataSerializer serializerFor(ReadType type)
    {
        switch (type)
        {
            case readTxnData:
                return readTxnData;
            case applyThenWaitUntilApplied:
                return applyThenWaitUntilApplied;
            case waitUntilApplied:
                return waitUntilApplied;
            default:
                throw new IllegalStateException("Unsupported ExecuteType " + type);
        }
    }

    public static final class ReplySerializer<D extends Data> implements IVersionedSerializer<ReadReply>
    {
        // TODO (now): use something other than ordinal
        final CommitOrReadNack[] nacks = CommitOrReadNack.values();
        private final IVersionedSerializer<D> dataSerializer;

        public ReplySerializer(IVersionedSerializer<D> dataSerializer)
        {
            this.dataSerializer = dataSerializer;
        }

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, int version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(1 + ((CommitOrReadNack) reply).ordinal());
                return;
            }

            out.writeByte(0);
            ReadOk readOk = (ReadOk) reply;
            serializeNullable(readOk.unavailable, out, version, KeySerializers.ranges);
            dataSerializer.serialize((D) readOk.data, out, version);
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, int version) throws IOException
        {
            int id = in.readByte();
            if (id != 0)
                return nacks[id - 1];

            Ranges ranges = deserializeNullable(in, version, KeySerializers.ranges);
            D data = dataSerializer.deserialize(in, version);
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
                   + dataSerializer.serializedSize((D) readOk.data, version);
        }
    }

    public static final IVersionedSerializer<ReadReply> reply = new ReplySerializer<>(TxnData.nullableSerializer);

    // TODO (consider): duplicates ReadTxnData ser/de logic; conside deduplicating if another instance of this is added
    public static final ReadDataSerializer<WaitUntilApplied> waitUntilApplied = new ReadDataSerializer<WaitUntilApplied>()
    {
        @Override
        public void serialize(WaitUntilApplied waitUntilApplied, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(waitUntilApplied.txnId, out, version);
            KeySerializers.participants.serialize(waitUntilApplied.readScope, out, version);
            out.writeUnsignedVInt(waitUntilApplied.waitForEpoch());
            CommandSerializers.timestamp.serialize(waitUntilApplied.executeAt, out , version);
        }

        @Override
        public WaitUntilApplied deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in, version);
            Participants<?> readScope = KeySerializers.participants.deserialize(in, version);
            long waitForEpoch = in.readUnsignedVInt();
            Timestamp executeAt = CommandSerializers.timestamp.deserialize(in, version);
            return WaitUntilApplied.SerializerSupport.create(txnId, readScope, executeAt, waitForEpoch);
        }

        @Override
        public long serializedSize(WaitUntilApplied waitUntilApplied, int version)
        {
            return CommandSerializers.txnId.serializedSize(waitUntilApplied.txnId, version)
                   + KeySerializers.participants.serializedSize(waitUntilApplied.readScope, version)
                   + TypeSizes.sizeofUnsignedVInt(waitUntilApplied.waitForEpoch())
                   + CommandSerializers.timestamp.serializedSize(waitUntilApplied.executeAt, version);
        }
    };
}
