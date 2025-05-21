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
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.CommitOrReadNack;
import accord.messages.ReadData.ReadOk;
import accord.messages.ReadData.ReadOkWithFutureEpoch;
import accord.messages.ReadData.ReadReply;
import accord.messages.ReadData.ReadType;
import accord.messages.ReadEphemeralTxnData;
import accord.messages.ReadTxnData;
import accord.messages.StableThenRead;
import accord.messages.WaitUntilApplied;
import accord.primitives.FullRoute;
import accord.primitives.PartialDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.txn.TxnData;

import static accord.messages.Commit.WithDeps.HasDeps;
import static accord.messages.Commit.WithDeps.NoDeps;
import static accord.messages.Commit.WithTxn.HasTxn;
import static accord.messages.Commit.WithTxn.NoTxn;
import static org.apache.cassandra.db.TypeSizes.sizeof;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class ReadDataSerializers
{
    public static final IVersionedSerializer<ReadData> readData = new IVersionedSerializer<ReadData>()
    {
        @Override
        public void serialize(ReadData t, DataOutputPlus out, Version version) throws IOException
        {
            out.writeByte(t.kind().val);
            serializerFor(t).serialize(t, out, version);
        }

        @Override
        public ReadData deserialize(DataInputPlus in, Version version) throws IOException
        {
            return serializerFor(ReadType.valueOf(in.readByte())).deserialize(in, version);
        }

        @Override
        public long serializedSize(ReadData t, Version version)
        {
            return sizeof(t.kind().val) + serializerFor(t).serializedSize(t, version);
        }
    };

    public static final ApplyThenWaitUntilAppliedSerializer applyThenWaitUntilApplied = new ApplyThenWaitUntilAppliedSerializer();

    public static class ApplyThenWaitUntilAppliedSerializer implements ReadDataSerializer<ApplyThenWaitUntilApplied>
    {
        @Override
        public void serialize(ApplyThenWaitUntilApplied msg, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(msg.txnId, out);
            KeySerializers.participants.serialize(msg.scope, out);
            out.writeUnsignedVInt(msg.minEpoch());
            ExecuteAtSerializer.serialize(msg.txnId, msg.executeAt, out);
            KeySerializers.fullRoute.serialize(msg.route, out);
            CommandSerializers.partialTxn.serialize(msg.txn, out, version);
            DepsSerializers.partialDeps.serialize(msg.deps, out);
            CommandSerializers.nullableWrites.serialize(msg.writes, out, version);
        }

        @Override
        public ApplyThenWaitUntilApplied deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            return ApplyThenWaitUntilApplied.SerializerSupport.create(
            txnId,
            KeySerializers.participants.deserialize(in),
            in.readUnsignedVInt(),
            ExecuteAtSerializer.deserialize(txnId, in),
            KeySerializers.fullRoute.deserialize(in),
            CommandSerializers.partialTxn.deserialize(in, version),
            DepsSerializers.partialDeps.deserialize(in),
            CommandSerializers.nullableWrites.deserialize(in, version),
            ResultSerializers.APPLIED);
         }

        @Override
        public long serializedSize(ApplyThenWaitUntilApplied msg, Version version)
        {
            return CommandSerializers.txnId.serializedSize(msg.txnId)
                   + KeySerializers.participants.serializedSize(msg.scope)
                   + TypeSizes.sizeofUnsignedVInt(msg.minEpoch())
                   + ExecuteAtSerializer.serializedSize(msg.txnId, msg.executeAt)
                   + KeySerializers.fullRoute.serializedSize(msg.route)
                   + CommandSerializers.partialTxn.serializedSize(msg.txn, version)
                   + DepsSerializers.partialDeps.serializedSize(msg.deps)
                   + CommandSerializers.nullableWrites.serializedSize(msg.writes, version);
        }
    }

    private static final ReadDataSerializer<ReadTxnData> readTxnData = new ReadDataSerializer<ReadTxnData>()
    {
        @Override
        public void serialize(ReadTxnData read, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out);
            KeySerializers.participants.serialize(read.scope, out);
            out.writeUnsignedVInt(read.executeAtEpoch);
        }

        @Override
        public ReadTxnData deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            long executeAtEpoch = in.readUnsignedVInt();
            return ReadTxnData.SerializerSupport.create(txnId, scope, executeAtEpoch);
        }

        @Override
        public long serializedSize(ReadTxnData read, Version version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId)
                   + KeySerializers.participants.serializedSize(read.scope)
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch);
        }
    };

    public static final ReadDataSerializer<ReadEphemeralTxnData> readEphemeralTxnData = new ReadDataSerializer<>()
    {
        @Override
        public void serialize(ReadEphemeralTxnData read, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out);
            KeySerializers.participants.serialize(read.scope, out);
            out.writeUnsignedVInt(read.executeAtEpoch);
            CommandSerializers.partialTxn.serialize(read.partialTxn(), out, version);
            DepsSerializers.partialDeps.serialize(read.partialDeps(), out);
            KeySerializers.fullRoute.serialize(read.route(), out);
        }

        @Override
        public ReadEphemeralTxnData deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            long executeAtEpoch = in.readUnsignedVInt();
            PartialTxn partialTxn = CommandSerializers.partialTxn.deserialize(in, version);
            PartialDeps partialDeps = DepsSerializers.partialDeps.deserialize(in);
            FullRoute<?> route = KeySerializers.fullRoute.deserialize(in);
            return ReadEphemeralTxnData.SerializerSupport.create(txnId, scope, executeAtEpoch, partialTxn, partialDeps, route);
        }

        @Override
        public long serializedSize(ReadEphemeralTxnData read, Version version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId)
                   + KeySerializers.participants.serializedSize(read.scope)
                   + TypeSizes.sizeofUnsignedVInt(read.executeAtEpoch)
                   + CommandSerializers.partialTxn.serializedSize(read.partialTxn(), version)
                   + DepsSerializers.partialDeps.serializedSize(read.partialDeps())
                   + KeySerializers.fullRoute.serializedSize(read.route());
        }
    };

    public interface ReadDataSerializer<T extends ReadData> extends IVersionedSerializer<T>
    {
        void serialize(T bound, DataOutputPlus out, Version version) throws IOException;
        T deserialize(DataInputPlus in, Version version) throws IOException;
        long serializedSize(T condition, Version version);
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
            case readDataWithoutTimestamp:
                return readEphemeralTxnData;
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
        final CommitOrReadNack[] nacks = CommitOrReadNack.values();
        private final VersionedSerializer<D, Version> dataSerializer;

        public ReplySerializer(VersionedSerializer<D, Version> dataSerializer)
        {
            this.dataSerializer = dataSerializer;
        }

        @Override
        public void serialize(ReadReply reply, DataOutputPlus out, Version version) throws IOException
        {
            if (!reply.isOk())
            {
                out.writeByte(3 + ((CommitOrReadNack) reply).ordinal());
                return;
            }

            ReadOk readOk = (ReadOk) reply;
            int flags = readOk.getClass() == ReadOkWithFutureEpoch.class ? 2 : readOk.uniqueHlc != 0 ? 1 : 0;
            out.writeByte(flags);
            serializeNullable(readOk.unavailable, out, KeySerializers.ranges);
            dataSerializer.serialize((D) readOk.data, out, version);
            switch (flags)
            {
                case 2: out.writeUnsignedVInt(((ReadOkWithFutureEpoch) reply).futureEpoch); break;
                case 1: out.writeUnsignedVInt(readOk.uniqueHlc);
            }
        }

        @Override
        public ReadReply deserialize(DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            if (flags > 2)
                return nacks[flags - 3];

            Ranges unavailable = deserializeNullable(in, KeySerializers.ranges);
            D data = dataSerializer.deserialize(in, version);

            long extraLong = flags == 0 ? 0 : in.readUnsignedVInt();
            if (flags <= 1)
                return new ReadOk(unavailable, data, extraLong);
            return new ReadOkWithFutureEpoch(unavailable, data, extraLong);
        }

        @Override
        public long serializedSize(ReadReply reply, Version version)
        {
            if (!reply.isOk())
                return TypeSizes.BYTE_SIZE;

            ReadOk readOk = (ReadOk) reply;
            long size = TypeSizes.BYTE_SIZE
                        + serializedNullableSize(readOk.unavailable, KeySerializers.ranges)
                        + dataSerializer.serializedSize((D) readOk.data, version);
            if (readOk.uniqueHlc != 0)
                size += TypeSizes.sizeofUnsignedVInt(readOk.uniqueHlc);
            else if (readOk instanceof ReadOkWithFutureEpoch)
                size += TypeSizes.sizeofUnsignedVInt(((ReadOkWithFutureEpoch) readOk).futureEpoch);
            return size;
        }
    }

    public static final IVersionedSerializer<ReadReply> reply = new ReplySerializer<>(TxnData.nullableSerializer);

    // TODO (desired): duplicates ReadTxnData ser/de logic; conside deduplicating if another instance of this is added
    public static final ReadDataSerializer<WaitUntilApplied> waitUntilApplied = new ReadDataSerializer<WaitUntilApplied>()
    {
        @Override
        public void serialize(WaitUntilApplied waitUntilApplied, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(waitUntilApplied.txnId, out);
            KeySerializers.participants.serialize(waitUntilApplied.scope, out);
            out.writeUnsignedVInt(waitUntilApplied.minEpoch());
            out.writeUnsignedVInt(waitUntilApplied.executeAtEpoch - waitUntilApplied.minEpoch());
        }

        @Override
        public WaitUntilApplied deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            long minEpoch = in.readUnsignedVInt();
            long executeAtEpoch = minEpoch + in.readUnsignedVInt();
            return WaitUntilApplied.SerializerSupport.create(txnId, scope, minEpoch, executeAtEpoch);
        }

        @Override
        public long serializedSize(WaitUntilApplied waitUntilApplied, Version version)
        {
            return CommandSerializers.txnId.serializedSize(waitUntilApplied.txnId)
                   + KeySerializers.participants.serializedSize(waitUntilApplied.scope)
                   + TypeSizes.sizeofUnsignedVInt(waitUntilApplied.minEpoch())
                   + TypeSizes.sizeofUnsignedVInt(waitUntilApplied.executeAtEpoch - waitUntilApplied.minEpoch());
        }
    };

    // TODO (desired): duplicates a lot of Commit serializer
    public static final ReadDataSerializer<StableThenRead> stableThenRead = new ReadDataSerializer<>()
    {
        @Override
        public void serialize(StableThenRead read, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.txnId.serialize(read.txnId, out);
            KeySerializers.participants.serialize(read.scope, out);
            CommitSerializers.kind.serialize(read.kind, out);
            out.writeUnsignedVInt(read.minEpoch);
            ExecuteAtSerializer.serialize(read.txnId, read.executeAt, out);
            if (read.kind.withTxn != NoTxn)
                CommandSerializers.nullablePartialTxn.serialize(read.partialTxn, out, version);
            if (read.kind.withDeps == HasDeps)
                DepsSerializers.partialDeps.serialize(read.partialDeps, out);
            if (read.kind.withTxn == HasTxn)
                KeySerializers.fullRoute.serialize(read.route, out);
        }

        @Override
        public StableThenRead deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            Participants<?> scope = KeySerializers.participants.deserialize(in);
            Commit.Kind kind = CommitSerializers.kind.deserialize(in);
            long minEpoch = in.readUnsignedVInt();
            Timestamp executeAt = ExecuteAtSerializer.deserialize(txnId, in);
            PartialTxn partialTxn = kind.withTxn == NoTxn ? null : CommandSerializers.nullablePartialTxn.deserialize(in, version);
            PartialDeps partialDeps = kind.withDeps == NoDeps ? null : DepsSerializers.partialDeps.deserialize(in);
            FullRoute < ?> route = kind.withTxn == HasTxn ? KeySerializers.fullRoute.deserialize(in) : null;
            return StableThenRead.SerializerSupport.create(txnId, scope, kind, minEpoch, executeAt, partialTxn, partialDeps, route);
        }

        @Override
        public long serializedSize(StableThenRead read, Version version)
        {
            return CommandSerializers.txnId.serializedSize(read.txnId)
                   + KeySerializers.participants.serializedSize(read.scope)
                   + CommitSerializers.kind.serializedSize(read.kind)
                   + TypeSizes.sizeofUnsignedVInt(read.minEpoch)
                   + ExecuteAtSerializer.serializedSize(read.txnId, read.executeAt)
                   + (read.kind.withTxn == NoTxn ? 0 : CommandSerializers.nullablePartialTxn.serializedSize(read.partialTxn, version))
                   + (read.kind.withDeps != HasDeps ? 0 : DepsSerializers.partialDeps.serializedSize(read.partialDeps))
                   + (read.kind.withTxn != HasTxn ? 0 : KeySerializers.fullRoute.serializedSize(read.route));
        }
    };
}
