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
import accord.coordinate.ExecuteFlag.ExecuteFlags;
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
import accord.primitives.Writes;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.utils.vint.VIntCoding;

import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class ReadDataSerializer implements IVersionedSerializer<ReadData>
{
    public static final ReadDataSerializer request = new ReadDataSerializer();

    private static final Commit.Kind[] KINDS = Commit.Kind.values();
    private static final ReadType[] TYPES = ReadType.values();
    private static final int HAS_TXN = 0x20;
    private static final int HAS_EXECUTE_AT = 0x40;
    private static final int HAS_EXECUTE_AT_EPOCH = 0x80;

    // ATWUA = ApplyThenWaitUntilApplied
    private static final int ATWUA_HAS_MIN_EPOCH = 0x1;
    private static final int ATWUA_HAS_WRITES = 0x2;

    // STR = StableThenRead
    private static final int STR_HAS_DEPS = 0x1;
    private static final int STR_HAS_FULL_ROUTE = 0x2;

    @Override
    public void serialize(ReadData read, DataOutputPlus out, Version version) throws IOException
    {
        ReadType type = read.kind();
        boolean hasTxn = read.partialTxn() != null;
        boolean hasExecuteAt = read.executeAt() != null;
        boolean hasExecuteAtEpoch = (read.executeAt() != null ? read.executeAt() : read.txnId).epoch() != read.executeAtEpoch;

        out.writeUnsignedVInt32(type.ordinal()
                                | (read.flags.bits() << 3)
                                | (hasTxn ? HAS_TXN : 0)
                                | (hasExecuteAt ? HAS_EXECUTE_AT : 0)
                                | (hasExecuteAtEpoch ? HAS_EXECUTE_AT_EPOCH : 0)
        );
        CommandSerializers.txnId.serialize(read.txnId, out);
        KeySerializers.participants.serialize(read.scope, out);
        if (hasTxn)
            CommandSerializers.partialTxn.serialize(read.partialTxn(), out, version);
        if (hasExecuteAt)
            ExecuteAtSerializer.serialize(read.txnId, read.executeAt(), out);
        if (hasExecuteAtEpoch)
            out.writeVInt(read.executeAtEpoch - (read.executeAt() != null ? read.executeAt() : read.txnId).epoch());

        switch (type)
        {
            default: throw new UnhandledEnum(type);
            case readTxnData: break;
            case waitUntilApplied:
            {
                out.writeVInt(read.txnId.epoch() - ((WaitUntilApplied)read).minEpoch());
                break;
            }
            case readEphemeral:
            {
                ReadEphemeralTxnData re = (ReadEphemeralTxnData) read;
                DepsSerializers.partialDeps.serialize(re.partialDeps(), out);
                KeySerializers.fullRoute.serialize(re.route(), out);
                break;
            }
            case stableThenRead:
            {
                StableThenRead str = (StableThenRead) read;
                out.writeUnsignedVInt32(str.kind.ordinal());
                out.writeVInt(read.txnId.epoch() - str.minEpoch);
                boolean hasDeps = str.partialDeps() != null;
                boolean hasFullRoute = str.route != null;
                out.writeUnsignedVInt32((hasDeps ? STR_HAS_DEPS : 0)
                                      | (hasFullRoute? STR_HAS_FULL_ROUTE : 0));
                if (hasDeps)
                    DepsSerializers.partialDeps.serialize(str.partialDeps(), out);
                if (hasFullRoute)
                    KeySerializers.fullRoute.serialize(str.route, out);
                break;
            }
            case applyThenWaitUntilApplied:
            {
                ApplyThenWaitUntilApplied msg = (ApplyThenWaitUntilApplied) read;
                boolean hasMinEpoch = msg.minEpoch() != read.txnId.epoch();
                boolean hasWrites = msg.writes() != null;
                out.writeUnsignedVInt32((hasMinEpoch ? ATWUA_HAS_MIN_EPOCH : 0)
                                      | (hasWrites ? ATWUA_HAS_WRITES : 0));
                if (hasMinEpoch)
                    out.writeVInt(read.txnId.epoch() - msg.minEpoch());
                DepsSerializers.partialDeps.serialize(msg.deps(), out);
                KeySerializers.fullRoute.serialize(msg.route, out);
                if (hasWrites)
                    CommandSerializers.writes.serialize(msg.writes(), out, version);
                break;
            }
        }
    }

    @Override
    public ReadData deserialize(DataInputPlus in, Version version) throws IOException
    {
        ReadType type;
        ExecuteFlags flags;
        boolean hasTxn, hasExecuteAt, hasExecuteAtEpoch;
        {
            int tmp = in.readUnsignedVInt32();
            type = TYPES[tmp & 0x7];
            flags = ExecuteFlags.get((tmp >>> 3) & 0x3);
            hasTxn = (tmp & HAS_TXN) != 0;
            hasExecuteAt = (tmp & HAS_EXECUTE_AT) != 0;
            hasExecuteAtEpoch = (tmp & HAS_EXECUTE_AT_EPOCH) != 0;
        }

        TxnId txnId = CommandSerializers.txnId.deserialize(in);
        Participants<?> scope = KeySerializers.participants.deserialize(in);
        PartialTxn txn = hasTxn ? CommandSerializers.partialTxn.deserialize(in, version) : null;
        Timestamp executeAt = hasExecuteAt ? ExecuteAtSerializer.deserialize(txnId, in) : null;
        long executeAtEpoch = (hasExecuteAt ? executeAt : txnId).epoch();
        if (hasExecuteAtEpoch)
            executeAtEpoch += in.readVInt();

        switch (type)
        {
            default: throw new UnhandledEnum(type);
            case readTxnData:
            {
                return ReadTxnData.SerializerSupport.create(txnId, scope, txn, executeAt, executeAtEpoch, flags);
            }
            case waitUntilApplied:
            {
                long minEpoch = txnId.epoch() - in.readVInt();
                return WaitUntilApplied.SerializerSupport.create(txnId, scope, minEpoch, executeAtEpoch);
            }
            case readEphemeral:
            {
                PartialDeps deps = DepsSerializers.partialDeps.deserialize(in);
                FullRoute<?> route = KeySerializers.fullRoute.deserialize(in);
                return ReadEphemeralTxnData.SerializerSupport.create(txnId, scope, txn, deps, route, flags);
            }
            case stableThenRead:
            {
                Commit.Kind kind = KINDS[in.readUnsignedVInt32()];
                long minEpoch = txnId.epoch() - in.readVInt();
                boolean hasDeps, hasFullRoute;
                {
                    int extraFlags = in.readUnsignedVInt32();
                    hasDeps = (extraFlags & STR_HAS_DEPS) != 0;
                    hasFullRoute = (extraFlags & STR_HAS_FULL_ROUTE) != 0;
                }
                PartialDeps deps = hasDeps ? DepsSerializers.partialDeps.deserialize(in) : null;
                FullRoute<?> route = hasFullRoute ? KeySerializers.fullRoute.deserialize(in) : null;
                return StableThenRead.SerializerSupport.create(txnId, scope, kind, minEpoch, executeAt, txn, deps, route);
            }
            case applyThenWaitUntilApplied:
            {
                boolean hasMinEpoch, hasWrites;
                {
                    int extraFlags = in.readUnsignedVInt32();
                    hasMinEpoch = (extraFlags & ATWUA_HAS_MIN_EPOCH) != 0;
                    hasWrites = (extraFlags & ATWUA_HAS_WRITES) != 0;
                }
                long minEpoch = txnId.epoch();
                if (hasMinEpoch)
                    minEpoch = txnId.epoch() - in.readVInt();
                PartialDeps deps = DepsSerializers.partialDeps.deserialize(in);
                FullRoute<?> route = KeySerializers.fullRoute.deserialize(in);
                Writes writes = hasWrites ? CommandSerializers.writes.deserialize(in, version) : null;
                return ApplyThenWaitUntilApplied.SerializerSupport.create(txnId, scope, minEpoch, executeAt, route, txn, deps, writes, ResultSerializers.APPLIED);
            }
        }
    }

    @Override
    public long serializedSize(ReadData read, Version version)
    {
        ReadType type = read.kind();
        boolean hasTxn = read.partialTxn() != null;
        boolean hasExecuteAt = read.executeAt() != null;
        boolean hasExecuteAtEpoch = (read.executeAt() != null ? read.executeAt() : read.txnId).epoch() != read.executeAtEpoch;

        long size = VIntCoding.computeUnsignedVIntSize(type.ordinal()
                                                       | (read.flags.bits() << 3)
                                                       | (hasTxn ? HAS_TXN : 0)
                                                       | (hasExecuteAt ? HAS_EXECUTE_AT : 0)
                                                       | (hasExecuteAtEpoch ? HAS_EXECUTE_AT_EPOCH : 0))
                    + CommandSerializers.txnId.serializedSize(read.txnId)
                    + KeySerializers.participants.serializedSize(read.scope);
        if (hasTxn)
            size += CommandSerializers.partialTxn.serializedSize(read.partialTxn(), version);
        if (hasExecuteAt)
            size += ExecuteAtSerializer.serializedSize(read.txnId, read.executeAt());
        if (hasExecuteAtEpoch)
            size += VIntCoding.computeVIntSize(read.executeAtEpoch - (read.executeAt() != null ? read.executeAt() : read.txnId).epoch());

        switch (type)
        {
            default: throw new UnhandledEnum(type);
            case readTxnData: break;
            case waitUntilApplied:
            {
                size += VIntCoding.computeVIntSize(read.txnId.epoch() - ((WaitUntilApplied)read).minEpoch());
                break;
            }
            case readEphemeral:
            {
                ReadEphemeralTxnData re = (ReadEphemeralTxnData) read;
                size += DepsSerializers.partialDeps.serializedSize(re.partialDeps());
                size += KeySerializers.fullRoute.serializedSize(re.route());
                break;
            }
            case stableThenRead:
            {
                StableThenRead str = (StableThenRead) read;
                size += VIntCoding.computeUnsignedVIntSize(str.kind.ordinal());
                size += VIntCoding.computeVIntSize(read.txnId.epoch() - str.minEpoch);
                boolean hasDeps = str.partialDeps() != null;
                boolean hasFullRoute = str.route != null;
                size += VIntCoding.computeUnsignedVIntSize((hasDeps ? STR_HAS_DEPS : 0)
                                                         | (hasFullRoute? STR_HAS_FULL_ROUTE : 0));
                if (hasDeps)
                    size += DepsSerializers.partialDeps.serializedSize(str.partialDeps());
                if (hasFullRoute)
                    size += KeySerializers.fullRoute.serializedSize(str.route);
                break;
            }
            case applyThenWaitUntilApplied:
            {
                ApplyThenWaitUntilApplied msg = (ApplyThenWaitUntilApplied) read;
                boolean hasMinEpoch = msg.minEpoch() != read.txnId.epoch();
                boolean hasWrites = msg.writes() != null;
                size += VIntCoding.computeUnsignedVIntSize((hasMinEpoch ? ATWUA_HAS_MIN_EPOCH : 0)
                                                           | (hasWrites ? ATWUA_HAS_WRITES : 0));
                if (hasMinEpoch)
                    size += VIntCoding.computeVIntSize(read.txnId.epoch() - msg.minEpoch());
                size += DepsSerializers.partialDeps.serializedSize(msg.deps());
                size += KeySerializers.fullRoute.serializedSize(msg.route);
                if (hasWrites)
                    size += CommandSerializers.writes.serializedSize(msg.writes(), version);
                break;
            }
        }
        return size;
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
}
