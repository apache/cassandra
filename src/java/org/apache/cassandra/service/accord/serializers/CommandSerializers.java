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
import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.api.Write;
import accord.coordinate.Infer;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Known;
import accord.primitives.Known.KnownDeps;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.IVersionedWithKeysSerializer.AbstractWithKeysSerializer;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.CastingSerializer;
import org.apache.cassandra.utils.NullableSerializer;

public class CommandSerializers
{
    private CommandSerializers()
    {
    }

    public static final TimestampSerializer<TxnId> txnId = new TimestampSerializer<>(TxnId::fromBits);
    public static final TimestampSerializer<Timestamp> timestamp = new TimestampSerializer<>(Timestamp::fromBits);
    public static final UnversionedSerializer<Timestamp> nullableTimestamp = NullableSerializer.wrap(timestamp);
    public static final BallotSerializer ballot = new BallotSerializer(); // permits null
    public static final UnversionedSerializer<Txn.Kind> kind = EncodeAsVInt32.of(Txn.Kind.class);
    public static final StoreParticipantsSerializer participants = new StoreParticipantsSerializer();

    public static class ExecuteAtSerializer
    {
        private static final int IS_TIMESTAMP = 1;
        private static final int HAS_UNIQUE_HLC = 2;
        private static final int HAS_EPOCH = 4;

        public static Timestamp deserialize(TxnId txnId, DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            if ((flags & 1) == 0)
                return txnId.addFlags(flags >>> 1);

            long epoch = txnId.epoch();
            if((flags & HAS_EPOCH) != 0)
            {
                long delta = in.readUnsignedVInt();
                if (delta == 0)
                    return Timestamp.NONE;
                epoch += delta - 1;
            }

            long hlc = txnId.hlc() + in.readUnsignedVInt();
            Node.Id node = new Node.Id(in.readUnsignedVInt32());
            if ((flags & HAS_UNIQUE_HLC) == 0)
                return Timestamp.fromValues(epoch, hlc, flags >>> 3, node);
            return new TimestampWithUniqueHlc(epoch, hlc, hlc + in.readUnsignedVInt(), flags >>> 3, node);
        }

        public static void skip(TxnId txnId, DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            if ((flags & 1) != 0)
            {
                if ((flags & HAS_EPOCH) != 0 && in.readUnsignedVInt() == 0)
                    return;
                in.readUnsignedVInt();
                in.readUnsignedVInt32();
                if ((flags & HAS_UNIQUE_HLC) != 0)
                    in.readUnsignedVInt();
            }
        }

        public static void serialize(TxnId txnId, Timestamp executeAt, DataOutputPlus out) throws IOException
        {
            int flags = flags(txnId, executeAt);
            out.writeUnsignedVInt32(flags);
            if ((flags & 1) != 0)
            {
                if ((flags & HAS_EPOCH) != 0)
                {
                    if (executeAt.equals(Timestamp.NONE))
                    {
                        out.writeUnsignedVInt(0L);
                        return;
                    }
                    out.writeUnsignedVInt(1 + executeAt.epoch() - txnId.epoch());
                }
                out.writeUnsignedVInt(executeAt.hlc() - txnId.hlc());
                out.writeUnsignedVInt32(executeAt.node.id);
                if ((flags & HAS_UNIQUE_HLC) != 0)
                    out.writeUnsignedVInt(executeAt.uniqueHlc() - executeAt.hlc());
            }
        }

        private static int flags(TxnId txnId, Timestamp executeAt)
        {
            if (executeAt.getClass() == TxnId.class)
                return (executeAt.flags() ^ txnId.flags()) << 1;

            int flags = executeAt.flags() << 3;
            if (executeAt.epoch() != txnId.epoch())
                flags |= HAS_EPOCH;
            if (executeAt.hasDistinctHlcAndUniqueHlc())
                flags |= HAS_UNIQUE_HLC;
            return flags | 1;
        }

        public static long serializedSize(TxnId txnId, Timestamp executeAt)
        {
            int flags = flags(txnId, executeAt);
            long size = TypeSizes.sizeofUnsignedVInt(flags);
            if ((flags & 1) != 0)
            {
                if ((flags & HAS_EPOCH) != 0)
                {
                    if (executeAt.equals(Timestamp.NONE))
                        return size + TypeSizes.sizeofUnsignedVInt(0L);

                    size += TypeSizes.sizeofUnsignedVInt(executeAt.epoch() - txnId.epoch());
                }
                size += TypeSizes.sizeofUnsignedVInt(executeAt.hlc() - txnId.hlc());
                size += TypeSizes.sizeofUnsignedVInt(executeAt.node.id);
                if ((flags & HAS_UNIQUE_HLC) != 0)
                    size += TypeSizes.sizeofUnsignedVInt(executeAt.uniqueHlc() - executeAt.hlc());
            }
            return size;
        }

        public static Timestamp deserialize(DataInputPlus in) throws IOException
        {
            return deserialize(in, false);
        }

        public static Timestamp deserializeNullable(DataInputPlus in) throws IOException
        {
            return deserialize(in, true);
        }

        private static Timestamp deserialize(DataInputPlus in, boolean nullable) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            if (nullable)
            {
                if (flags == 0) return null;
                flags--;
            }
            long epoch = in.readUnsignedVInt();
            if (epoch-- == 0)
                return Timestamp.NONE;

            long hlc = in.readUnsignedVInt();
            Node.Id node = new Node.Id(in.readUnsignedVInt32());
            if ((flags & HAS_UNIQUE_HLC) == 0)
            {
                if ((flags & IS_TIMESTAMP) == 0)
                    return TxnId.fromValues(epoch, hlc, flags >>> 2, node);
                return Timestamp.fromValues(epoch, hlc, flags >>> 2, node);
            }
            return new TimestampWithUniqueHlc(epoch, hlc, hlc + in.readUnsignedVInt(), flags >>> 2, node);
        }

        public static void skip(DataInputPlus in) throws IOException
        {
            skip(in, false);
        }

        public static void skipNullable(DataInputPlus in) throws IOException
        {
            skip(in, true);
        }

        private static void skip(DataInputPlus in, boolean nullable) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            if (nullable)
            {
                if (flags == 0)
                    return;
                flags--;
            }
            if (0 == in.readUnsignedVInt())
                return;
            in.readUnsignedVInt();
            in.readUnsignedVInt32();
            if ((flags & HAS_UNIQUE_HLC) != 0)
                in.readUnsignedVInt();
        }

        public static void serialize(Timestamp executeAt, DataOutputPlus out) throws IOException
        {
            serialize(executeAt, out, false);
        }

        public static void serializeNullable(Timestamp executeAt, DataOutputPlus out) throws IOException
        {
            serialize(executeAt, out, true);
        }

        private static void serialize(Timestamp executeAt, DataOutputPlus out, boolean nullable) throws IOException
        {
            int flags = flags(executeAt, nullable);
            out.writeUnsignedVInt32(flags);
            if (executeAt == null)
            {
                Invariants.require(nullable);
            }
            else if (executeAt.equals(Timestamp.NONE))
            {
                out.writeUnsignedVInt(0L);
            }
            else
            {
                out.writeUnsignedVInt(1 + executeAt.epoch());
                out.writeUnsignedVInt(executeAt.hlc());
                out.writeUnsignedVInt32(executeAt.node.id);
                if (executeAt.hasDistinctHlcAndUniqueHlc())
                    out.writeUnsignedVInt(executeAt.uniqueHlc() - executeAt.hlc());
            }
        }

        public static long serializedSize(Timestamp executeAt)
        {
            return serializedSize(executeAt, false);
        }

        public static long serializedNullableSize(Timestamp executeAt)
        {
            return serializedSize(executeAt, true);
        }

        private static long serializedSize(Timestamp executeAt, boolean nullable)
        {
            int flags = flags(executeAt, nullable);
            long size = TypeSizes.sizeofUnsignedVInt(flags);
            if (executeAt == null)
            {
                Invariants.require(nullable);
                return size;
            }
            if (executeAt.equals(Timestamp.NONE)) size += TypeSizes.sizeofUnsignedVInt(0);
            else
            {
                size += TypeSizes.sizeofUnsignedVInt(1 + executeAt.epoch());
                size += TypeSizes.sizeofUnsignedVInt(executeAt.hlc());
                size += TypeSizes.sizeofUnsignedVInt(executeAt.node.id);
                if (executeAt.hasDistinctHlcAndUniqueHlc())
                    size += TypeSizes.sizeofUnsignedVInt(executeAt.uniqueHlc() - executeAt.hlc());
            }
            return size;
        }

        private static int flags(Timestamp executeAt, boolean nullable)
        {
            if (executeAt == null)
            {
                Invariants.require(nullable);
                return 0;
            }

            int flags = executeAt.flags() << 2;
            // for compatibility with other serialized form
            flags |= (executeAt.getClass() == TxnId.class) ? 0 : 1;
            if (executeAt.hasDistinctHlcAndUniqueHlc())
                flags |= HAS_UNIQUE_HLC;
            if (nullable)
                flags++;
            return flags;
        }
    }

    // TODO (expected): optimise using subset serializers, or perhaps simply with some deduping key serializer
    public static class StoreParticipantsSerializer implements IVersionedSerializer<StoreParticipants>
    {
        static final int HAS_ROUTE = 0x1;
        static final int HAS_TOUCHED_EQUALS_ROUTE = 0x2;
        static final int TOUCHES_EQUALS_HAS_TOUCHED = 0x4;
        static final int OWNS_EQUALS_TOUCHES = 0x8;
        static final int EXECUTES_IS_NULL = 0x10;
        static final int EXECUTES_IS_OWNS = 0x20;
        static final int WAITSON_IS_OWNS = 0x40;

        @Override
        public void serialize(StoreParticipants t, DataOutputPlus out, Version version) throws IOException
        {
            boolean hasRoute = t.route() != null;
            boolean hasTouchedEqualsRoute = t.route() == t.hasTouched();
            boolean touchesEqualsHasTouched = t.touches() == t.hasTouched();
            boolean ownsEqualsTouches = t.owns() == t.touches();
            boolean executesIsNull = t.executes() == null;
            boolean executesIsOwns = !executesIsNull && t.executes() == t.owns();
            boolean waitsOnIsOwns = !executesIsNull && t.waitsOn() == t.owns();
            out.writeByte((hasRoute ? HAS_ROUTE : 0)
                          | (hasTouchedEqualsRoute ? HAS_TOUCHED_EQUALS_ROUTE : 0)
                          | (touchesEqualsHasTouched ? TOUCHES_EQUALS_HAS_TOUCHED : 0)
                          | (ownsEqualsTouches ? OWNS_EQUALS_TOUCHES : 0)
                          | (executesIsNull ? EXECUTES_IS_NULL : 0)
                          | (executesIsOwns ? EXECUTES_IS_OWNS : 0)
                          | (waitsOnIsOwns ? WAITSON_IS_OWNS : 0)
            );
            if (hasRoute) KeySerializers.route.serialize(t.route(), out);
            if (!hasTouchedEqualsRoute) KeySerializers.participants.serialize(t.hasTouched(), out);
            if (!touchesEqualsHasTouched) KeySerializers.participants.serialize(t.touches(), out);
            if (!ownsEqualsTouches) KeySerializers.participants.serialize(t.owns(), out);
            if (!executesIsNull && !executesIsOwns) KeySerializers.participants.serialize(t.executes(), out);
            if (!executesIsNull && !waitsOnIsOwns) KeySerializers.participants.serialize(t.waitsOn(), out);
        }

        public void skip(DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            if (0 != (flags & HAS_ROUTE)) KeySerializers.route.skip(in);
            if (0 == (flags & HAS_TOUCHED_EQUALS_ROUTE)) KeySerializers.participants.skip(in);
            if (0 == (flags & TOUCHES_EQUALS_HAS_TOUCHED)) KeySerializers.participants.skip(in);
            if (0 == (flags & OWNS_EQUALS_TOUCHES)) KeySerializers.participants.skip(in);
            if (0 == (flags & (EXECUTES_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skip(in);
            if (0 == (flags & (WAITSON_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skip(in);
        }

        @Override
        public StoreParticipants deserialize(DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            Route<?> route = 0 == (flags & HAS_ROUTE) ? null : KeySerializers.route.deserialize(in);
            Participants<?> hasTouched = 0 != (flags & HAS_TOUCHED_EQUALS_ROUTE) ? route : KeySerializers.participants.deserialize(in);
            Participants<?> touches = 0 != (flags & TOUCHES_EQUALS_HAS_TOUCHED) ? hasTouched : KeySerializers.participants.deserialize(in);
            Participants<?> owns = 0 != (flags & OWNS_EQUALS_TOUCHES) ? touches : KeySerializers.participants.deserialize(in);
            Participants<?> executes = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & EXECUTES_IS_OWNS) ? owns : KeySerializers.participants.deserialize(in);
            Participants<?> waitsOn = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & WAITSON_IS_OWNS) ? owns : KeySerializers.participants.deserialize(in);
            return StoreParticipants.create(route, owns, executes, waitsOn, touches, hasTouched);
        }

        @Override
        public long serializedSize(StoreParticipants t, Version version)
        {
            boolean hasRoute = t.route() != null;
            boolean hasTouchedEqualsRoute = t.route() == t.hasTouched();
            boolean touchesEqualsHasTouched = t.touches() == t.hasTouched();
            boolean ownsEqualsTouches = t.owns() == t.touches();
            boolean executesIsNotNullAndNotOwns = t.executes() != null && t.owns() != t.executes();
            long size = 1;
            if (hasRoute) size += KeySerializers.route.serializedSize(t.route());
            if (!hasTouchedEqualsRoute) size += KeySerializers.participants.serializedSize(t.hasTouched());
            if (!touchesEqualsHasTouched) size += KeySerializers.participants.serializedSize(t.touches());
            if (!ownsEqualsTouches) size += KeySerializers.participants.serializedSize(t.owns());
            if (executesIsNotNullAndNotOwns) size += KeySerializers.participants.serializedSize(t.executes());
            return size;
        }
    }

    public static class TimestampSerializer<T extends Timestamp> implements UnversionedSerializer<T>
    {
        interface Factory<T extends Timestamp>
        {
            T create(long msb, long lsb, Node.Id node);
        }

        private final TimestampSerializer.Factory<T> factory;

        private TimestampSerializer(TimestampSerializer.Factory<T> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(T ts, DataOutputPlus out) throws IOException
        {
            out.writeLong(ts.msb);
            out.writeLong(ts.lsb);
            TopologySerializers.nodeId.serialize(ts.node, out);
        }

        public <V> int serialize(T ts, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            position += accessor.putLong(dst, position, ts.msb);
            position += accessor.putLong(dst, position, ts.lsb);
            position += TopologySerializers.nodeId.serialize(ts.node, dst, accessor, position);
            int size = position - offset;
            Preconditions.checkState(size == serializedSize());
            return size;
        }

        public void serialize(T ts, ByteBuffer out)
        {
            out.putLong(ts.msb);
            out.putLong(ts.lsb);
            TopologySerializers.nodeId.serialize(ts.node, out);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            in.skipBytesFully(serializedSize());
        }

        @Override
        public T deserialize(DataInputPlus in) throws IOException
        {
            return factory.create(in.readLong(),
                                  in.readLong(),
                                  TopologySerializers.nodeId.deserialize(in));
        }

        public <V> T deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            long msb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            long lsb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            Node.Id node = TopologySerializers.nodeId.deserialize(src, accessor, offset);
            return factory.create(msb, lsb, node);
        }

        public T deserialize(ByteBuffer buffer, int position)
        {
            long msb = buffer.getLong(position);
            position += TypeSizes.LONG_SIZE;
            long lsb = buffer.getLong(position);
            position += TypeSizes.LONG_SIZE;
            Node.Id node = TopologySerializers.nodeId.deserialize(buffer, position);
            return factory.create(msb, lsb, node);
        }

        @Override
        public long serializedSize(T ts)
        {
            return serializedSize();
        }

        public int serializedSize()
        {
            return Math.toIntExact(TypeSizes.LONG_SIZE +  // ts.msb
                                   TypeSizes.LONG_SIZE +  // ts.lsb
                                   TopologySerializers.nodeId.serializedSize(null)); // ts.node
        }
    }

    public static class BallotSerializer implements UnversionedSerializer<Ballot>
    {
        final TimestampSerializer<Ballot> wrapped = new TimestampSerializer<>(Ballot::fromBits);

        @Override
        public void serialize(Ballot t, DataOutputPlus out) throws IOException
        {
            if (t == null || t.equals(Ballot.ZERO) || t.equals(Ballot.MAX))
            {
                out.writeByte(t == null ? 1 : t.equals(Ballot.ZERO) ? 2 : 3);
            }
            else
            {
                out.writeByte(0);
                wrapped.serialize(t, out);
            }
        }

        @Override
        public Ballot deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readByte();
            switch (flags)
            {
                default: throw new IOException("Corrupted input: expected [0..3], received: " + flags);
                case 0: return wrapped.deserialize(in);
                case 1: return null;
                case 2: return Ballot.ZERO;
                case 3: return Ballot.MAX;
            }
        }

        public void skip(DataInputPlus in) throws IOException
        {
            int flags = in.readByte();
            if (flags == 0)
                wrapped.skip(in);
        }

        @Override
        public long serializedSize(Ballot t)
        {
            if (t == null || t.equals(Ballot.ZERO) || t.equals(Ballot.MAX))
                return 1;
            return 1 + wrapped.serializedSize();
        }
    }

    public static class PartialTxnSerializer extends AbstractWithKeysSerializer
    implements IVersionedSerializer<PartialTxn>
    {
        private final VersionedSerializer<Read, Version> readSerializer;
        private final UnversionedSerializer<Query> querySerializer;
        private final VersionedSerializer<Update, Version> updateSerializer;

        public PartialTxnSerializer(VersionedSerializer<Read, Version> readSerializer,
                                    UnversionedSerializer<Query> querySerializer,
                                    VersionedSerializer<Update, Version> updateSerializer)
        {
            this.readSerializer = readSerializer;
            this.querySerializer = querySerializer;
            this.updateSerializer = updateSerializer;
        }

        @Override
        public void serialize(PartialTxn txn, DataOutputPlus out, Version version) throws IOException
        {
            KeySerializers.seekables.serialize(txn.keys(), out);
            serializeWithoutKeys(txn, out, version);
        }

        @Override
        public PartialTxn deserialize(DataInputPlus in, Version version) throws IOException
        {
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in);
            return deserializeWithoutKeys(keys, in, version);
        }

        @Override
        public long serializedSize(PartialTxn txn, Version version)
        {
            long size = KeySerializers.seekables.serializedSize(txn.keys());
            size += serializedSizeWithoutKeys(txn, version);
            return size;
        }

        private void serializeWithoutKeys(PartialTxn txn, DataOutputPlus out, Version version) throws IOException
        {
            CommandSerializers.kind.serialize(txn.kind(), out);
            readSerializer.serialize(txn.read(), out, version);
            querySerializer.serialize(txn.query(), out);
            out.writeBoolean(txn.update() != null);
            if (txn.update() != null)
                updateSerializer.serialize(txn.update(), out, version);
        }

        private PartialTxn deserializeWithoutKeys(Seekables<?, ?> keys, DataInputPlus in, Version version) throws IOException
        {
            Txn.Kind kind = CommandSerializers.kind.deserialize(in);
            Read read = readSerializer.deserialize(in, version);
            Query query = querySerializer.deserialize(in);
            Update update = in.readBoolean() ? updateSerializer.deserialize(in, version) : null;
            return new PartialTxn.InMemory(kind, keys, read, query, update);
        }

        private long serializedSizeWithoutKeys(PartialTxn txn, Version version)
        {
            long size = CommandSerializers.kind.serializedSize(txn.kind());
            size += readSerializer.serializedSize(txn.read(), version);
            size += querySerializer.serializedSize(txn.query());
            size += TypeSizes.sizeof(txn.update() != null);
            if (txn.update() != null)
                size += updateSerializer.serializedSize(txn.update(), version);
            return size;
        }
    }

    public static final VersionedSerializer<Read, Version> read;
    public static final UnversionedSerializer<Query> query;
    public static final VersionedSerializer<Update, Version> update;
    public static final VersionedSerializer<Write, Version> write;

    public static final VersionedSerializer<PartialTxn, Version> partialTxn;
    public static final VersionedSerializer<PartialTxn, Version> nullablePartialTxn;

    static
    {
        // We use a separate class for initialization to make it easier for BurnTest to plug its own serializers.
        QuerySerializers querySerializers = new QuerySerializers();
        read = querySerializers.read;
        query = querySerializers.query;
        update = querySerializers.update;
        write = querySerializers.write;

        partialTxn = querySerializers.partialTxn;
        nullablePartialTxn = querySerializers.nullablePartialTxn;
    }

    @VisibleForTesting
    public static class QuerySerializers
    {
        public final VersionedSerializer<Read, Version> read;
        public final UnversionedSerializer<Query> query;
        public final VersionedSerializer<Update, Version> update;
        public final VersionedSerializer<Write, Version> write;

        public final VersionedSerializer<PartialTxn, Version> partialTxn;
        public final VersionedSerializer<PartialTxn, Version> nullablePartialTxn;

        private QuerySerializers()
        {
            this(CastingSerializer.create(TxnRead.class, TxnRead.serializer),
                 CastingSerializer.create(TxnQuery.class, TxnQuery.serializer),
                 CastingSerializer.create(AccordUpdate.class, AccordUpdate.serializer),
                 CastingSerializer.create(TxnWrite.class, TxnWrite.serializer));
        }

        public QuerySerializers(VersionedSerializer<Read, Version> read,
                                UnversionedSerializer<Query> query,
                                VersionedSerializer<Update, Version> update,
                                VersionedSerializer<Write, Version> write)
        {
            this.read = read;
            this.query = query;
            this.update = update;
            this.write = write;

            this.partialTxn = new PartialTxnSerializer(read, query, update);
            this.nullablePartialTxn = NullableSerializer.wrap(partialTxn);
        }
    }

    public static final UnversionedSerializer<SaveStatus> saveStatus = EncodeAsVInt32.of(SaveStatus.class);
    public static final UnversionedSerializer<Status> status = EncodeAsVInt32.of(Status.class);
    public static final UnversionedSerializer<Durability> durability = EncodeAsVInt32.of(Durability.class);

    public static final IVersionedSerializer<Writes> writes = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Writes writes, DataOutputPlus out, Version version) throws IOException
        {
            txnId.serialize(writes.txnId, out);
            ExecuteAtSerializer.serialize(writes.txnId, writes.executeAt, out);
            KeySerializers.seekables.serialize(writes.keys, out);
            boolean hasWrites = writes.write != null;
            out.writeBoolean(hasWrites);

            if (hasWrites)
                CommandSerializers.write.serialize(writes.write, out, version);
        }

        @Override
        public Writes deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId id = txnId.deserialize(in);
            return new Writes(id, ExecuteAtSerializer.deserialize(id, in),
                              KeySerializers.seekables.deserialize(in),
                              in.readBoolean() ? CommandSerializers.write.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(Writes writes, Version version)
        {
            long size = txnId.serializedSize(writes.txnId);
            size += ExecuteAtSerializer.serializedSize(writes.txnId, writes.executeAt);
            size += KeySerializers.seekables.serializedSize(writes.keys);
            boolean hasWrites = writes.write != null;
            size += TypeSizes.sizeof(hasWrites);
            if (hasWrites)
                size += CommandSerializers.write.serializedSize(writes.write, version);
            return size;
        }
    };

    public static final VersionedSerializer<Writes, Version> nullableWrites = NullableSerializer.wrap(writes);
    public static final UnversionedSerializer<KnownDeps> knownDeps = EncodeAsVInt32.of(KnownDeps.class);
    public static final UnversionedSerializer<Infer.InvalidIf> invalidIf = EncodeAsVInt32.of(Infer.InvalidIf.class);

    public static final UnversionedSerializer<Known> known = EncodeAsVInt32.withNulls(known -> known.encoded, Known::new);

}