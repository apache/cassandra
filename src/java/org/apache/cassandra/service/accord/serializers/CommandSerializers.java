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
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

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
import accord.primitives.Routable;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Unseekables;
import accord.primitives.Writes;
import accord.utils.ArrayBuffers;
import accord.utils.BitUtils;
import accord.utils.Invariants;
import accord.utils.VIntCoding;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.NullableSerializer;

import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializedPackedBitsSize;

public class CommandSerializers
{
    private CommandSerializers()
    {
    }

    public static final VariableWidthTimestampSerializer<TxnId> txnId = new VariableWidthTimestampSerializer<>(TxnId::fromValues, TxnId::fromBits, TxnId[]::new);
    public static final VariableWidthTimestampSerializer<Timestamp> timestamp = new VariableWidthTimestampSerializer<>(Timestamp::fromValues, Timestamp::fromBits, Timestamp[]::new);
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

    public static class StoreParticipantsSerializer implements UnversionedSerializer<StoreParticipants>
    {
        static final int HAS_ROUTE = 0x1;
        static final int ROUTE_EQUALS_SUPERSET = 0x2;
        static final int HAS_TOUCHED_EQUALS_SUPERSET = 0x4;
        static final int TOUCHES_EQUALS_HAS_TOUCHED = 0x8;
        static final int OWNS_EQUALS_TOUCHES = 0x10;
        static final int EXECUTES_IS_NULL = 0x20;
        static final int EXECUTES_IS_OWNS = 0x40;
        static final int WAITSON_IS_OWNS = 0x80;

        @Override
        public void serialize(StoreParticipants t, DataOutputPlus out) throws IOException
        {
            Participants<?> hasTouched = t.hasTouched();
            Route<?> route = t.route();
            Participants<?> owns = t.owns();
            Participants<?> executes = t.executes();
            Participants<?> touches = t.touches();
            boolean hasRoute = route != null;
            boolean touchesEqualsHasTouched = touches == hasTouched;
            boolean ownsEqualsTouches = owns == touches;
            boolean executesIsNull = executes == null;
            boolean executesIsOwns = !executesIsNull && executes == owns;
            boolean waitsOnIsOwns = !executesIsNull && t.waitsOn() == owns;
            boolean encodeSubsets = hasTouched.domain() == Routable.Domain.Key;
            Participants<?> superset = !hasRoute ? hasTouched : encodeSubsets ? route.with((Participants)hasTouched) : route;
            boolean routeEqualsSuperset = route == superset;
            boolean hasTouchedEqualsSuperset = hasTouched == superset;
            out.writeByte((hasRoute ? HAS_ROUTE : 0)
                          | (routeEqualsSuperset ? ROUTE_EQUALS_SUPERSET : 0)
                          | (hasTouchedEqualsSuperset ? HAS_TOUCHED_EQUALS_SUPERSET : 0)
                          | (touchesEqualsHasTouched ? TOUCHES_EQUALS_HAS_TOUCHED : 0)
                          | (ownsEqualsTouches ? OWNS_EQUALS_TOUCHES : 0)
                          | (executesIsNull ? EXECUTES_IS_NULL : 0)
                          | (executesIsOwns ? EXECUTES_IS_OWNS : 0)
                          | (waitsOnIsOwns ? WAITSON_IS_OWNS : 0)
            );

            KeySerializers.participants.serialize(superset, out);
            if (encodeSubsets)
            {
                if (hasRoute && !routeEqualsSuperset) KeySerializers.route.serializeSubset(route, superset, out);
                if (!hasTouchedEqualsSuperset) KeySerializers.participants.serializeSubset(hasTouched, superset, out);
                if (!touchesEqualsHasTouched) KeySerializers.participants.serializeSubset(touches, superset, out);
                if (!ownsEqualsTouches) KeySerializers.participants.serializeSubset(owns, superset, out);
                if (!executesIsNull && !executesIsOwns) KeySerializers.participants.serializeSubset(executes, superset, out);
                if (!executesIsNull && !waitsOnIsOwns) KeySerializers.participants.serializeSubset(t.waitsOn(), superset, out);
            }
            else
            {
                if (hasRoute && !routeEqualsSuperset) KeySerializers.route.serialize(route, out);
                if (!hasTouchedEqualsSuperset) KeySerializers.participants.serialize(hasTouched, out);
                if (!touchesEqualsHasTouched) KeySerializers.participants.serialize(touches, out);
                if (!ownsEqualsTouches) KeySerializers.participants.serialize(owns, out);
                if (!executesIsNull && !executesIsOwns) KeySerializers.participants.serialize(executes, out);
                if (!executesIsNull && !waitsOnIsOwns) KeySerializers.participants.serialize(t.waitsOn(), out);
            }
        }

        public void skip(DataInputPlus in) throws IOException
        {
            int flags = in.readByte();
            Unseekables.UnseekablesKind kind = KeySerializers.participants.readKind(in);
            int supersetCount = KeySerializers.participants.countAndSkip(kind, in);
            boolean skipSubset = kind.domain() == Routable.Domain.Key;
            if (skipSubset)
            {
                if (0 != (flags & HAS_ROUTE) && 0 == (flags & ROUTE_EQUALS_SUPERSET)) KeySerializers.route.skipSubset(supersetCount, in);
                if (0 == (flags & HAS_TOUCHED_EQUALS_SUPERSET)) KeySerializers.participants.skipSubset(supersetCount, in);
                if (0 == (flags & TOUCHES_EQUALS_HAS_TOUCHED)) KeySerializers.participants.skipSubset(supersetCount, in);
                if (0 == (flags & OWNS_EQUALS_TOUCHES)) KeySerializers.participants.skipSubset(supersetCount, in);
                if (0 == (flags & (EXECUTES_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skipSubset(supersetCount, in);
                if (0 == (flags & (WAITSON_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skipSubset(supersetCount, in);
            }
            else
            {
                if (0 != (flags & HAS_ROUTE) && 0 == (flags & ROUTE_EQUALS_SUPERSET)) KeySerializers.route.skip(in);
                if (0 == (flags & HAS_TOUCHED_EQUALS_SUPERSET)) KeySerializers.participants.skip(in);
                if (0 == (flags & TOUCHES_EQUALS_HAS_TOUCHED)) KeySerializers.participants.skip(in);
                if (0 == (flags & OWNS_EQUALS_TOUCHES)) KeySerializers.participants.skip(in);
                if (0 == (flags & (EXECUTES_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skip(in);
                if (0 == (flags & (WAITSON_IS_OWNS | EXECUTES_IS_NULL))) KeySerializers.participants.skip(in);
            }
        }

        @Override
        public StoreParticipants deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readByte();
            Participants<?> superset = KeySerializers.participants.deserialize(in);
            boolean decodeSubset = superset.domain() == Routable.Domain.Key;
            if (decodeSubset)
            {
                Route<?> route = 0 == (flags & HAS_ROUTE) ? null : 0 != (flags & ROUTE_EQUALS_SUPERSET) ? (Route<?>)superset : KeySerializers.route.deserializeSubset(superset, in);
                Participants<?> hasTouched = 0 != (flags & HAS_TOUCHED_EQUALS_SUPERSET) ? superset : KeySerializers.participants.deserializeSubset(superset, in);
                Participants<?> touches = 0 != (flags & TOUCHES_EQUALS_HAS_TOUCHED) ? hasTouched : KeySerializers.participants.deserializeSubset(superset, in);
                Participants<?> owns = 0 != (flags & OWNS_EQUALS_TOUCHES) ? touches : KeySerializers.participants.deserializeSubset(superset, in);
                Participants<?> executes = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & EXECUTES_IS_OWNS) ? owns : KeySerializers.participants.deserializeSubset(superset, in);
                Participants<?> waitsOn = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & WAITSON_IS_OWNS) ? owns : KeySerializers.participants.deserializeSubset(superset, in);
                return StoreParticipants.create(route, owns, executes, waitsOn, touches, hasTouched);
            }
            else
            {
                Route<?> route = 0 == (flags & HAS_ROUTE) ? null : 0 != (flags & ROUTE_EQUALS_SUPERSET) ? (Route<?>)superset : KeySerializers.route.deserialize(in);
                Participants<?> hasTouched = 0 != (flags & HAS_TOUCHED_EQUALS_SUPERSET) ? superset : KeySerializers.participants.deserialize(in);
                Participants<?> touches = 0 != (flags & TOUCHES_EQUALS_HAS_TOUCHED) ? hasTouched : KeySerializers.participants.deserialize(in);
                Participants<?> owns = 0 != (flags & OWNS_EQUALS_TOUCHES) ? touches : KeySerializers.participants.deserialize(in);
                Participants<?> executes = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & EXECUTES_IS_OWNS) ? owns : KeySerializers.participants.deserialize(in);
                Participants<?> waitsOn = 0 != (flags & EXECUTES_IS_NULL) ? null : 0 != (flags & WAITSON_IS_OWNS) ? owns : KeySerializers.participants.deserialize(in);
                return StoreParticipants.create(route, owns, executes, waitsOn, touches, hasTouched);
            }
        }

        @Override
        public long serializedSize(StoreParticipants t)
        {
            Participants<?> hasTouched = t.hasTouched();
            Route<?> route = t.route();
            Participants<?> owns = t.owns();
            Participants<?> executes = t.executes();
            Participants<?> touches = t.touches();
            boolean hasRoute = route != null;
            boolean touchesEqualsHasTouched = touches == hasTouched;
            boolean ownsEqualsTouches = owns == touches;
            boolean executesIsNull = executes == null;
            boolean executesIsOwns = !executesIsNull && executes == owns;
            boolean waitsOnIsOwns = !executesIsNull && t.waitsOn() == owns;
            boolean encodeSubsets = hasTouched.domain() == Routable.Domain.Key;
            Participants<?> superset = !hasRoute ? hasTouched : encodeSubsets ? route.with((Participants)hasTouched) : route;
            boolean routeEqualsSuperset = route == superset;
            boolean hasTouchedEqualsSuperset = hasTouched == superset;
            long size = 1 + KeySerializers.participants.serializedSize(superset);
            if (encodeSubsets)
            {
                if (hasRoute && !routeEqualsSuperset) size += KeySerializers.route.serializedSubsetSize(route, superset);
                if (!hasTouchedEqualsSuperset) size += KeySerializers.participants.serializedSubsetSize(hasTouched, superset);
                if (!touchesEqualsHasTouched) size += KeySerializers.participants.serializedSubsetSize(touches, superset);
                if (!ownsEqualsTouches) size += KeySerializers.participants.serializedSubsetSize(owns, superset);
                if (!executesIsNull && !executesIsOwns) size += KeySerializers.participants.serializedSubsetSize(executes, superset);
                if (!executesIsNull && !waitsOnIsOwns) size += KeySerializers.participants.serializedSubsetSize(t.waitsOn(), superset);
            }
            else
            {
                if (hasRoute && !routeEqualsSuperset) size += KeySerializers.route.serializedSize(route);
                if (!hasTouchedEqualsSuperset) size += KeySerializers.participants.serializedSize(hasTouched);
                if (!touchesEqualsHasTouched) size += KeySerializers.participants.serializedSize(touches);
                if (!ownsEqualsTouches) size += KeySerializers.participants.serializedSize(owns);
                if (!executesIsNull && !executesIsOwns) size += KeySerializers.participants.serializedSize(executes);
                if (!executesIsNull && !waitsOnIsOwns) size += KeySerializers.participants.serializedSize(t.waitsOn());
            }
            return size;
        }
    }

    public static class VariableWidthTimestampSerializer<T extends Timestamp> implements UnversionedSerializer<T>
    {
        private static final int NODE_SHIFT = 0;
        private static final int NODE_MASK = 0x3;
        private static final int NODE_MIN_LENGTH = 1;
        private static final int FLAGS_SHIFT = NODE_SHIFT + Integer.bitCount(NODE_MASK);
        private static final int FLAGS_MASK = 0x1;
        private static final int FLAGS_MIN_LENGTH = 1;
        private static final int HLC_SHIFT = FLAGS_SHIFT + Integer.bitCount(FLAGS_MASK);
        private static final int HLC_MASK = 0x3;
        private static final int HLC_MIN_LENGTH = 5;
        private static final int EPOCH_SHIFT = HLC_SHIFT + Integer.bitCount(HLC_MASK);
        private static final int EPOCH_MASK = 0x3;
        private static final int EPOCH_MIN_LENGTH = 3;
        static final byte NULL_BYTE = (byte) 0x80;
        static
        {
            Invariants.require(EPOCH_MASK << EPOCH_SHIFT >= 0);
            Invariants.require(EPOCH_SHIFT + Integer.bitCount(EPOCH_MASK) < 8);
        }

        private final Timestamp.ValueFactory<T> factory;
        private final Timestamp.RawFactory<T> rawFactory;
        private final IntFunction<T[]> allocator;

        T decodeSpecial(int encodingFlags)
        {
            Invariants.require(encodingFlags == NULL_BYTE);
            return null;
        }

        byte encodeSpecial(T value)
        {
            if (value != null)
                return 0;
            return NULL_BYTE;
        }

        private VariableWidthTimestampSerializer(Timestamp.ValueFactory<T> factory, Timestamp.RawFactory<T> rawFactory, IntFunction<T[]> allocator)
        {
            this.factory = factory;
            this.rawFactory = rawFactory;
            this.allocator = allocator;
        }

        @Override
        public void serialize(T ts, DataOutputPlus out) throws IOException
        {
            {
                byte specialByte = encodeSpecial(ts);
                if (specialByte != 0)
                {
                    Invariants.require(specialByte < 0);
                    out.writeByte(specialByte);
                    return;
                }
            }
            long epoch = ts.epoch();
            long hlc = ts.hlc();
            int flags = ts.flags();
            int epochLength = length(epoch, EPOCH_MIN_LENGTH);
            int hlcLength = length(hlc, HLC_MIN_LENGTH);
            int flagsLength = length(flags, FLAGS_MIN_LENGTH);
            int nodeLength = length(ts.node.id, NODE_MIN_LENGTH);
            int encodingFlags = encodeLength(epochLength, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK)
                              | encodeLength(hlcLength,   HLC_SHIFT,   HLC_MIN_LENGTH,   HLC_MASK)
                              | encodeLength(flagsLength, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK)
                              | encodeLength(nodeLength,  NODE_SHIFT,  NODE_MIN_LENGTH,  NODE_MASK);
            Invariants.require(((byte)encodingFlags) >= 0);
            out.writeByte(encodingFlags);
            out.writeLeastSignificantBytes(epoch, epochLength);
            out.writeLeastSignificantBytes(hlc, hlcLength);
            out.writeLeastSignificantBytes(flags, flagsLength);
            out.writeLeastSignificantBytes(ts.node.id, nodeLength);
        }

        public void serializeArray(T[] ts, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(ts.length);
            if (ts.length == 0)
                return;

            long minEpoch = Long.MAX_VALUE, maxEpoch = 0;
            long minHlc = Long.MAX_VALUE, maxHlc = 0;
            int minFlags = 0xFFFF, maxFlags = 0;
            int minNodeId = Integer.MAX_VALUE, maxNodeId = 0;
            for (int i = 0; i < ts.length; i++)
            {
                T t = ts[i];
                long epoch = t.epoch();
                minEpoch = Math.min(epoch, minEpoch);
                maxEpoch = Math.max(epoch, maxEpoch);
                long hlc = t.hlc();
                minHlc = Math.min(hlc, minHlc);
                maxHlc = Math.max(hlc, maxHlc);
                int flags = t.flags();
                minFlags = Math.min(flags, minFlags);
                maxFlags = Math.max(flags, maxFlags);
                int nodeId = t.node.id;
                minNodeId = Math.min(nodeId, minNodeId);
                maxNodeId = Math.max(nodeId, maxNodeId);
            }

            int epochBits = BitUtils.numberOfBitsToRepresent(maxEpoch - minEpoch);
            int hlcBits = BitUtils.numberOfBitsToRepresent(maxHlc - minHlc);
            int flagBits = BitUtils.numberOfBitsToRepresent(maxFlags - minFlags);
            int nodeBits = BitUtils.numberOfBitsToRepresent(maxNodeId - minNodeId);

            // we could pack these a bit more tightly if we wanted to
            out.writeUnsignedVInt(minEpoch);
            out.writeUnsignedVInt(minHlc);
            out.writeUnsignedVInt32(minFlags);
            out.writeUnsignedVInt32(minNodeId);
            out.writeByte(epochBits);
            out.writeByte(hlcBits);
            out.writeByte(flagBits);
            out.writeByte(nodeBits);

            long finalMinEpoch = minEpoch;
            SerializePacked.serializePacked((in, i) -> in[i].epoch() - finalMinEpoch, ts, 0, ts.length, maxEpoch - minEpoch, out);
            long finalMinHlc = minHlc;
            SerializePacked.serializePacked((in, i) -> in[i].hlc() - finalMinHlc, ts, 0, ts.length, maxHlc - minHlc, out);
            long finalMinFlags = minFlags;
            SerializePacked.serializePacked((in, i) -> in[i].flags() - finalMinFlags, ts, 0, ts.length, maxFlags - minFlags, out);
            long finalMinNodeId = minNodeId;
            SerializePacked.serializePacked((in, i) -> in[i].node.id - finalMinNodeId, ts, 0, ts.length, maxNodeId - minNodeId, out);
        }

        public int flags(T ts)
        {
            long epoch = ts.epoch();
            long hlc = ts.hlc();
            int flags = ts.flags();
            int epochLength = length(epoch, EPOCH_MIN_LENGTH);
            int hlcLength = length(hlc, HLC_MIN_LENGTH);
            int flagsLength = length(flags, FLAGS_MIN_LENGTH);
            int nodeLength = length(ts.node.id, NODE_MIN_LENGTH);
            return encodeLength(epochLength, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK)
                 | encodeLength(hlcLength,   HLC_SHIFT,   HLC_MIN_LENGTH,   HLC_MASK)
                 | encodeLength(flagsLength, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK)
                 | encodeLength(nodeLength,  NODE_SHIFT,  NODE_MIN_LENGTH,  NODE_MASK);
        }

        // exactly the same fundamental format as serialize(), only we interleave the length bits with the values, maintaining ordering
        public <V> int serializeComparable(T ts, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            Invariants.require(encodeSpecial(ts) == 0);
            long epoch = ts.epoch();
            long hlc = ts.hlc();
            int flags = ts.flags();
            int epochLength = length(epoch, EPOCH_MIN_LENGTH);
            int hlcLength = length(hlc, HLC_MIN_LENGTH);
            int flagsLength = length(flags, FLAGS_MIN_LENGTH);
            int nodeLength = length(ts.node.id, NODE_MIN_LENGTH);

            long pack = packLength(epochLength, epochLength * 8, EPOCH_MIN_LENGTH, EPOCH_MASK);
            pack |= epoch;
            pack <<= 5;
            pack |= packLength(hlcLength, 3, HLC_MIN_LENGTH, HLC_MASK);
            pack |= hlc >>> ((hlcLength*8)-3);
            accessor.putLeastSignificantBytes(dst, position, pack, epochLength + 1);
            position += epochLength + 1;

            hlc <<= 3;
            hlc |= packLength(flagsLength, 2, FLAGS_MIN_LENGTH, FLAGS_MASK);
            hlc |= flags >>> ((flagsLength * 8) - 2);
            accessor.putLeastSignificantBytes(dst, position, hlc, hlcLength);
            position += hlcLength;

            pack = (long)flags << (2 + nodeLength * 8);
            pack |= packLength(nodeLength, nodeLength * 8, NODE_MIN_LENGTH, NODE_MASK);
            pack |= ts.node.id & 0xffffffffL;
            accessor.putLeastSignificantBytes(dst, position, pack, flagsLength + nodeLength);
            position += flagsLength + nodeLength;
            return position - offset;
        }

        public <V> int serialize(T ts, V dst, ValueAccessor<V> accessor, int offset)
        {
            {
                byte specialByte = encodeSpecial(ts);
                if (specialByte != 0)
                {
                    Invariants.require(specialByte < 0);
                    accessor.putByte(dst, offset, specialByte);
                    return 1;
                }
            }

            long epoch = ts.epoch();
            long hlc = ts.hlc();
            int flags = ts.flags();
            int epochLength = length(epoch, EPOCH_MIN_LENGTH);
            int hlcLength = length(hlc, HLC_MIN_LENGTH);
            int flagsLength = length(flags, FLAGS_MIN_LENGTH);
            int nodeLength = length(ts.node.id, NODE_MIN_LENGTH);
            int encodingFlags = encodeLength(epochLength, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK)
                              | encodeLength(hlcLength,   HLC_SHIFT,   HLC_MIN_LENGTH,   HLC_MASK)
                              | encodeLength(flagsLength, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK)
                              | encodeLength(nodeLength,  NODE_SHIFT,  NODE_MIN_LENGTH,  NODE_MASK);
            Invariants.require(((byte)encodingFlags) >= 0);

            int position = offset;
            position += accessor.putByte(dst, position, (byte)encodingFlags);
            position += accessor.putLeastSignificantBytes(dst, position, epoch, epochLength);
            position += accessor.putLeastSignificantBytes(dst, position, hlc, hlcLength);
            position += accessor.putLeastSignificantBytes(dst, position, flags, flagsLength);
            position += accessor.putLeastSignificantBytes(dst, position, ts.node.id, nodeLength);
            return position - offset;
        }

        public ByteBuffer serialize(T ts)
        {
            int size = Math.toIntExact(serializedSize(ts));
            ByteBuffer result = ByteBuffer.allocate(size);
            serialize(ts, result, ByteBufferAccessor.instance, 0);
            return result;
        }

        public void serialize(T ts, ByteBuffer out)
        {
            int position = out.position();
            position += serialize(ts, out, ByteBufferAccessor.instance, 0);
            out.position(position);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            int encodingFlags = in.readByte();
            if (encodingFlags < 0)
                return;
            in.skipBytesFully(lengthWithFlags(encodingFlags));
        }

        public int lengthWithFlags(int encodingFlags)
        {
            int epochLength = decodeLength(encodingFlags, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK);
            int hlcLength = decodeLength(encodingFlags, HLC_SHIFT, HLC_MIN_LENGTH, HLC_MASK);
            int flagsLength = decodeLength(encodingFlags, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK);
            int nodeLength = decodeLength(encodingFlags, NODE_SHIFT, NODE_MIN_LENGTH, NODE_MASK);
            return epochLength + hlcLength + flagsLength + nodeLength;
        }

        @Override
        public T deserialize(DataInputPlus in) throws IOException
        {
            int encodingFlags = in.readByte();
            if (encodingFlags < 0)
                return decodeSpecial(encodingFlags);
            return deserializeFixed(encodingFlags, in);
        }

        public T[] deserializeArray(DataInputPlus in) throws IOException
        {
            int length = in.readUnsignedVInt32();
            if (length == 0)
                return allocator.apply(0);

            // we could pack these a bit more tightly if we wanted to
            long minEpoch = in.readUnsignedVInt();
            long minHlc = in.readUnsignedVInt();
            int minFlags = in.readUnsignedVInt32();
            int minNodeId = in.readUnsignedVInt32();
            int epochBits = in.readByte();
            int hlcBits = in.readByte();
            int flagBits = in.readByte();
            int nodeBits = in.readByte();

            long[] bits = ArrayBuffers.cachedLongs().getLongs(length * 2);
            SerializePacked.deserializePacked((out, i, v) -> out[i*2] = Timestamp.epochMsb(minEpoch + v), bits, 0, length, mask(epochBits), in);
            SerializePacked.deserializePacked((out, i, v) -> {
                long hlc = minHlc + v;
                out[i*2] |= Timestamp.hlcMsb(hlc);
                out[i*2+1] = Timestamp.hlcLsb(hlc);
            }, bits, 0, length, mask(hlcBits), in);
            SerializePacked.deserializePacked((out, i, v) -> out[i*2 + 1] |= minFlags + v, bits, 0, length, mask(flagBits), in);
            T[] ts = allocator.apply(length);
            SerializePacked.deserializePacked((out, i, v) -> {
                Node.Id id = new Node.Id(minNodeId + (int)v);
                ts[i] = rawFactory.create(bits[i*2], bits[i*2 + 1], id);
            }, bits, 0, length, mask(nodeBits), in);
            ArrayBuffers.cachedLongs().forceDiscard(bits);
            return ts;
        }

        private static long mask(int bits)
        {
            return bits == 0 ? 0 : -1L >>> (64 - bits);
        }

        public T deserializeFixed(int encodingFlags, DataInputPlus in) throws IOException
        {
            Invariants.require(((byte)encodingFlags) >= 0);
            int epochLength = decodeLength(encodingFlags, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK);
            int hlcLength = decodeLength(encodingFlags, HLC_SHIFT, HLC_MIN_LENGTH, HLC_MASK);
            int flagsLength = decodeLength(encodingFlags, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK);
            int nodeLength = decodeLength(encodingFlags, NODE_SHIFT, NODE_MIN_LENGTH, NODE_MASK);
            return deserialize(epochLength, hlcLength, flagsLength, nodeLength, in);
        }

        private T deserialize(int epochLength, int hlcLength, int flagsLength, int nodeLength, DataInputPlus in) throws IOException
        {
            long epoch = in.readLeastSignificantBytes(epochLength);
            long hlc = in.readLeastSignificantBytes(hlcLength);
            int flags = Math.toIntExact(in.readLeastSignificantBytes(flagsLength));
            int nodeId = (int)in.readLeastSignificantBytes(nodeLength);
            return factory.create(epoch, hlc, flags, new Node.Id(nodeId));
        }

        public <V> T deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            int encodingFlags = accessor.getByte(src, offset);
            if (encodingFlags < 0)
                return decodeSpecial(encodingFlags);
            ++offset;
            int epochLength = decodeLength(encodingFlags, EPOCH_SHIFT, EPOCH_MIN_LENGTH, EPOCH_MASK);
            int hlcLength = decodeLength(encodingFlags, HLC_SHIFT, HLC_MIN_LENGTH, HLC_MASK);
            int flagsLength = decodeLength(encodingFlags, FLAGS_SHIFT, FLAGS_MIN_LENGTH, FLAGS_MASK);
            int nodeLength = decodeLength(encodingFlags, NODE_SHIFT, NODE_MIN_LENGTH, NODE_MASK);
            long epoch = accessor.getLeastSignificantBytes(src, offset, epochLength);
            offset += epochLength;
            long hlc = accessor.getLeastSignificantBytes(src, offset, hlcLength);
            offset += hlcLength;
            int flags = Math.toIntExact(accessor.getLeastSignificantBytes(src, offset, flagsLength));
            offset += flagsLength;
            int nodeId = (int)accessor.getLeastSignificantBytes(src, offset, nodeLength);
            return factory.create(epoch, hlc, flags, new Node.Id(nodeId));
        }

        public T deserialize(ByteBuffer buffer, int position)
        {
            return deserialize(buffer, ByteBufferAccessor.instance, position);
        }

        public T deserialize(ByteBuffer buffer)
        {
            return deserialize(buffer, ByteBufferAccessor.instance, 0);
        }

        // exactly the same fundamental format as deserialize(), only we interleave the length bits with the values, maintaining ordering
        public <V> T deserializeComparable(V src, ValueAccessor<V> accessor, int offset)
        {
            int b = accessor.getByte(src, offset++);
            int epochLength = decodeLength(b, 5, EPOCH_MIN_LENGTH, EPOCH_MASK);
            long bits64 = accessor.getLeastSignificantBytes(src, offset, epochLength);
            offset += epochLength;
            long epoch = (b&0x1fL) << (epochLength*8 - 5);
            epoch |= bits64 >>> 5;

            int hlcLength = decodeLength((int)bits64, 3, HLC_MIN_LENGTH, HLC_MASK);
            long hlc = (bits64 & 0x7L) << (hlcLength*8 - 3);
            bits64 = accessor.getLeastSignificantBytes(src, offset, hlcLength);
            offset += hlcLength;
            hlc |= bits64 >>> 3;

            int flagsLength = decodeLength((int)bits64, 2, FLAGS_MIN_LENGTH, FLAGS_MASK);
            int flags = ((int)bits64 & 0x3) << (flagsLength*8-2);
            int bits32 = (int) accessor.getLeastSignificantBytes(src, offset, flagsLength);
            offset += flagsLength;
            flags |= bits32 >>> 2;

            int nodeLength = decodeLength(bits32, 0, NODE_MIN_LENGTH, NODE_MASK);
            int node = (int) accessor.getLeastSignificantBytes(src, offset, nodeLength);
            return factory.create(epoch, hlc, flags, new Node.Id(node));
        }

        @Override
        public long serializedSize(T ts)
        {
            if (encodeSpecial(ts) != 0)
                return 1;
            int epochLength = length(ts.epoch(), EPOCH_MIN_LENGTH);
            int hlcLength = length(ts.hlc(), HLC_MIN_LENGTH);
            int flagsLength = length(ts.flags(), FLAGS_MIN_LENGTH);
            int nodeLength = length(ts.node.id, NODE_MIN_LENGTH);
            return 1 + epochLength + hlcLength + flagsLength + nodeLength;
        }

        public long serializedArraySize(T[] ts)
        {
            if (ts.length == 0)
                return 1;

            long minEpoch = Long.MAX_VALUE, maxEpoch = 0;
            long minHlc = Long.MAX_VALUE, maxHlc = 0;
            int minFlags = 0xFFFF, maxFlags = 0;
            int minNodeId = Integer.MAX_VALUE, maxNodeId = 0;
            for (int i = 0; i < ts.length; i++)
            {
                T t = ts[i];
                long epoch = t.epoch();
                minEpoch = Math.min(epoch, minEpoch);
                maxEpoch = Math.max(epoch, maxEpoch);
                long hlc = t.hlc();
                minHlc = Math.min(hlc, minHlc);
                maxHlc = Math.max(hlc, maxHlc);
                int flags = t.flags();
                minFlags = Math.min(flags, minFlags);
                maxFlags = Math.max(flags, maxFlags);
                int nodeId = t.node.id;
                minNodeId = Math.min(nodeId, minNodeId);
                maxNodeId = Math.max(nodeId, maxNodeId);
            }

            int epochBits = BitUtils.numberOfBitsToRepresent(maxEpoch - minEpoch);
            int hlcBits = BitUtils.numberOfBitsToRepresent(maxHlc - minHlc);
            int flagBits = BitUtils.numberOfBitsToRepresent(maxFlags - minFlags);
            int nodeBits = BitUtils.numberOfBitsToRepresent(maxNodeId - minNodeId);

            return VIntCoding.sizeOfUnsignedVInt(ts.length)
                   + VIntCoding.sizeOfUnsignedVInt(minEpoch)
                   + VIntCoding.sizeOfUnsignedVInt(minHlc)
                   + VIntCoding.sizeOfUnsignedVInt(minFlags)
                   + VIntCoding.sizeOfUnsignedVInt(minNodeId)
                   + 4
                   + serializedPackedBitsSize(ts.length, epochBits)
                   + serializedPackedBitsSize(ts.length, hlcBits)
                   + serializedPackedBitsSize(ts.length, flagBits)
                   + serializedPackedBitsSize(ts.length, nodeBits);
        }

        private static int length(long value, int minLength)
        {
            int length = ((64 + 7) - Long.numberOfLeadingZeros(value))/8;
            return Math.max(length, minLength);
        }

        private static int length(int value, int minLength)
        {
            int length = ((32 + 7) - Integer.numberOfLeadingZeros(value))/8;
            return Math.max(length, minLength);
        }

        private static int encodeLength(int length, int shift, int minLength, int mask)
        {
            int encoded = length - minLength;
            Invariants.require(encoded <= mask);
            return encoded << shift;
        }

        private static int reencodePartDecodedLength(int length, int shift, int mask)
        {
            Invariants.require(length <= mask);
            return length << shift;
        }

        private static long packLength(int length, int shift, int minLength, int mask)
        {
            int encoded = length - minLength;
            Invariants.require(encoded <= mask);
            return (long)encoded << shift;
        }

        private static int decodeLength(int encodingFlags, int shift, int minLength, int mask)
        {
            return minLength + ((encodingFlags >>> shift) & mask);
        }

        private static int maxPartDecoded(int flagsa, int flagsb, int shift, int mask)
        {
            return Math.max(((flagsa >>> shift) & mask), (flagsb >>> shift) & mask);
        }
    }

    public static class BallotSerializer extends VariableWidthTimestampSerializer<Ballot>
    {
        private static final byte ZERO_BYTE = (byte) 0x81;
        private static final byte MAX_BYTE = (byte) 0x82;
        private BallotSerializer()
        {
            super(Ballot::fromValues, Ballot::fromBits, Ballot[]::new);
        }

        @Override
        byte encodeSpecial(Ballot value)
        {
            if (value == null) return NULL_BYTE;
            if (value == Ballot.ZERO) return ZERO_BYTE;
            if (value == Ballot.MAX) return MAX_BYTE;
            return 0;
        }

        @Override
        Ballot decodeSpecial(int specialByte)
        {
            if (specialByte == NULL_BYTE) return null;
            if (specialByte == ZERO_BYTE) return Ballot.ZERO;
            if (specialByte == MAX_BYTE) return Ballot.MAX;
            throw new IllegalArgumentException("Unexpected specialByte: " + specialByte);
        }
    }

    public static class PartialTxnSerializer
    implements IVersionedSerializer<PartialTxn>
    {
        private final ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> readSerializer;
        private final UnversionedSerializer<Query> querySerializer;
        private final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> updateSerializer;
        private final UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeysSerializer;

        public PartialTxnSerializer(ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> readSerializer,
                                    UnversionedSerializer<Query> querySerializer,
                                    ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> updateSerializer,
                                    UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeysSerializer)
        {
            this.readSerializer = readSerializer;
            this.querySerializer = querySerializer;
            this.updateSerializer = updateSerializer;
            this.tablesAndKeysSerializer = tablesAndKeysSerializer;
        }

        @Override
        public void serialize(PartialTxn txn, DataOutputPlus out, Version version) throws IOException
        {
            PartialTxn.InMemory cast = (PartialTxn.InMemory)txn;
            CommandSerializers.kind.serialize(txn.kind(), out);
            TableMetadatasAndKeys tablesAndKeys = (TableMetadatasAndKeys) cast.implementationDefined;
            if (tablesAndKeys != null) tablesAndKeysSerializer.serialize(tablesAndKeys, out);
            else KeySerializers.seekables.serialize(txn.keys(), out);
            readSerializer.serialize(txn.read(), tablesAndKeys, out, version);
            querySerializer.serialize(txn.query(), out);
            out.writeBoolean(txn.update() != null);
            if (txn.update() != null)
                updateSerializer.serialize(txn.update(), tablesAndKeys, out, version);
        }

        @Override
        public PartialTxn deserialize(DataInputPlus in, Version version) throws IOException
        {
            Txn.Kind kind = CommandSerializers.kind.deserialize(in);
            TableMetadatasAndKeys tablesAndKeys = tablesAndKeysSerializer.deserialize(in);
            Seekables keys = tablesAndKeys != null ? tablesAndKeys.keys : KeySerializers.seekables.deserialize(in);
            Read read = readSerializer.deserialize(tablesAndKeys, in, version);
            Query query = querySerializer.deserialize(in);
            Update update = in.readBoolean() ? updateSerializer.deserialize(tablesAndKeys, in, version) : null;
            return new PartialTxn.InMemory(kind, keys, read, query, update, tablesAndKeys);
        }

        @Override
        public long serializedSize(PartialTxn txn, Version version)
        {
            long size = CommandSerializers.kind.serializedSize(txn.kind());
            TableMetadatasAndKeys tablesAndKeys = (TableMetadatasAndKeys) ((PartialTxn.InMemory)txn).implementationDefined;
            if (tablesAndKeys != null) size += tablesAndKeysSerializer.serializedSize(tablesAndKeys);
            else size += KeySerializers.seekables.serializedSize(txn.keys());
            size += readSerializer.serializedSize(txn.read(), tablesAndKeys, version);
            size += querySerializer.serializedSize(txn.query());
            size += TypeSizes.sizeof(txn.update() != null);
            if (txn.update() != null)
                size += updateSerializer.serializedSize(txn.update(), tablesAndKeys, version);
            return size;
        }
    }

    public static final ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> read;
    public static final UnversionedSerializer<Query> query;
    public static final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> update;
    public static final ParameterisedVersionedSerializer<Write, Seekables, Version> write;
    public static final UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeys;

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
        tablesAndKeys = querySerializers.tablesAndKeys;

        partialTxn = querySerializers.partialTxn;
        nullablePartialTxn = querySerializers.nullablePartialTxn;
    }

    @VisibleForTesting
    public static class QuerySerializers
    {
        public final ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> read;
        public final UnversionedSerializer<Query> query;
        public final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> update;
        public final ParameterisedVersionedSerializer<Write, Seekables, Version> write;
        public final UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeys;

        public final VersionedSerializer<PartialTxn, Version> partialTxn;
        public final VersionedSerializer<PartialTxn, Version> nullablePartialTxn;

        private QuerySerializers()
        {
            this((ParameterisedVersionedSerializer) TxnRead.serializer,
                 (UnversionedSerializer) TxnQuery.serializer,
                 (ParameterisedVersionedSerializer) AccordUpdate.serializer,
                 (ParameterisedVersionedSerializer) TxnWrite.serializer,
                 TableMetadatasAndKeys.serializer);
        }

        public QuerySerializers(ParameterisedVersionedSerializer<Read, TableMetadatasAndKeys, Version> read,
                                UnversionedSerializer<Query> query,
                                ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> update,
                                ParameterisedVersionedSerializer<Write, Seekables, Version> write,
                                UnversionedSerializer<TableMetadatasAndKeys> tablesAndKeys)
        {
            this.read = read;
            this.query = query;
            this.update = update;
            this.write = write;
            this.tablesAndKeys = tablesAndKeys;

            this.partialTxn = new PartialTxnSerializer(read, query, update, tablesAndKeys);
            this.nullablePartialTxn = NullableSerializer.wrap(partialTxn);
        }
    }

    public static final UnversionedSerializer<SaveStatus> saveStatus = EncodeAsVInt32.of(SaveStatus.class);
    public static final UnversionedSerializer<Status> status = EncodeAsVInt32.of(Status.class);
    public static final UnversionedSerializer<Durability> durability = EncodeAsVInt32.withoutNulls(Durability::encoded, Durability::forEncoded);
    public static final UnversionedSerializer<Durability.HasOutcome> outcomeDurability = EncodeAsVInt32.of(Durability.HasOutcome.class);

    public static final IVersionedSerializer<Writes> writes = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Writes writes, DataOutputPlus out, Version version) throws IOException
        {
            txnId.serialize(writes.txnId, out);
            ExecuteAtSerializer.serialize(writes.txnId, writes.executeAt, out);
            boolean hasWrite = writes.write != null;
            out.writeBoolean(hasWrite);
            KeySerializers.seekables.serialize(writes.keys, out);
            if (hasWrite)
                CommandSerializers.write.serialize(writes.write, writes.keys, out, version);
        }

        @Override
        public Writes deserialize(DataInputPlus in, Version version) throws IOException
        {
            TxnId id = txnId.deserialize(in);
            Timestamp executeAt = ExecuteAtSerializer.deserialize(id, in);
            boolean hasWrite = in.readBoolean();
            Seekables seekables = KeySerializers.seekables.deserialize(in);
            Write write = null;
            if (hasWrite)
                write = CommandSerializers.write.deserialize(seekables, in, version);
            return new Writes(id, executeAt, seekables, write);
        }

        @Override
        public void skip(DataInputPlus in, Version version) throws IOException
        {
            txnId.skip(in);
            ExecuteAtSerializer.skip(null, in);
            boolean hasWrite = in.readBoolean();
            if (hasWrite)
            {
                Seekables seekables = KeySerializers.seekables.deserialize(in);
                CommandSerializers.write.skip(seekables, in, version);
            }
            else KeySerializers.seekables.skip(in);
        }

        @Override
        public long serializedSize(Writes writes, Version version)
        {
            long size = txnId.serializedSize(writes.txnId);
            size += ExecuteAtSerializer.serializedSize(writes.txnId, writes.executeAt);
            boolean hasWrites = writes.write != null;
            size += KeySerializers.seekables.serializedSize(writes.keys);
            size += TypeSizes.sizeof(hasWrites);
            if (hasWrites)
                size += CommandSerializers.write.serializedSize(writes.write, writes.keys, version);
            return size;
        }
    };

    public static final VersionedSerializer<Writes, Version> nullableWrites = NullableSerializer.wrap(writes);
    public static final UnversionedSerializer<KnownDeps> knownDeps = EncodeAsVInt32.of(KnownDeps.class);
    public static final UnversionedSerializer<Infer.InvalidIf> invalidIf = EncodeAsVInt32.of(Infer.InvalidIf.class);

    public static final UnversionedSerializer<Known> known = EncodeAsVInt32.withNulls(known -> known.encoded, Known::new);

}