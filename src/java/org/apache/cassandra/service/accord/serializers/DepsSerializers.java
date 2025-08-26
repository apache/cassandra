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

import com.google.common.annotations.VisibleForTesting;

import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.VIntCoding;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.txnIdsToKeys;
import static accord.primitives.RangeDeps.SerializerSupport.ranges;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.txnIdsToRanges;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.deserializePackedInts;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializePackedInts;
import static org.apache.cassandra.service.accord.serializers.SerializePacked.serializedPackedIntsSize;

public class DepsSerializers
{
    public static final UnversionedSerializer<Range> tokenRange;
    public static final AbstractDepsSerializer<Deps> deps;
    public static final UnversionedSerializer<Deps> nullableDeps;
    public static final AbstractDepsSerializer<PartialDeps> partialDeps;
    public static final AbstractDepsSerializer<PartialDeps> partialDepsById;
    public static final UnversionedSerializer<PartialDeps> nullablePartialDeps;

    static
    {
        // We use a separate class for initialization to make it easier for BurnTest to plug its own serializers.
        Impl serializers = new Impl((UnversionedSerializer<Range>) (UnversionedSerializer<?>) TokenRange.serializer);
        tokenRange = serializers.tokenRange;
        deps = serializers.deps;
        nullableDeps = serializers.nullableDeps;
        partialDeps = serializers.partialDeps;
        partialDepsById = serializers.partialDepsById;
        nullablePartialDeps = serializers.nullablePartialDeps;
    }

    public static abstract class AbstractDepsSerializer<D extends Deps> implements UnversionedSerializer<D>
    {
        static final int KEYS_BY_TXNID = 0x1;
        static final int RANGES_BY_TXNID = 0x2;
        static final int FLAGS_SIZE = VIntCoding.sizeOfUnsignedVInt(KEYS_BY_TXNID | RANGES_BY_TXNID);
        final boolean forceByTxnId;

        protected final UnversionedSerializer<Range> tokenRange;
        public AbstractDepsSerializer(boolean forceByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            this.forceByTxnId = forceByTxnId;
            this.tokenRange = tokenRange;
        }

        abstract D deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException;

        @Override
        public void serialize(D deps, DataOutputPlus out) throws IOException
        {
            boolean keysByTxnId = forceByTxnId || deps.keyDeps.hasByTxnId();
            boolean rangesByTxnId = forceByTxnId || deps.rangeDeps.hasByTxnId();
            out.writeUnsignedVInt32((keysByTxnId ? KEYS_BY_TXNID : 0) | (rangesByTxnId ? RANGES_BY_TXNID : 0));
            {
                KeyDeps keyDeps = deps.keyDeps;
                KeySerializers.routingKeys.serialize(keyDeps.keys(), out);
                CommandSerializers.txnId.serializeArray(KeyDeps.SerializerSupport.txnIds(keyDeps), out);
                if (keysByTxnId) serializePackedXtoY(txnIdsToKeys(keyDeps), keyDeps.txnIdCount(), keyDeps.keys().size(), out);
                else serializePackedXtoY(keysToTxnIds(keyDeps), keyDeps.keys().size(), keyDeps.txnIdCount(), out);
            }
            {
                RangeDeps rangeDeps = deps.rangeDeps;
                KeySerializers.rangeArray.serialize(ranges(rangeDeps), out);
                CommandSerializers.txnId.serializeArray(RangeDeps.SerializerSupport.txnIds(rangeDeps), out);
                if (rangesByTxnId) serializePackedXtoY(txnIdsToRanges(rangeDeps), rangeDeps.txnIdCount(), rangeDeps.rangeCount(), out);
                else serializePackedXtoY(rangesToTxnIds(rangeDeps), rangeDeps.rangeCount(), rangeDeps.txnIdCount(), out);
            }
        }

        private static void serializePackedXtoY(int[] xtoy, int xCount, int yCount, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(xtoy.length);

            if ((xCount <= 1 || yCount <= 1) && (xtoy.length == xCount + yCount || xCount == 0 || yCount == 0))
            {
                // no point serializing as can be directly inferred
                if (Invariants.isParanoid())
                {
                    if (xCount == 1)
                    {
                        Invariants.require(xtoy[0] == xtoy.length, "%d != %d", xtoy[0], xtoy.length);
                        for (int i = 0 ; i < yCount ; ++i) Invariants.require(xtoy[1 + i] == i, "%d != %d", xtoy[1 + i], i);
                    }
                    else if (yCount == 1)
                    {
                        for (int i = 0 ; i < xCount ; ++i) Invariants.require(xtoy[i] == xCount + i + 1, "%d != %d", xtoy[i], xCount + i + 1);
                        for (int i = xCount ; i < xtoy.length ; ++i) Invariants.require(xtoy[i] == 0, "%d != %d", xtoy[i], 0);
                    }
                    else if (yCount == 0)
                    {
                        for (int i = 0 ; i < xCount ; ++i) Invariants.require(xtoy[i] == xCount, "%d != %d", xtoy[i], xCount);
                    }
                    else
                    {
                        Invariants.require(xtoy.length == 0);
                    }
                }
            }
            else
            {
                serializePackedInts(xtoy, 0, xCount, xtoy.length, out);
                serializePackedInts(xtoy, xCount, xtoy.length, yCount - 1, out);
            }
        }

        @Override
        public D deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            KeyDeps keyDeps;
            {
                RoutingKeys keys = KeySerializers.routingKeys.deserialize(in);
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] txnIdsToKeys = null, keysToTxnIds = null;
                if (0 != (flags & KEYS_BY_TXNID)) txnIdsToKeys = deserializePackedXtoY(txnIds.length, keys.size(), in);
                else keysToTxnIds = deserializePackedXtoY(keys.size(), txnIds.length, in);
                keyDeps = KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds, txnIdsToKeys);
            }

            RangeDeps rangeDeps;
            {
                Range[] ranges = KeySerializers.rangeArray.deserialize(in);
                TxnId[] txnIds = CommandSerializers.txnId.deserializeArray(in);
                int[] txnIdsToRanges = null, rangesToTxnIds = null;
                if (0 != (flags & RANGES_BY_TXNID)) txnIdsToRanges = deserializePackedXtoY(txnIds.length, ranges.length, in);
                else rangesToTxnIds = deserializePackedXtoY(ranges.length, txnIds.length, in);
                rangeDeps = RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds, txnIdsToRanges);
            }
            return deserialize(keyDeps, rangeDeps, in);
        }

        private static int[] deserializePackedXtoY(int xCount, int yCount, DataInputPlus in) throws IOException
        {
            int length = in.readUnsignedVInt32();
            int[] xtoy = new int[length];

            if ((xCount <= 1 || yCount <= 1) && (xtoy.length == xCount + yCount || xCount == 0 || yCount == 0))
            {
                // no point serializing as can be directly inferred
                if (xCount == 1)
                {
                    xtoy[0] = xtoy.length;
                    for (int i = 0 ; i < yCount ; ++i)
                        xtoy[1 + i] = i;
                }
                else if (yCount == 1)
                {
                    for (int i = 0 ; i < xCount ; ++i)
                        xtoy[i] = xCount + i + 1;
                }
                else if (yCount == 0)
                {
                    for (int i = 0 ; i < xCount ; ++i)
                        xtoy[i] = xCount;
                }
                else
                {
                    Invariants.require(length == 0);
                }
            }
            else
            {
                deserializePackedInts(xtoy, 0, xCount, xtoy.length, in);
                deserializePackedInts(xtoy, xCount, xtoy.length, yCount - 1, in);
            }
            return xtoy;
        }

        @Override
        public long serializedSize(D deps)
        {
            boolean keysByTxnId = forceByTxnId || deps.keyDeps.hasByTxnId();
            boolean rangesByTxnId = forceByTxnId || deps.rangeDeps.hasByTxnId();
            long size = FLAGS_SIZE;
            {
                KeyDeps keyDeps = deps.keyDeps;
                size += KeySerializers.routingKeys.serializedSize(keyDeps.keys());
                size += CommandSerializers.txnId.serializedArraySize(KeyDeps.SerializerSupport.txnIds(keyDeps));
                size += keysByTxnId ? serializedPackedXtoYSize(txnIdsToKeys(keyDeps), keyDeps.txnIdCount(), keyDeps.keys().size())
                                    : serializedPackedXtoYSize(keysToTxnIds(keyDeps), keyDeps.keys().size(), keyDeps.txnIdCount());
            }
            {
                RangeDeps rangeDeps = deps.rangeDeps;
                size += KeySerializers.rangeArray.serializedSize(ranges(rangeDeps));
                size += CommandSerializers.txnId.serializedArraySize(RangeDeps.SerializerSupport.txnIds(rangeDeps));
                size += rangesByTxnId ? serializedPackedXtoYSize(txnIdsToRanges(rangeDeps), rangeDeps.txnIdCount(), rangeDeps.rangeCount())
                                      : serializedPackedXtoYSize(rangesToTxnIds(rangeDeps), rangeDeps.rangeCount(), rangeDeps.txnIdCount());
            }
            return size;
        }

        private static long serializedPackedXtoYSize(int[] xtoy, int xCount, int yCount)
        {
            long size = VIntCoding.sizeOfUnsignedVInt(xtoy.length);
            if ((xCount <= 1 || yCount <= 1) && (xtoy.length == xCount + yCount || xCount == 0 || yCount == 0))
            {
                // no point serializing as can be directly inferred
            }
            else
            {
                size += serializedPackedIntsSize(xtoy, 0, xCount, xtoy.length);
                size += serializedPackedIntsSize(xtoy, xCount, xtoy.length, yCount - 1);
            }
            return size;
        }
    }

    static class PartialDepsSerializer extends AbstractDepsSerializer<PartialDeps>
    {
        public PartialDepsSerializer(boolean preferByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            super(preferByTxnId, tokenRange);
        }

        @Override
        PartialDeps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException
        {
            Participants<?> covering = KeySerializers.participants.deserialize(in);
            return new PartialDeps(covering, keyDeps, rangeDeps);
        }

        @Override
        public void serialize(PartialDeps partialDeps, DataOutputPlus out) throws IOException
        {
            super.serialize(partialDeps, out);
            KeySerializers.participants.serialize(partialDeps.covering, out);
        }

        @Override
        public long serializedSize(PartialDeps partialDeps)
        {
            return super.serializedSize(partialDeps)
                   + KeySerializers.participants.serializedSize(partialDeps.covering);
        }
    }

    static class DepsSerializer extends AbstractDepsSerializer<Deps>
    {
        public DepsSerializer(boolean preferByTxnId, UnversionedSerializer<Range> tokenRange)
        {
            super(preferByTxnId, tokenRange);
        }

        @Override
        Deps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException
        {
            return new Deps(keyDeps, rangeDeps);
        }
    }

    @VisibleForTesting
    public static class Impl
    {
        final UnversionedSerializer<Range> tokenRange;
        final DepsSerializer deps;
        final UnversionedSerializer<Deps> nullableDeps;
        final PartialDepsSerializer partialDeps;
        final PartialDepsSerializer partialDepsById;
        final UnversionedSerializer<PartialDeps> nullablePartialDeps;

        public Impl(UnversionedSerializer<Range> tokenRange)
        {
            this.tokenRange = tokenRange;
            this.deps = new DepsSerializer(false, tokenRange);
            this.nullableDeps = NullableSerializer.wrap(deps);
            this.partialDeps = new PartialDepsSerializer(false, tokenRange);
            this.partialDepsById = new PartialDepsSerializer(true, tokenRange);
            this.nullablePartialDeps = NullableSerializer.wrap(partialDeps);
        }
    }

}