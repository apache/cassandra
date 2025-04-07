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
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.utils.UnhandledEnum;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static accord.utils.SortedArrays.Search.FAST;

/**
 * De/serialize a structure that can refer to a known superset of RoutingKeys/Keys/Ranges...
 */
public interface IVersionedWithKeysSerializer<K extends Routables<?>, T> extends IVersionedSerializer<T>
{
    /**
     * Serialize the specified type into the specified DataOutputStream instance.
     *
     * @param t type that needs to be serialized
     * @param out DataOutput into which serialization needs to happen.
     * @param version protocol version
     * @throws IOException if serialization fails
     */
    void serialize(K keys, T t, DataOutputPlus out, Version version) throws IOException;

    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    T deserialize(K keys, DataInputPlus in, Version version) throws IOException;

    /**
     * Calculate serialized size of object without actually serializing.
     * @param t object to calculate serialized size
     * @param version protocol version
     * @return serialized size of object t
     */
    long serializedSize(K keys, T t, Version version);

    abstract class AbstractWithKeysSerializer
    {
        /**
         * If both ends have a pre-shared superset of the columns we are serializing, we can send them much
         * more efficiently. Both ends must provide the identically same set of columns.
         */
        protected void serializeSubsetInternal(Routables<?> serialize, Routables<?> superset, DataOutputPlus out) throws IOException
        {
            /**
             * We weight this towards small sets, and sets where the majority of items are present, since
             * we expect this to mostly be used for serializing result sets.
             *
             * For supersets with fewer than 64 columns, we encode a bitmap of *missing* columns,
             * which equates to a zero (single byte) when all columns are present, and otherwise
             * a positive integer that can typically be vint encoded efficiently.
             *
             * If we have 64 or more columns, we cannot neatly perform a bitmap encoding, so we just switch
             * to a vint encoded set of deltas, either adding or subtracting (whichever is most efficient).
             * We indicate this switch by sending our bitmap with every bit set, i.e. -1L
             */
            int serializeCount = serialize.size();
            int supersetCount = superset.size();
            if (serializeCount == supersetCount)
            {
                out.writeUnsignedVInt(0L);
            }
            else if (supersetCount < 64)
            {
                switch (serialize.domain())
                {
                    default: throw UnhandledEnum.unknown(serialize.domain());
                    case Key:
                        out.writeUnsignedVInt(encodeBitmap((AbstractUnseekableKeys)serialize, (AbstractUnseekableKeys)superset, supersetCount));
                        break;
                    case Range:
                        out.writeUnsignedVInt(encodeBitmap((AbstractRanges)serialize, (AbstractRanges)superset, supersetCount));
                        break;
                }
            }
            else
            {
                switch (serialize.domain())
                {
                    default: throw UnhandledEnum.unknown(serialize.domain());
                    case Key:
                        serializeLargeSubset((AbstractUnseekableKeys)serialize, serializeCount, (AbstractUnseekableKeys)superset, supersetCount, out);
                        break;
                    case Range:
                        serializeLargeSubset((AbstractRanges)serialize, serializeCount, (AbstractRanges)superset, supersetCount, out);
                        break;
                }
            }
        }

        public long serializedSubsetSizeInternal(Routables<?> serialize, Routables<?> superset)
        {
            int columnCount = serialize.size();
            int supersetCount = superset.size();
            if (columnCount == supersetCount)
            {
                return TypeSizes.sizeofUnsignedVInt(0);
            }
            else if (supersetCount < 64)
            {
                switch (serialize.domain())
                {
                    default: throw UnhandledEnum.unknown(serialize.domain());
                    case Key:
                        return TypeSizes.sizeofUnsignedVInt(encodeBitmap((AbstractUnseekableKeys)serialize, (AbstractUnseekableKeys)superset, supersetCount));
                    case Range:
                        return TypeSizes.sizeofUnsignedVInt(encodeBitmap((AbstractRanges)serialize, (AbstractRanges)superset, supersetCount));
                }
            }
            else
            {
                switch (serialize.domain())
                {
                    default: throw UnhandledEnum.unknown(serialize.domain());
                    case Key:
                        return serializeLargeSubsetSize((AbstractUnseekableKeys)serialize, columnCount, (AbstractUnseekableKeys)superset, supersetCount);
                    case Range:
                        return serializeLargeSubsetSize((AbstractRanges)serialize, columnCount, (AbstractRanges)superset, supersetCount);
                }
            }
        }

        // encodes a 1 bit for every *missing* column, on the assumption presence is more common,
        // and because this is consistent with encoding 0 to represent all present
        private static <K extends RoutableKey> long encodeBitmap(AbstractKeys<K> serialize, AbstractKeys<K> superset, int supersetCount)
        {
            // the index we would encounter next if all columns are present
            long bitmap = superset.foldl(serialize, (k, p1, v, i) -> {
                return v | (1L << i);
            }, 0L, 0L, -1L);
            bitmap ^= -1L >>> (64 - supersetCount);
            return bitmap;
        }

        private static long encodeBitmap(AbstractRanges serialize, AbstractRanges superset, int supersetCount)
        {
            // the index we would encounter next if all columns are present
            long bitmap = superset.foldl(serialize, (k, p1, v, i) -> {
                return v | (1L << i);
            }, 0L, 0L, -1L);
            bitmap ^= -1L >>> (64 - supersetCount);
            return bitmap;
        }

        @DontInline
        private <K extends RoutableKey> void serializeLargeSubset(AbstractKeys<K> serialize, int serializeCount, AbstractKeys<K> superset, int supersetCount, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(supersetCount - serializeCount);
            int serializeIndex = 0, supersetIndex = 0;
            while (serializeIndex < serializeCount)
            {
                int prevSupersetIndex = supersetIndex;
                int nextSupersetIndex;
                do
                {
                    nextSupersetIndex = superset.findNext(supersetIndex, serialize.get(serializeIndex++), FAST);
                    if (supersetIndex + 1 != nextSupersetIndex)
                        break;
                    supersetIndex++;
                }
                while (serializeIndex < serializeCount);

                out.writeUnsignedVInt32(supersetIndex - prevSupersetIndex);
                out.writeUnsignedVInt32(nextSupersetIndex - supersetIndex);
                supersetIndex = nextSupersetIndex;
            }
        }

        @DontInline
        private void serializeLargeSubset(AbstractRanges serialize, int serializeCount, AbstractRanges superset, int supersetCount, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(supersetCount - serializeCount);
            int serializeIndex = 0, supersetIndex = 0;
            while (serializeIndex < serializeCount)
            {
                int prevSupersetIndex = supersetIndex;
                int nextSupersetIndex;
                do
                {
                    nextSupersetIndex = superset.findNext(supersetIndex, serialize.get(serializeIndex++), FAST);
                    if (supersetIndex + 1 != nextSupersetIndex)
                        break;
                    supersetIndex++;
                }
                while (serializeIndex < serializeCount);

                out.writeUnsignedVInt32(supersetIndex - prevSupersetIndex);
                out.writeUnsignedVInt32(nextSupersetIndex - supersetIndex);
                supersetIndex = nextSupersetIndex;
            }
        }

        public Routables<?> deserializeSubsetInternal(Routables<?> superset, DataInputPlus in) throws IOException
        {
            switch (superset.domain())
            {
                default: throw UnhandledEnum.unknown(superset.domain());
                case Key: return deserializeRoutingKeySubset((AbstractUnseekableKeys) superset, in, (ks, s) -> ks == null ? s : RoutingKeys.of(ks));
                case Range: return deserializeRangeSubset((AbstractRanges) superset, in, (rs, s) -> rs == null ? s : Ranges.of(rs));
            }
        }

        public void skipSubsetInternal(int supersetCount, DataInputPlus in) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            if (supersetCount <= 64)
                return;

            int deserializeCount = supersetCount - (int)encoded;
            int count = 0;
            while (count < deserializeCount)
            {
                count += in.readUnsignedVInt32();
                in.readUnsignedVInt32();
            }
        }

        public <T, S extends AbstractUnseekableKeys> T deserializeRoutingKeySubset(S superset, DataInputPlus in, BiFunction<RoutingKey[], S, T> result) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            int supersetCount = superset.size();
            if (encoded == 0L)
                return result.apply(null, superset);
            else if (supersetCount >= 64)
                return result.apply(deserializeLargeRoutingKeySubset(in, superset, supersetCount, (int) encoded), superset);
            else
                return result.apply(deserializeSmallRoutingKeySubset(encoded, superset, supersetCount), superset);
        }

        public <T, S extends AbstractRanges> T deserializeRangeSubset(S superset, DataInputPlus in, BiFunction<Range[], S, T> result) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            int supersetCount = superset.size();
            if (encoded == 0L)
                return result.apply(null, superset);
            else if (supersetCount >= 64)
                return result.apply(deserializeLargeRangeSubset(in, superset, supersetCount, (int) encoded), superset);
            else
                return result.apply(deserializeSmallRangeSubsetArray(encoded, superset, supersetCount), superset);
        }

        private RoutingKey[] deserializeSmallRoutingKeySubset(long encoded, AbstractUnseekableKeys superset, int supersetCount)
        {
            return deserializeSmallSubsetArray(encoded, superset, supersetCount, RoutingKey[]::new);
        }

        private Range[] deserializeSmallRangeSubsetArray(long encoded, AbstractRanges superset, int supersetCount)
        {
            return deserializeSmallSubsetArray(encoded, superset, supersetCount, Range[]::new);
        }

        private <R extends Routable> R[] deserializeSmallSubsetArray(long encoded, Routables<R> superset, int supersetCount, IntFunction<R[]> allocator)
        {
            encoded ^= -1L >>> (64 - supersetCount);
            int deserializeCount = Long.bitCount(encoded);
            R[] out = allocator.apply(deserializeCount);
            int count = 0;
            while (encoded != 0)
            {
                long lowestBit = Long.lowestOneBit(encoded);
                out[count++] = superset.get(Long.numberOfTrailingZeros(lowestBit));
                encoded ^= lowestBit;
            }
            return out;
        }

        @DontInline
        private RoutingKey[] deserializeLargeRoutingKeySubset(DataInputPlus in, AbstractUnseekableKeys superset, int supersetCount, int delta) throws IOException
        {
            int deserializeCount = supersetCount - delta;
            RoutingKey[] out = new RoutingKey[deserializeCount];
            int supersetIndex = 0;
            int count = 0;
            while (count < deserializeCount)
            {
                int takeCount = in.readUnsignedVInt32();
                while (takeCount-- > 0) out[count++] = superset.get(supersetIndex++);
                supersetIndex += in.readUnsignedVInt32();
            }
            return out;
        }

        @DontInline
        private Range[] deserializeLargeRangeSubset(DataInputPlus in, AbstractRanges superset, int supersetCount, int delta) throws IOException
        {
            int deserializeCount = supersetCount - delta;
            Range[] out = new Range[deserializeCount];
            int supersetIndex = 0;
            int count = 0;
            while (count < deserializeCount)
            {
                int takeCount = in.readUnsignedVInt32();
                while (takeCount-- > 0) out[count++] = superset.get(supersetIndex++);
                supersetIndex += in.readUnsignedVInt32();
            }
            return out;
        }

        @DontInline
        private <K extends RoutableKey> long serializeLargeSubsetSize(AbstractKeys<K> serialize, int serializeCount, AbstractKeys<K> superset, int supersetCount)
        {
            long size = TypeSizes.sizeofUnsignedVInt(supersetCount - serializeCount);
            int serializeIndex = 0, supersetIndex = 0;
            while (serializeIndex < serializeCount)
            {
                int prevSupersetIndex = supersetIndex;
                int nextSupersetIndex;
                do
                {
                    nextSupersetIndex = superset.findNext(supersetIndex, serialize.get(serializeIndex++), FAST);
                    if (supersetIndex + 1 != nextSupersetIndex)
                        break;
                    supersetIndex++;
                }
                while (serializeIndex < serializeCount);

                size += TypeSizes.sizeofUnsignedVInt(supersetIndex - prevSupersetIndex);
                size += TypeSizes.sizeofUnsignedVInt(nextSupersetIndex - supersetIndex);
                supersetIndex = nextSupersetIndex;
            }
            return size;
        }

        @DontInline
        private long serializeLargeSubsetSize(AbstractRanges serialize, int serializeCount, AbstractRanges superset, int supersetCount)
        {
            long size = TypeSizes.sizeofUnsignedVInt(supersetCount - serializeCount);
            int serializeIndex = 0, supersetIndex = 0;
            while (serializeIndex < serializeCount)
            {
                int prevSupersetIndex = supersetIndex;
                int nextSupersetIndex;
                do
                {
                    nextSupersetIndex = superset.findNext(supersetIndex, serialize.get(serializeIndex++), FAST);
                    if (supersetIndex + 1 != nextSupersetIndex)
                        break;
                    supersetIndex++;
                }
                while (serializeIndex < serializeCount);

                size += TypeSizes.sizeofUnsignedVInt(supersetIndex - prevSupersetIndex);
                size += TypeSizes.sizeofUnsignedVInt(nextSupersetIndex - supersetIndex);
                supersetIndex = nextSupersetIndex;
            }
            return size;
        }
    }

}
