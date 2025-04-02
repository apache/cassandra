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

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Keys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.utils.UnhandledEnum;
import net.nicoulaj.compilecommand.annotations.DontInline;
import net.nicoulaj.compilecommand.annotations.Inline;
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
                switch (serialize.domainKind())
                {
                    default: throw UnhandledEnum.unknown(serialize.domainKind());
                    case SeekableKey:
                        out.writeUnsignedVInt(encodeBitmap((Keys)serialize, (Keys)superset, supersetCount));
                        break;
                    case UnseekableKey:
                        out.writeUnsignedVInt(encodeBitmap((AbstractUnseekableKeys)serialize, (AbstractUnseekableKeys)superset, supersetCount));
                        break;
                    case Range:
                        out.writeUnsignedVInt(encodeBitmap((AbstractRanges)serialize, (AbstractRanges)superset, supersetCount));
                        break;
                }
            }
            else
            {
                switch (serialize.domainKind())
                {
                    default: throw UnhandledEnum.unknown(serialize.domainKind());
                    case SeekableKey:
                        serializeLargeSubset((Keys)serialize, serializeCount, (Keys)superset, supersetCount, out);
                        break;
                    case UnseekableKey:
                        serializeLargeSubset((AbstractUnseekableKeys)serialize, serializeCount, (AbstractUnseekableKeys)superset, supersetCount, out);
                        break;
                    case Range:
                        serializeLargeSubset((AbstractRanges)serialize, serializeCount, (AbstractRanges)superset, supersetCount, out);
                        break;
                }
            }
        }

        protected long serializedSubsetSizeInternal(Routables<?> serialize, Routables<?> superset)
        {
            int columnCount = serialize.size();
            int supersetCount = superset.size();
            if (columnCount == supersetCount)
            {
                return TypeSizes.sizeofUnsignedVInt(0);
            }
            else if (supersetCount < 64)
            {
                switch (serialize.domainKind())
                {
                    default: throw UnhandledEnum.unknown(serialize.domainKind());
                    case SeekableKey:
                        return TypeSizes.sizeofUnsignedVInt(encodeBitmap((Keys)serialize, (Keys)superset, supersetCount));
                    case UnseekableKey:
                        return TypeSizes.sizeofUnsignedVInt(encodeBitmap((AbstractUnseekableKeys)serialize, (AbstractUnseekableKeys)superset, supersetCount));
                    case Range:
                        return TypeSizes.sizeofUnsignedVInt(encodeBitmap((AbstractRanges)serialize, (AbstractRanges)superset, supersetCount));
                }
            }
            else
            {
                switch (serialize.domainKind())
                {
                    default: throw UnhandledEnum.unknown(serialize.domainKind());
                    case SeekableKey:
                        return serializeLargeSubsetSize((Keys)serialize, columnCount, (Keys)superset, supersetCount);
                    case UnseekableKey:
                        return serializeLargeSubsetSize((AbstractUnseekableKeys)serialize, columnCount, (AbstractUnseekableKeys)superset, supersetCount);
                    case Range:
                        return serializeLargeSubsetSize((AbstractRanges)serialize, columnCount, (AbstractRanges)superset, supersetCount);
                }
            }
        }

        @DontInline
        private <K extends Routable, R extends Routables<K>> long serializeLargeSubsetSize(R serialize, int serializeCount, R superset, int supersetCount)
        {
            long size = TypeSizes.sizeofUnsignedVInt(supersetCount - serializeCount);
            if (serializeCount == 0) return size;
            int prevSupersetIndex = 0;
            int supersetIndex = 0;
            int take = 0;
            for (int i = 0; i < serializeCount; i++)
            {
                int offset = supersetIndex + take;
                int nextIndex = superset.findNext(offset, serialize.get(i), FAST);
                if (nextIndex == offset)
                {
                    take++;
                    continue;
                }
                if (take != 0) // since this is dealing with subsets, the only time take=0 is when i=0 and the first superset offset isn't included
                {
                    size += TypeSizes.sizeofUnsignedVInt(take);
                    size += TypeSizes.sizeofUnsignedVInt(supersetIndex - prevSupersetIndex);
                    prevSupersetIndex = supersetIndex;
                }

                supersetIndex = nextIndex;
                take = 1;
            }
            size += TypeSizes.sizeofUnsignedVInt(take);
            size += TypeSizes.sizeofUnsignedVInt(supersetIndex - prevSupersetIndex);
            return size;
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
        private <K extends Routable, R extends Routables<K>> void serializeLargeSubset(R serialize, int serializeCount,
                                                                                       R superset, int supersetCount,
                                                                                       DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(supersetCount - serializeCount);
            if (serializeCount == 0) return;
            int prevSupersetIndex = 0;
            int supersetIndex = 0;
            int take = 0;
            for (int i = 0; i < serializeCount; i++)
            {
                int offset = supersetIndex + take;
                int nextIndex = superset.findNext(offset, serialize.get(i), FAST);
                if (nextIndex == offset)
                {
                    take++;
                    continue;
                }
                if (take != 0)
                {
                    out.writeUnsignedVInt32(take);
                    out.writeUnsignedVInt32(supersetIndex - prevSupersetIndex);
                    prevSupersetIndex = supersetIndex;
                }

                supersetIndex = nextIndex;
                take = 1;
            }
            out.writeUnsignedVInt32(take);
            out.writeUnsignedVInt32(supersetIndex - prevSupersetIndex);
        }

        public Routables<?> deserializeSubsetInternal(Routables<?> superset, DataInputPlus in) throws IOException
        {
            switch (superset.domainKind())
            {
                default: throw UnhandledEnum.unknown(superset.domainKind());
                case SeekableKey: return deserializeSubset((Keys) superset, in, (ks, s) -> ks == null ? s : Keys.of(ks), Key[]::new);
                case UnseekableKey: return deserializeSubset((AbstractUnseekableKeys) superset, in, (ks, s) -> ks == null ? s : RoutingKeys.of(ks), RoutingKey[]::new);
                case Range: return deserializeSubset((AbstractRanges) superset, in, (rs, s) -> rs == null ? s : Ranges.of(rs), Range[]::new);
            }
        }

        public <K extends Routable, R extends Routables<K>, T> T deserializeSubset(R superset, DataInputPlus in, BiFunction<K[], R, T> result, IntFunction<K[]> allocator) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            int supersetCount = superset.size();
            if (encoded == 0L)
                return result.apply(null, superset);
            else if (supersetCount >= 64)
                return result.apply(deserializeLargeSubset(in, superset, supersetCount, (int) encoded, allocator), superset);
            else
                return result.apply(deserializeSmallSubsetArray(encoded, superset, supersetCount, allocator), superset);
        }

        @Inline
        private <T extends Routable> T[] deserializeLargeSubset(DataInputPlus in, Routables<T> superset, int supersetCount, int delta, IntFunction<T[]> allocator) throws IOException
        {
            int deserializeCount = supersetCount - delta;
            T[] out = allocator.apply(deserializeCount);
            int count = 0;
            int prevSupersetIndex = 0;
            while (count < deserializeCount)
            {
                int take = in.readUnsignedVInt32();
                int supersetIndex = in.readUnsignedVInt32() + prevSupersetIndex;
                prevSupersetIndex = supersetIndex;
                for (int i = 0; i < take; i++)
                    out[count++] = superset.get(supersetIndex + i);
            }
            return out;
        }

        private <K extends Routable> K[] deserializeSmallSubsetArray(long encoded, Routables<K> superset, int supersetCount, IntFunction<K[]> allocator)
        {
            encoded ^= -1L >>> (64 - supersetCount);
            int deserializeCount = Long.bitCount(encoded);
            K[] out = allocator.apply(deserializeCount);
            int count = 0;
            while (encoded != 0)
            {
                long lowestBit = Long.lowestOneBit(encoded);
                out[count++] = superset.get(Long.numberOfTrailingZeros(lowestBit));
                encoded ^= lowestBit;
            }
            return out;
        }

        public void skipSubsetInternal(int supersetCount, DataInputPlus in) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            if (encoded == 0 || supersetCount < 64) return;
            // large
            int deserializeCount = supersetCount - ((int) encoded);
            int count = 0;
            while (count < deserializeCount)
            {
                int take = in.readUnsignedVInt32();
                in.readUnsignedVInt32();
                for (int i = 0; i < take; i++)
                    count++;
            }
        }
    }

}
