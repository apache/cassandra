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

import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.RoutingKeys;
import accord.primitives.Unseekables;
import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
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
    void serialize(K keys, T t, DataOutputPlus out, int version) throws IOException;

    /**
     * Deserialize into the specified DataInputStream instance.
     * @param in DataInput from which deserialization needs to happen.
     * @param version protocol version
     * @return the type that was deserialized
     * @throws IOException if deserialization fails
     */
    T deserialize(K keys, DataInputPlus in, int version) throws IOException;

    /**
     * Calculate serialized size of object without actually serializing.
     * @param t object to calculate serialized size
     * @param version protocol version
     * @return serialized size of object t
     */
    long serializedSize(K keys, T t, int version);

    final class NullableWithKeysSerializer<K extends Routables<?>, T> implements IVersionedWithKeysSerializer<K, T>
    {
        final IVersionedWithKeysSerializer<K, T> wrapped;
        public NullableWithKeysSerializer(IVersionedWithKeysSerializer<K, T> wrapped)
        {
            this.wrapped = wrapped;
        }

        @Override
        public void serialize(T t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t == null ? 0 : 1);
            if (t != null) wrapped.serialize(t, out, version);
        }

        @Override
        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            if (in.readByte() == 0) return null;
            return wrapped.deserialize(in, version);
        }

        @Override
        public long serializedSize(T t, int version)
        {
            return t == null ? 1 : 1 + wrapped.serializedSize(t, version);
        }

        @Override
        public void serialize(K keys, T t, DataOutputPlus out, int version) throws IOException
        {
            out.writeByte(t == null ? 0 : 1);
            if (t != null) wrapped.serialize(keys, t, out, version);
        }

        @Override
        public T deserialize(K keys, DataInputPlus in, int version) throws IOException
        {
            if (in.readByte() == 0) return null;
            return wrapped.deserialize(keys, in, version);
        }

        @Override
        public long serializedSize(K keys, T t, int version)
        {
            return t == null ? 1 : 1 + wrapped.serializedSize(keys, t, version);
        }
    }

    abstract class AbstractWithKeysSerializer
    {
        /**
         * If both ends have a pre-shared superset of the columns we are serializing, we can send them much
         * more efficiently. Both ends must provide the identically same set of columns.
         */
        protected void serializeSubset(Routables<?> serialize, Routables<?> superset, DataOutputPlus out) throws IOException
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
                    default: throw new AssertionError("Unhandled domain: " + serialize.domain());
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
                    default: throw new AssertionError("Unhandled domain: " + serialize.domain());
                    case Key:
                        serializeLargeSubset((AbstractUnseekableKeys)serialize, serializeCount, (AbstractUnseekableKeys)superset, supersetCount, out);
                        break;
                    case Range:
                        serializeLargeSubset((AbstractRanges)serialize, serializeCount, (AbstractRanges)superset, supersetCount, out);
                        break;
                }
            }
        }

        public long serializedSubsetSize(Routables<?> serialize, Routables<?> superset)
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
                    default: throw new AssertionError("Unhandled domain: " + serialize.domain());
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
                    default: throw new AssertionError("Unhandled domain: " + serialize.domain());
                    case Key:
                        return serializeLargeSubsetSize((AbstractUnseekableKeys)serialize, columnCount, (AbstractUnseekableKeys)superset, supersetCount);
                    case Range:
                        return serializeLargeSubsetSize((AbstractRanges)serialize, columnCount, (AbstractRanges)superset, supersetCount);
                }
            }
        }

        public Unseekables<?> deserializeSubset(Unseekables<?> superset, DataInputPlus in) throws IOException
        {
            long encoded = in.readUnsignedVInt();
            int supersetCount = superset.size();
            if (encoded == 0L)
            {
                return superset;
            }
            else if (supersetCount >= 64)
            {
                return deserializeLargeSubset(in, superset, supersetCount, (int) encoded);
            }
            else
            {
                encoded ^= -1L >>> (64 - supersetCount);
                int deserializeCount = Long.bitCount(encoded);
                switch (superset.domain())
                {
                    default: throw new AssertionError("Unhandled domain: " + superset.domain());
                    case Key:
                    {
                        AbstractUnseekableKeys keys = (AbstractUnseekableKeys) superset;
                        RoutingKey[] out = new RoutingKey[deserializeCount];
                        int count = 0;
                        while (encoded != 0)
                        {
                            long lowestBit = Long.lowestOneBit(encoded);
                            out[count++] = keys.get(Long.numberOfTrailingZeros(lowestBit));
                            encoded ^= lowestBit;
                        }
                        return RoutingKeys.ofSortedUnique(out);
                    }
                    case Range:
                    {
                        AbstractRanges ranges = (AbstractRanges)superset;
                        Range[] out = new Range[deserializeCount];
                        int count = 0;
                        while (encoded != 0)
                        {
                            long lowestBit = Long.lowestOneBit(encoded);
                            out[count++] = ranges.get(Long.numberOfTrailingZeros(lowestBit));
                            encoded ^= lowestBit;
                        }
                        return Ranges.ofSortedAndDeoverlapped(out);
                    }
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

        @DontInline
        private Unseekables<?> deserializeLargeSubset(DataInputPlus in, Unseekables<?> superset, int supersetCount, int delta) throws IOException
        {
            int deserializeCount = supersetCount - delta;
            switch (superset.domain())
            {
                default: throw new AssertionError("Unhandled domain: " + superset.domain());
                case Key:
                {
                    RoutingKeys keys = (RoutingKeys) superset;
                    RoutingKey[] out = new RoutingKey[deserializeCount];
                    int supersetIndex = 0;
                    int count = 0;
                    while (count < deserializeCount)
                    {
                        int takeCount = in.readUnsignedVInt32();
                        while (takeCount-- > 0) out[count++] = keys.get(supersetIndex++);
                        supersetIndex += in.readUnsignedVInt32();
                    }
                    return RoutingKeys.ofSortedUnique(out);
                }
                case Range:
                {
                    AbstractRanges ranges = (AbstractRanges)superset;
                    Range[] out = new Range[deserializeCount];
                    int supersetIndex = 0;
                    int count = 0;
                    while (count < deserializeCount)
                    {
                        int takeCount = in.readUnsignedVInt32();
                        while (takeCount-- > 0) out[count++] = ranges.get(supersetIndex++);
                        supersetIndex += in.readUnsignedVInt32();
                    }
                    return Ranges.ofSortedAndDeoverlapped(out);
                }
            }
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
