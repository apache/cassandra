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
package org.apache.cassandra.db;

import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.dht.*;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public interface PartitionPosition extends RingPosition<PartitionPosition>, ByteComparable
{
    public static enum Kind
    {
        // Only add new values to the end of the enum, the ordinal is used
        // during serialization
        ROW_KEY, MIN_BOUND, MAX_BOUND;

        private static final Kind[] allKinds = Kind.values();

        static Kind fromOrdinal(int ordinal)
        {
            return allKinds[ordinal];
        }
    }

    public static final class ForKey
    {
        public static PartitionPosition get(ByteBuffer key, IPartitioner p)
        {
            return key == null || key.remaining() == 0 ? p.getMinimumToken().minKeyBound() : p.decorateKey(key);
        }
    }

    public static final RowPositionSerializer serializer = new RowPositionSerializer();

    public Kind kind();
    public boolean isMinimum();

    /**
     * Produce a prefix-free byte-comparable representation of the key, i.e. such a sequence of bytes that any pair x, y
     * of valid positions (with the same key column types and partitioner),
     *   x.compareTo(y) == compareLexicographicallyUnsigned(x.asComparableBytes(), y.asComparableBytes())
     * and
     *   x.asComparableBytes() is not a prefix of y.asComparableBytes()
     *
     * We use a two-component tuple for decorated keys, and a one-component tuple for key bounds, where the terminator
     * byte is chosen to yield the correct comparison result. No decorated key can be a prefix of another (per the tuple
     * encoding), and no key bound can be a prefix of one because it uses a terminator byte that is different from the
     * tuple separator.
     */
    public abstract ByteSource asComparableBytes(Version version);

    /**
     * Produce a byte-comparable representation for the position before or after the key.
     * This does nothing for token boundaries (which are already at a position between valid keys), and changes
     * the terminator byte for keys.
     */
    public abstract ByteComparable asComparableBound(boolean before);

    public static class RowPositionSerializer implements IPartitionerDependentSerializer<PartitionPosition>
    {
        /*
         * We need to be able to serialize both Token.KeyBound and
         * DecoratedKey. To make this compact, we first write a byte whose
         * meaning is:
         *   - 0: DecoratedKey
         *   - 1: a 'minimum' Token.KeyBound
         *   - 2: a 'maximum' Token.KeyBound
         * In the case of the DecoratedKey, we then serialize the key (the
         * token is recreated on the other side). In the other cases, we then
         * serialize the token.
         */
        public void serialize(PartitionPosition pos, DataOutputPlus out, int version) throws IOException
        {
            Kind kind = pos.kind();
            out.writeByte(kind.ordinal());
            if (kind == Kind.ROW_KEY)
                ByteBufferUtil.writeWithShortLength(((DecoratedKey)pos).getKey(), out);
            else
                Token.serializer.serialize(pos.getToken(), out, version);
        }

        public PartitionPosition deserialize(DataInput in, IPartitioner p, int version) throws IOException
        {
            Kind kind = Kind.fromOrdinal(in.readByte());
            if (kind == Kind.ROW_KEY)
            {
                ByteBuffer k = ByteBufferUtil.readWithShortLength(in);
                return p.decorateKey(k);
            }
            else
            {
                Token t = Token.serializer.deserialize(in, p, version);
                return kind == Kind.MIN_BOUND ? t.minKeyBound() : t.maxKeyBound();
            }
        }

        public long serializedSize(PartitionPosition pos, int version)
        {
            Kind kind = pos.kind();
            int size = 1; // 1 byte for enum
            if (kind == Kind.ROW_KEY)
            {
                int keySize = ((DecoratedKey)pos).getKey().remaining();
                size += TypeSizes.sizeof((short) keySize) + keySize;
            }
            else
            {
                size += Token.serializer.serializedSize(pos.getToken(), version);
            }
            return size;
        }
    }
}
