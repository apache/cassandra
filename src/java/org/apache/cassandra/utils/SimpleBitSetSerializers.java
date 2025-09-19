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

package org.apache.cassandra.utils;

import java.io.IOException;

import accord.utils.Invariants;
import accord.utils.LargeBitSet;
import accord.utils.SimpleBitSet;
import accord.utils.SmallBitSet;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class SimpleBitSetSerializers
{
    public static final AnySerializer any = new AnySerializer();
    public static final SmallSerializer small = new SmallSerializer();
    public static final SmallWithCapacitySerializer smallWithCapacity = new SmallWithCapacitySerializer();
    public static final LargeSerializer large = new LargeSerializer();

    public static class SmallSerializer implements UnversionedSerializer<SmallBitSet>
    {
        @Override
        public void serialize(SmallBitSet t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt(t.bits());
        }

        @Override
        public SmallBitSet deserialize(DataInputPlus in) throws IOException
        {
            return new SmallBitSet(in.readUnsignedVInt());
        }

        @Override
        public long serializedSize(SmallBitSet t)
        {
            return TypeSizes.sizeofUnsignedVInt(t.bits());
        }
    }

    public static class SmallWithCapacitySerializer extends SmallSerializer
    {
        @Override
        public void serialize(SmallBitSet t, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(1);
            super.serialize(t, out);
        }

        @Override
        public SmallBitSet deserialize(DataInputPlus in) throws IOException
        {
            Invariants.require(in.readUnsignedVInt32() <= 1);
            return super.deserialize(in);
        }

        @Override
        public long serializedSize(SmallBitSet t)
        {
            return TypeSizes.sizeofUnsignedVInt(1) + super.serializedSize(t);
        }
    }

    public static class LargeSerializer implements UnversionedSerializer<LargeBitSet>
    {
        @Override
        public void serialize(LargeBitSet t, DataOutputPlus out) throws IOException
        {
            long[] raw = LargeBitSet.SerializationSupport.getArray(t);
            out.writeUnsignedVInt32(raw.length);
            if (raw.length <= 1)
            {
                out.writeUnsignedVInt(raw.length == 0 ? 0L : raw[0]);
            }
            else
            {
                // find the first word written
                int wordsInUse = wordsInUse(raw);
                out.writeUnsignedVInt32(wordsInUse);
                for (int i = 0; i < wordsInUse; i++)
                    out.writeUnsignedVInt(raw[i]);
            }
        }

        @Override
        public LargeBitSet deserialize(DataInputPlus in) throws IOException
        {
            int capacity = in.readUnsignedVInt32();
            return deserializeWithCapacity(capacity, in);
        }

        LargeBitSet deserializeWithCapacity(int capacityInLongs, DataInputPlus in) throws IOException
        {
            if (capacityInLongs <= 1)
            {
                long v = in.readUnsignedVInt();
                Invariants.require(capacityInLongs == 1 || v == 0);
                long[] raw = capacityInLongs == 0 ? new long[0] : new long[] { v };
                return LargeBitSet.SerializationSupport.construct(raw);
            }

            long[] raw = new long[capacityInLongs];
            int wordsInUse = in.readUnsignedVInt32();
            for (int i = 0; i < wordsInUse; i++)
                raw[i] = in.readUnsignedVInt();
            return LargeBitSet.SerializationSupport.construct(raw);
        }

        @Override
        public long serializedSize(LargeBitSet t)
        {
            long[] raw = LargeBitSet.SerializationSupport.getArray(t);
            return TypeSizes.sizeofUnsignedVInt(raw.length)
                   + serializedSizeWithoutCapacity(raw);
        }

        private long serializedSizeWithoutCapacity(long[] raw)
        {
            if (raw.length <= 1)
                return TypeSizes.sizeofUnsignedVInt(raw.length == 0 ? 0L : raw[0]);

            // find the last word written
            int wordsInUse = wordsInUse(raw);
            long size = TypeSizes.sizeofUnsignedVInt(wordsInUse);
            for (int i = 0; i < wordsInUse; i++)
                size += TypeSizes.sizeofUnsignedVInt(raw[i]);
            return size;
        }

        private static int wordsInUse(long[] raw)
        {
            int wordsInUse = raw.length;
            for (int i = raw.length - 1; i >= 0; i--)
            {
                if (raw[i] != 0)
                    return wordsInUse;
                wordsInUse--;
            }
            return wordsInUse;
        }
    }

    private static <S extends SimpleBitSet> UnversionedSerializer<S> serializer(SimpleBitSet set)
    {
        if (set.getClass() == LargeBitSet.class) return (UnversionedSerializer<S>) large;
        if (set.getClass() == SmallBitSet.class) return (UnversionedSerializer<S>) smallWithCapacity;
        throw new UnsupportedOperationException("Unknown bitset type: " + set.getClass());
    }

    public static class AnySerializer implements UnversionedSerializer<SimpleBitSet>
    {
        @Override
        public void serialize(SimpleBitSet bitset, DataOutputPlus out) throws IOException
        {
            serializer(bitset).serialize(bitset, out);
        }

        @Override
        public SimpleBitSet deserialize(DataInputPlus in) throws IOException
        {
            int capacity = in.readUnsignedVInt32();
            // can use small or large deserializer for capacity <= 1, but to ensure capacity() is the same we use small serializer only when capacity == 1
            if (capacity == 1) return small.deserialize(in);
            else return large.deserializeWithCapacity(capacity, in);
        }

        @Override
        public long serializedSize(SimpleBitSet bitset)
        {
            return serializer(bitset).serializedSize(bitset);
        }
    }

}
