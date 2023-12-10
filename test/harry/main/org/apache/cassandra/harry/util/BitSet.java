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

package org.apache.cassandra.harry.util;

import java.util.function.IntConsumer;

import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.Surjections;

public interface BitSet
{
    static long BITSET_STREAM_ID = 0x7670411362L;

    static long bitMask(int bits)
    {
        if (bits <= 0)
            return 0L;
        else if (bits < 64)
            return (1L << bits) - 1;
        else
            return -1L;
    }

    public static BitSet allSet(int bits)
    {
        return new BitSet64Bit(bitMask(bits), bits);
    }

    public static BitSet allUnset(int bits)
    {
        return new BitSet64Bit(0, bits);
    }

    public static BitSet create(long value, int bits)
    {
        return new BitSet64Bit(value, bits);
    }

    public void eachBit(BitConsumer iter);

    public void eachSetBit(IntConsumer iter);
    public void eachSetBit(IntConsumer iter, BitSet mask);

    public void eachUnsetBit(IntConsumer iter);

    public boolean allUnset();

    public boolean allUnset(BitSet mask);

    public boolean allSet();

    public void set(int idx);

    public void unset(int idx);

    public boolean isSet(int idx);
    public boolean isSet(int idx, BitSet mask);

    public static boolean isSet(long bits, int idx)
    {
        return (bits & (1L << idx)) != 0;
    }

    public static int setCount(long bits, int size)
    {
        int count = 0;
        for (int i = 0; i < size; i++)
        {
            if (BitSet.isSet(bits, i))
                count++;
        }
        return count;
    }

    public int setCount();
    public int setCount(BitSet mask);

    public int size();

    public BitSet clone(boolean invert);

    public class BitSet64Bit implements BitSet
    {
        private long bits;
        // TODO: use count to check out-of-bounds
        private int count;

        public BitSet64Bit(int count)
        {
            this(0, count);
        }

        public BitSet64Bit(long value, int count)
        {
            this.bits = value & bitMask(count);
            this.count = count;
        }

        public boolean allUnset()
        {
            return bits == 0;
        }

        public boolean allUnset(BitSet mask)
        {
            assert mask instanceof BitSet64Bit;
            return (((BitSet64Bit) mask).bits & bits) == 0;
        }

        public boolean allSet()
        {
            return bits == bitMask(count);
        }

        public void eachBit(BitConsumer iter)
        {
            for (int i = 0; i < count; i++)
                iter.consume(i, isSet(i));
        }

        public void eachSetBit(IntConsumer iter)
        {
            for (int i = 0; i < count; i++)
            {
                boolean isSet = isSet(i);
                if (isSet)
                    iter.accept(i);
            }
        }

        public void eachSetBit(IntConsumer iter, BitSet mask)
        {
            assert mask instanceof BitSet64Bit;
            long bits = (((BitSet64Bit) mask).bits & this.bits);
            for (int i = 0; i < count; i++)
            {
                boolean isSet = BitSet.isSet(bits, i);
                if (isSet)
                    iter.accept(i);
            }
        }

        public void eachUnsetBit(IntConsumer iter)
        {
            for (int i = 0; i < count; i++)
            {
                boolean isSet = isSet(i);
                if (!isSet)
                    iter.accept(i);
            }
        }

        public void set(int idx)
        {
            this.bits |= (1L << idx);
        }

        public void unset(int idx)
        {
            this.bits &= ~(1L << idx);
        }

        public boolean isSet(int idx)
        {
            assert idx < size() : String.format("Trying to query the bit (%s) outside the range of bitset (%s)", idx, size());
            return BitSet.isSet(bits, idx);
        }

        public boolean isSet(int idx, BitSet mask)
        {
            assert idx < size();
            assert mask instanceof BitSet64Bit;
            return BitSet.isSet(bits & ((BitSet64Bit) mask).bits, idx);
        }

        public int setCount()
        {
            return BitSet.setCount(bits, size());
        }

        public int setCount(BitSet mask)
        {
            assert mask instanceof BitSet64Bit;
            return BitSet.setCount(bits & ((BitSet64Bit) mask).bits, size());
        }

        public int size()
        {
            return count;
        }

        public BitSet clone(boolean invert)
        {
            return new BitSet64Bit(invert ? ~bits & bitMask(count) : bits,
                                   count);
        }

        public String toString()
        {
            String s = "";
            for (int i = 0; i < size(); i++)
            {
                s += isSet(i) ? "set" : "unset";
                if (i < (size() - 1))
                    s += ", ";
            }

            return String.format("[%s]", s);
        }
    }

    public interface BitConsumer
    {
        public void consume(int idx, boolean isSet);
    }

    public static Generator<BitSet> generator(int length)
    {
        return (rng) -> {
            return BitSet.create(rng.next(), length);
        };
    }

    public static Surjections.Surjection<BitSet> surjection(int length)
    {
        return generator(length).toSurjection(BITSET_STREAM_ID);
    }
}
