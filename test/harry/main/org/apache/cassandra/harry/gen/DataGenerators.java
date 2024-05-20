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

package org.apache.cassandra.harry.gen;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.rng.RngUtils;

public class DataGenerators
{
    public static final Object UNSET_VALUE = new Object() {
        public String toString()
        {
            return "UNSET";
        }
    };

    // There is still a slim chance that we're going to produce either of these values by chance, but we'll catch this
    // during value generation
    public static long UNSET_DESCR = Long.MAX_VALUE;
    public static long NIL_DESCR = Long.MIN_VALUE;
    // Empty value, for the types that support it
    public static long EMPTY_VALUE = Long.MIN_VALUE + 1;

    public static Object[] inflateData(List<ColumnSpec<?>> columns, long[] descriptors)
    {
        // This can be not true depending on how we implement subselections
        assert columns.size() == descriptors.length;
        Object[] data = new Object[descriptors.length];
        for (int i = 0; i < descriptors.length; i++)
        {
            ColumnSpec columnSpec = columns.get(i);
            if (descriptors[i] == UNSET_DESCR)
                data[i] = UNSET_VALUE;
            else if (descriptors[i] == NIL_DESCR)
                data[i] = null;
            else
                data[i] = columnSpec.inflate(descriptors[i]);
        }
        return data;
    }

    public static long[] deflateData(List<ColumnSpec<?>> columns, Object[] data)
    {
        // This can be not true depending on how we implement subselections
        assert columns.size() == data.length;
        long[] descriptors = new long[data.length];
        for (int i = 0; i < descriptors.length; i++)
        {
            ColumnSpec columnSpec = columns.get(i);
            if (data[i] == null)
                descriptors[i] = NIL_DESCR;
            else if (data[i] == UNSET_VALUE)
                descriptors[i] = UNSET_DESCR;
            else
                descriptors[i] = columnSpec.deflate(data[i]);
        }
        return descriptors;
    }

    public static int[] requiredBytes(List<ColumnSpec<?>> columns)
    {
        switch (columns.size())
        {
            case 0:
                throw new RuntimeException("Can't inflate empty data column set as it is not inversible");
            case 1:
                return new int[]{ Math.min(columns.get(0).type.maxSize(), Long.SIZE) };
            default:
                class Pair
                {
                    final int idx, maxSize;

                    Pair(int idx, int maxSize)
                    {
                        this.idx = idx;
                        this.maxSize = maxSize;
                    }
                }
                int[] bytes = new int[Math.min(KeyGenerator.MAX_UNIQUE_PREFIX_COLUMNS, columns.size())];
                Pair[] sorted = new Pair[bytes.length];
                for (int i = 0; i < sorted.length; i++)
                    sorted[i] = new Pair(i, columns.get(i).type.maxSize());

                int remainingBytes = Long.BYTES;
                int slotSize = remainingBytes / bytes.length;
                // first pass: give it at most a slot number of bytes
                for (int i = 0; i < sorted.length; i++)
                {
                    int size = sorted[i].maxSize;
                    int allotedSize = Math.min(size, slotSize);
                    remainingBytes -= allotedSize;
                    bytes[sorted[i].idx] = allotedSize;
                }

                // sliced evenly
                if (remainingBytes == 0)
                    return bytes;

                // second pass: try to occupy remaining bytes
                // it is possible to improve the second pass and separate additional bytes evenly, but it is
                // questionable how much it'll bring since it does not change the total amount of entropy.
                for (int i = 0; i < sorted.length; i++)
                {
                    if (remainingBytes == 0)
                        break;
                    Pair p = sorted[i];
                    if (bytes[p.idx] < p.maxSize)
                    {
                        int allotedSize = Math.min(p.maxSize - bytes[p.idx], remainingBytes);
                        remainingBytes -= allotedSize;
                        bytes[p.idx] += allotedSize;
                    }
                }

                return bytes;
        }
    }

    public static Object[] inflateKey(List<ColumnSpec<?>> columns, long descriptor, long[] slices)
    {
        assert columns.size() >= slices.length : String.format("Columns: %s. Slices: %s", columns, Arrays.toString(slices));
        assert columns.size() > 0 : "Can't deflate from empty columnset";

        Object[] res = new Object[columns.size()];
        for (int i = 0; i < slices.length; i++)
        {
            ColumnSpec spec = columns.get(i);
            res[i] = spec.inflate(slices[i]);
        }

        // The rest can be random, since prefix is always fixed
        long current = descriptor;
        for (int i = slices.length; i < columns.size(); i++)
        {
            current = RngUtils.next(current);
            res[i] = columns.get(i).inflate(current);
        }

        return res;
    }

    public static long[] deflateKey(List<ColumnSpec<?>> columns, Object[] values)
    {
        assert columns.size() == values.length : String.format("%s != %s", columns.size(), values.length);
        assert columns.size() > 0 : "Can't deflate from empty columnset";

        int fixedPart = Math.min(KeyGenerator.MAX_UNIQUE_PREFIX_COLUMNS, columns.size());

        long[] slices = new long[fixedPart];
        boolean allNulls = true;
        for (int i = 0; i < fixedPart; i++)
        {
            ColumnSpec spec = columns.get(i);
            Object value = values[i];
            if (value != null)
                allNulls = false;

            slices[i] = value == null ? NIL_DESCR : spec.deflate(value);
        }

        if (allNulls)
            return null;

        return slices;
    }

    public static KeyGenerator createKeyGenerator(List<ColumnSpec<?>> columns)
    {
        switch (columns.size())
        {
            case 0:
                return EMPTY_KEY_GEN;
            case 1:
                return new SinglePartKeyGenerator(columns);
            default:
                return new MultiPartKeyGenerator(columns);
        }
    }

    private static final KeyGenerator EMPTY_KEY_GEN = new KeyGenerator(Collections.emptyList())
    {
        private final long[] EMPTY_SLICED = new long[0];
        private final Object[] EMPTY_INFLATED = new Object[0];

        public long[] slice(long descriptor)
        {
            return EMPTY_SLICED;
        }

        public long stitch(long[] parts)
        {
            return 0;
        }

        public long minValue(int idx)
        {
            return 0;
        }

        public long maxValue(int idx)
        {
            return 0;
        }

        @Override
        public Object[] inflate(long descriptor)
        {
            return EMPTY_INFLATED;
        }

        @Override
        public long deflate(Object[] value)
        {
            return 0;
        }

        public long adjustEntropyDomain(long descriptor)
        {
            return 0;
        }

        public int byteSize()
        {
            return 0;
        }

        public int compare(long l, long r)
        {
            return 0;
        }
    };

    public static abstract class KeyGenerator implements Bijections.Bijection<Object[]>
    {
        // Maximum number of columns that uniquely identify the value (i.e. use entropy bits).
        // Subsequent columns will have random data in them.
        public static final int MAX_UNIQUE_PREFIX_COLUMNS = 4;
        @VisibleForTesting
        public final List<ColumnSpec<?>> columns;

        protected KeyGenerator(List<ColumnSpec<?>> columns)
        {
            this.columns = columns;
        }

        public abstract long[] slice(long descriptor);

        public abstract long stitch(long[] parts);

        public long minValue()
        {
            return Bijections.minForSize(byteSize());
        }

        public long maxValue()
        {
            return Bijections.maxForSize(byteSize());
        }

        /**
         * Min value for a segment: 0, possibly with an inverted 0 sign for stitching.
         */
        public abstract long minValue(int idx);
        public abstract long maxValue(int idx);
    }

    public static class SinglePartKeyGenerator extends KeyGenerator
    {
        private final Bijections.Bijection keyGen;
        private final int totalSize;

        public SinglePartKeyGenerator(List<ColumnSpec<?>> columns)
        {
            super(columns);
            assert columns.size() == 1;
            this.keyGen = columns.get(0).generator();
            this.totalSize = keyGen.byteSize();
        }

        public long[] slice(long descriptor)
        {
            if (shouldInvertSign())
                descriptor ^= Bytes.signMaskFor(byteSize());

            descriptor = adjustEntropyDomain(descriptor);
            return new long[]{ descriptor };
        }

        public long stitch(long[] parts)
        {
            long descriptor = parts[0];

            if (shouldInvertSign())
                descriptor ^= Bytes.signMaskFor(byteSize());

            return adjustEntropyDomain(descriptor);
        }

        public long minValue(int idx)
        {
            assert idx == 0;
            return keyGen.minValue();
        }

        public long maxValue(int idx)
        {
            assert idx == 0;
            return keyGen.maxValue();
        }

        public Object[] inflate(long descriptor)
        {
            long[] sliced = slice(descriptor);
            return new Object[]{ keyGen.inflate(sliced[0]) };
        }

        public boolean shouldInvertSign()
        {
            return totalSize != Long.BYTES && !keyGen.unsigned();
        }

        public long deflate(Object[] value)
        {
            Object v = value[0];
            if (v == null)
                return NIL_DESCR;
            long descriptor = keyGen.deflate(v);
            return stitch(new long[] { descriptor });
        }

        public int byteSize()
        {
            return totalSize;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }
    }

    public static class MultiPartKeyGenerator extends KeyGenerator
    {
        @VisibleForTesting
        public final int[] sizes;
        protected final int totalSize;

        public MultiPartKeyGenerator(List<ColumnSpec<?>> columns)
        {
            super(columns);
            assert columns.size() > 1 : "It makes sense to use a multipart generator if you have more than one column, but you have " + columns.size();

            this.sizes = requiredBytes(columns);
            int total = 0;
            for (int size : sizes)
                total += size;

            this.totalSize = total;
        }

        public long deflate(Object[] values)
        {
            long[] stiched = DataGenerators.deflateKey(columns, values);
            if (stiched == null)
                return NIL_DESCR;
            return stitch(stiched);
        }

        public Object[] inflate(long descriptor)
        {
            return DataGenerators.inflateKey(columns, descriptor, slice(descriptor));
        }

        // Checks whether we need to invert a slice sign to preserve order of the sliced descriptor
        public boolean shouldInvertSign(int idx)
        {
            Bijections.Bijection<?> gen = columns.get(idx).generator();

            int maxSliceSize = gen.byteSize();
            int actualSliceSize = sizes[idx];

            if (idx == 0)
            {
                // We consume a sign of a descriptor (long, long), (int, int), etc.
                if (totalSize == Long.BYTES)
                {
                    // If we use only 3 bytes for a 4-byte int, or 4 bytes for a 8-byte int,
                    // they're effectively unsigned/byte-ordered, so their order won't match
                    if (maxSliceSize > actualSliceSize)
                        return true;
                    // Sign of the current descriptor should match the sign of the slice.
                    // For example, (tinyint, double) or (double, tinyint). In the first case (tinyint first),
                    // sign of the first component is going to match the sign of the descriptor.
                    // In the second case (double first), double is 7-bit, but its most significant bit
                    // does not hold a sign, so we have to invert it to match sign of the descriptor.
                    else
                        return gen.unsigned();
                }
                // We do not consume a sign of a descriptor (float, tinyint), (int, tinyint), etc,
                // so we have to only invert signs of the values, since their order doesn't match.
                else
                {
                    assert maxSliceSize == actualSliceSize;
                    return !gen.unsigned();
                }
            }
            else if (gen.unsigned())
                return false;
            else
                // We invert sign of all subsequent chunks if they have enough entropy to have a sign bit set
                return maxSliceSize == actualSliceSize;
        }

        public long[] slice(long descriptor)
        {
            long[] pieces = new long[columns.size()];
            long pos = totalSize;
            for (int i = 0; i < sizes.length; i++)
            {
                final int size = sizes[i];
                long piece = descriptor >> ((pos - size) * Byte.SIZE);

                if (shouldInvertSign(i))
                    piece ^= Bytes.signMaskFor(size);

                piece &= Bytes.bytePatternFor(size);

                pieces[i] = piece;
                pos -= size;
            }

            // The rest can be random, since prefix is always fixed
            long current = descriptor;
            for (int i = sizes.length; i < columns.size(); i++)
            {
                current = RngUtils.next(current);
                pieces[i] = columns.get(i).generator().adjustEntropyDomain(current);
            }

            return pieces;
        }

        public long stitch(long[] parts)
        {
            long stitched = 0;
            int consumed = 0;
            for (int i = sizes.length - 1; i >= 0; i--)
            {
                int size = sizes[i];
                long piece = parts[i];

                if (shouldInvertSign(i))
                    piece ^= Bytes.signMaskFor(size);

                piece &= Bytes.bytePatternFor(size);
                stitched |= piece << (consumed * Byte.SIZE);
                consumed += size;
            }
            return stitched;
        }

        public long minValue(int idx)
        {
            long res = columns.get(idx).generator().minValue();
            // Inverting sign is important for range queries and RTs, since we're
            // making boundaries that'll be stitched later.
            if (shouldInvertSign(idx))
                res ^= Bytes.signMaskFor(sizes[idx]);
            return res;
        }

        public long maxValue(int idx)
        {
            long res = columns.get(idx).generator().maxValue();
            if (shouldInvertSign(idx))
                res ^= Bytes.signMaskFor(sizes[idx]);
            return res;
        }

        public int byteSize()
        {
            return totalSize;
        }

        public int compare(long l, long r)
        {
            return Long.compare(l, r);
        }
    }
}