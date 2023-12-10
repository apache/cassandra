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

package org.apache.cassandra.fuzz.harry.gen;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.harry.ddl.ColumnSpec;
import org.apache.cassandra.harry.gen.Bijections;
import org.apache.cassandra.harry.gen.Bytes;
import org.apache.cassandra.harry.gen.DataGenerators;
import org.apache.cassandra.harry.gen.EntropySource;
import org.apache.cassandra.harry.gen.Generator;
import org.apache.cassandra.harry.gen.StringBijection;

public class DataGeneratorsTest
{
    private static final int RUNS = 100;
    private static final EntropySource rand = EntropySource.forTests(1);

    @Test
    public void testSingleTypeRoundTrip()
    {
        for (ColumnSpec.DataType dt : new ColumnSpec.DataType[]{ColumnSpec.int8Type,
                                                                ColumnSpec.int16Type,
                                                                ColumnSpec.int32Type,
                                                                ColumnSpec.int64Type,
                                                                ColumnSpec.asciiType,
                                                                ColumnSpec.floatType,
                                                                ColumnSpec.doubleType})
        {
            for (int i = 0; i < RUNS; i++)
            {
                DataGenerators.SinglePartKeyGenerator gen = new DataGenerators.SinglePartKeyGenerator(Collections.singletonList(ColumnSpec.ck("ck0", dt, false)));
                long descriptor = rand.next();
                descriptor = gen.adjustEntropyDomain(descriptor);
                Assert.assertEquals(descriptor,
                                    gen.deflate(gen.inflate(descriptor)));
            }
        }
    }

    @Test
    public void testRequiredBytes()
    {
        testRequiredBytes(sizes(4, 1),
                          ColumnSpec.int32Type, ColumnSpec.int8Type);
        testRequiredBytes(sizes(7, 1),
                          ColumnSpec.int64Type, ColumnSpec.int8Type);
        testRequiredBytes(sizes(4, 4),
                          ColumnSpec.int32Type, ColumnSpec.int64Type);
        testRequiredBytes(sizes(4, 4),
                          ColumnSpec.int32Type, ColumnSpec.int32Type);
        testRequiredBytes(sizes(4, 3),
                          ColumnSpec.int32Type, ColumnSpec.floatType);
        testRequiredBytes(sizes(4, 4),
                          ColumnSpec.int64Type, ColumnSpec.int64Type);
        testRequiredBytes(sizes(4, 2, 2),
                          ColumnSpec.int64Type, ColumnSpec.int16Type, ColumnSpec.int32Type);
        testRequiredBytes(sizes(6, 1, 1),
                          ColumnSpec.int64Type, ColumnSpec.int8Type, ColumnSpec.int8Type);
        testRequiredBytes(sizes(4, 2, 2),
                          ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int64Type);
        testRequiredBytes(sizes(4, 2, 2),
                          ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int64Type);
        testRequiredBytes(sizes(1, 5, 2),
                          ColumnSpec.int8Type, ColumnSpec.asciiType, ColumnSpec.int64Type);
        testRequiredBytes(sizes(1, 1, 6),
                          ColumnSpec.int8Type, ColumnSpec.int8Type, ColumnSpec.int64Type);
        testRequiredBytes(sizes(2, 2, 2, 2),
                          ColumnSpec.int64Type, ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int32Type);
        testRequiredBytes(sizes(1, 3, 2, 2),
                          ColumnSpec.int8Type, ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int32Type);
        testRequiredBytes(sizes(1, 3, 2, 2),
                          ColumnSpec.int8Type, ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int32Type);
        testRequiredBytes(sizes(1, 3, 2, 2),
                          ColumnSpec.int8Type, ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int32Type, ColumnSpec.int64Type);
    }

    private static void testRequiredBytes(int[] sizes,
                                         ColumnSpec.DataType<?>... types)
    {
        int sum = 0;
        for (int size : sizes)
            sum += size;
        Assert.assertTrue(sum > 0);
        Assert.assertTrue(sum <= 8);

        List<ColumnSpec<?>> columns = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++)
            columns.add(ColumnSpec.ck("r" + i, types[i], false));
        Assert.assertArrayEquals(columns.toString(),
                                 sizes,
                                 DataGenerators.requiredBytes(columns));
    }

    @Test
    public void testSliceStitch()
    {
        for (int i = 2; i < 5; i++)
        {
            Iterator<ColumnSpec.DataType[]> iter = permutations(i,
                                                                ColumnSpec.DataType.class,
                                                                ColumnSpec.int8Type,
                                                                ColumnSpec.asciiType,
                                                                ColumnSpec.int16Type,
                                                                ColumnSpec.int32Type,
                                                                ColumnSpec.int32Type,
                                                                ColumnSpec.floatType,
                                                                ColumnSpec.doubleType);
            while (iter.hasNext())
            {
                testSliceStitch(iter.next());
            }
        }
    }

    private static void testSliceStitch(ColumnSpec.DataType... types)
    {
        List<ColumnSpec<?>> spec = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++)
            spec.add(ColumnSpec.ck("r" + i, types[i], false));
        DataGenerators.MultiPartKeyGenerator gen = new DataGenerators.MultiPartKeyGenerator(spec);

        for (int i = 0; i < RUNS; i++)
        {
            long orig = gen.adjustEntropyDomain(rand.next());
            long[] sliced = gen.slice(orig);
            long stitched = gen.stitch(sliced);
            Assert.assertEquals(String.format("Orig: %s. Stitched: %s",
                                              Long.toHexString(orig),
                                              Long.toHexString(stitched)),
                                orig, stitched);
        }
    }

    @Test
    public void testKeyGenerators()
    {
        for (int i = 1; i < 5; i++)
        {
            for (boolean asReversed : new boolean[]{ false, true })
            {
                Iterator<ColumnSpec.DataType[]> iter = permutations(i,
                                                                    ColumnSpec.DataType.class,
                                                                    ColumnSpec.int8Type,
                                                                    ColumnSpec.asciiType,
                                                                    ColumnSpec.int16Type,
                                                                    ColumnSpec.int32Type,
                                                                    ColumnSpec.int64Type,
                                                                    ColumnSpec.floatType,
                                                                    ColumnSpec.doubleType
                );

                while (iter.hasNext())
                {
                    ColumnSpec.DataType[] types = iter.next();
                    try
                    {
                        testKeyGenerators(asReversed, types);
                    }
                    catch (Throwable t)
                    {
                        throw new AssertionError("Caught error for the type combination " + Arrays.toString(types), t);
                    }
                }
            }
        }
    }

    @Ignore
    @Test // this one is mostly useful when the above test fails and you need a quicker turnaround
    public void testSomeKeyGenerators()
    {
        Iterator<ColumnSpec.DataType[]> iter = Collections.singletonList(new ColumnSpec.DataType[]{ ColumnSpec.int32Type, ColumnSpec.int32Type }).iterator();

        while (iter.hasNext())
        {
            ColumnSpec.DataType[] types = iter.next();
            try
            {
                testKeyGenerators(true, types);
                testKeyGenerators(false, types);
            }
            catch (Throwable t)
            {
                throw new AssertionError("Caught error for the type combination " + Arrays.toString(types), t);
            }
        }
    }

    static void testKeyGenerators(boolean reversed, ColumnSpec.DataType<?>... types)
    {
        List<ColumnSpec<?>> spec = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++)
            spec.add(ColumnSpec.ck("r" + i, types[i], reversed));

        DataGenerators.KeyGenerator keyGenerator = DataGenerators.createKeyGenerator(spec);

        for (int i = 0; i < RUNS; i++)
        {
            testKeyGenerators(rand.next(), rand.next(), keyGenerator);
            // test some edge cases
            testKeyGenerators(0, 0, keyGenerator);
            testKeyGenerators(0xffffffffffffffffL, 0xffffffffffffffffL, keyGenerator);
            testKeyGenerators(keyGenerator.minValue(), keyGenerator.maxValue(), keyGenerator);
            testKeyGenerators(0, keyGenerator.minValue(), keyGenerator);
            testKeyGenerators(0, keyGenerator.maxValue(), keyGenerator);
            long descriptor = rand.next();
            testKeyGenerators(descriptor, 0, keyGenerator);
            testKeyGenerators(descriptor, keyGenerator.minValue(), keyGenerator);
            testKeyGenerators(descriptor, keyGenerator.maxValue(), keyGenerator);
            testKeyGenerators(descriptor, 0xffffffffffffffffL, keyGenerator);
            testKeyGenerators(descriptor, descriptor + 1, keyGenerator);
            testKeyGenerators(descriptor, descriptor - 1, keyGenerator);
            testKeyGenerators(descriptor, descriptor, keyGenerator);
        }

        // Fixed prefix, tests sign inversion of subsequent values
        if (types.length > 1)
        {

            long pattern = Bytes.bytePatternFor(Long.BYTES - DataGenerators.requiredBytes(spec)[0]);
            for (int i = 0; i < RUNS; i++)
            {
                long descriptor = rand.next();
                long descriptor2 = (descriptor & ~pattern) | (rand.next() & pattern);
                testKeyGenerators(descriptor, descriptor2, keyGenerator);
            }

        }
    }

    static void testKeyGenerators(long descriptor1, long descriptor2, DataGenerators.KeyGenerator keyGenerator)
    {
        descriptor1 = keyGenerator.adjustEntropyDomain(descriptor1);
        descriptor2 = keyGenerator.adjustEntropyDomain(descriptor2);
        Object[] value1 = keyGenerator.inflate(descriptor1);
        Object[] value2 = keyGenerator.inflate(descriptor2);

        assertDescriptorsEqual(descriptor1,
                               keyGenerator.deflate(value1));
        assertDescriptorsEqual(descriptor2,
                               keyGenerator.deflate(value2));

        Assert.assertEquals(String.format("%s %s %s and %s %s %s have different order. ",
                                          Arrays.toString(value1),
                                          toSignString(compare(value1, value2, keyGenerator.columns)),
                                          Arrays.toString(value2),
                                          Long.toHexString(descriptor1),
                                          toSignString(Long.compare(descriptor1, descriptor2)),
                                          Long.toHexString(descriptor2)),
                            normalize(Long.compare(descriptor1, descriptor2)),
                            compare(value1, value2, keyGenerator.columns));
    }

    private static void assertDescriptorsEqual(long l, long r)
    {
        Assert.assertEquals(String.format("Expected %d (0x%s), but got %d (0x%s)",
                                          l, Long.toHexString(l), r, Long.toHexString(r)),
                            l, r);
    }

    @Test
    public void int8GeneratorTest()
    {
        testInverse(Bijections.INT8_GENERATOR);
    }

    @Test
    public void int16GeneratorTest()
    {
        testInverse(Bijections.INT16_GENERATOR);
    }

    @Test
    public void int32GeneratorTest()
    {
        testInverse(Bijections.INT32_GENERATOR);
    }

    @Test
    public void int64GeneratorTest()
    {
        testInverse(Bijections.INT64_GENERATOR);
        testOrderPreserving(Bijections.INT64_GENERATOR);
        testInverse(new Bijections.ReverseBijection(Bijections.INT64_GENERATOR));
        testOrderPreserving(new Bijections.ReverseBijection(Bijections.INT64_GENERATOR), true);
    }

    @Test
    public void booleanGenTest()
    {
        testInverse(Bijections.BOOLEAN_GENERATOR);
        testOrderPreserving(Bijections.BOOLEAN_GENERATOR);
    }

    @Test
    public void floatGeneratorTest()
    {
        testInverse(Bijections.FLOAT_GENERATOR);
        testOrderPreserving(Bijections.FLOAT_GENERATOR, Float::compareTo);
    }

    @Test
    public void doubleGeneratorTest()
    {
        testInverse(Bijections.DOUBLE_GENERATOR);
        testOrderPreserving(Bijections.DOUBLE_GENERATOR);
    }


    @Test
    public void stringGenTest()
    {
        testInverse(new StringBijection());
        testOrderPreserving(new StringBijection());
    }

    public static <T> void testInverse(Bijections.Bijection<T> gen)
    {
        test(gen,
             (v) -> Assert.assertEquals(gen.adjustEntropyDomain(v.descriptor), gen.deflate(v.value)));
    }

    public static <T extends Comparable> void testOrderPreserving(Bijections.Bijection<T> gen)
    {
        testOrderPreserving(gen, false);
    }

    public static <T extends Comparable> void testOrderPreserving(Bijections.Bijection<T> gen, boolean reverse)
    {
        test(gen, gen,
             (v1, v2) -> {
                 long v1Descriptor = gen.adjustEntropyDomain(v1.descriptor);
                 long v2Descriptor = gen.adjustEntropyDomain(v2.descriptor);
                 Assert.assertEquals(String.format("%s (%s) and %s (%s) sort wrong",
                                                   v1.value,
                                                   Long.toHexString(v1Descriptor),
                                                   v2.value,
                                                   Long.toHexString(v2Descriptor)),
                                     normalize(Long.compare(v1Descriptor, v2Descriptor)) * (reverse ? -1 : 1),
                                     normalize(v1.value.compareTo(v2.value)));
             });
    }

    public static <T extends Comparable> void testOrderPreserving(Bijections.Bijection<T> gen, Comparator<T> comparator)
    {
        test(gen, gen,
             (v1, v2) -> Assert.assertEquals(normalize(Long.compare(gen.adjustEntropyDomain(v1.descriptor),
                                                                    gen.adjustEntropyDomain(v2.descriptor))),
                                             normalize(comparator.compare(v1.value, v2.value))));
    }

    public static <T1> void test(Bijections.Bijection<T1> gen1,
                                 Consumer<Generator.Value<T1>> validate)
    {

        for (int i = 0; i < RUNS; i++)
        {
            long descriptor1 = rand.next();
            validate.accept(new Generator.Value<T1>(descriptor1, gen1.inflate(gen1.adjustEntropyDomain(descriptor1))));
        }
    }

    public static <T1, T2> void test(Bijections.Bijection<T1> gen1,
                                     Bijections.Bijection<T2> gen2,
                                     BiConsumer<Generator.Value<T1>, Generator.Value<T2>> validate)
    {

        for (int i = 0; i < RUNS; i++)
        {
            long descriptor1 = rand.next();
            long descriptor2 = rand.next();
            validate.accept(new Generator.Value<T1>(descriptor1, gen1.inflate(gen1.adjustEntropyDomain(descriptor1))),
                            new Generator.Value<T2>(descriptor2, gen2.inflate(gen1.adjustEntropyDomain(descriptor2))));
        }
    }

    public static int[] sizes(int... ints)
    {
        return ints;
    }


    public static String toSignString(int l)
    {
        if (l == 0)
            return "=";
        else if (l > 0)
            return ">";
        return "<";
    }
    public static int normalize(int l)
    {
        if (l == 0)
            return 0;
        if (l > 0)
            return 1;
        else
            return -1;
    }

    static int compare(Object[] a, Object[] b, List<ColumnSpec<?>> spec)
    {
        assert a.length == b.length;
        for (int i = 0; i < a.length; i++)
        {
            Comparable comparableA = (Comparable) a[i];
            Comparable comparableB = (Comparable) b[i];

            int cmp = comparableA.compareTo(comparableB);
            if (cmp != 0)
            {
                if (spec.get(i).isReversed())
                    cmp *= -1;

                return cmp < 0 ? -1 : 1;
            }
        }
        return 0;
    }

    public static <T> Iterator<T[]> permutations(int size, Class<T> klass, T... values)
    {
        int[] cursors = new int[size];
        return new Iterator<T[]>()
        {
            int left = 0;
            T[] next = fromCursors();

            public boolean hasNext()
            {
                for (int i = cursors.length - 1; i >= 0; i--)
                {
                    if (cursors[i] < values.length - 1)
                        return true;
                }
                return false;
            }

            public T[] computeNext()
            {
                cursors[left]++;

                for (int i = 0; i < cursors.length; i++)
                {
                    if (cursors[i] == values.length)
                    {
                        cursors[i] = 0;
                        cursors[i + 1]++;
                    }
                }

                return fromCursors();
            }

            public T[] next()
            {
                if (next == null)
                    next = computeNext();

                T[] ret = next;
                next = null;
                return ret;
            }

            public T[] fromCursors()
            {
                T[] res = (T[]) Array.newInstance(klass, cursors.length);
                for (int i = 0; i < cursors.length; i++)
                    res[i] = values[cursors[i]];
                return res;
            }
        };
    }
}