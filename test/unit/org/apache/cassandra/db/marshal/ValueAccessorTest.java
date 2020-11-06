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

package org.apache.cassandra.db.marshal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;
import org.quicktheories.generators.SourceDSL;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;
import static org.apache.cassandra.utils.ByteArrayUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.integers;

public class ValueAccessorTest
{
    final static Supplier<Gen<Integer>> leftPadding = () -> integers().between(0, 2);
    final static Supplier<Gen<Integer>> rightPadding = () -> integers().between(0, 2);
    final static Supplier<Gen<ValueAccessor>> accessors = () -> SourceDSL.arbitrary().pick(ACCESSORS);
    final static Supplier<Gen<Function>> paddings = () -> leftPadding.get().zip(rightPadding.get(), (l, r) -> v -> maybePad(v, l, r));
    final static byte[] bytes = new byte[200];

    @BeforeClass
    public static void beforeClass()
    {
        new Random(0).nextBytes(bytes);
    }

    /**
     * This method should be used to test values to be processed by {@link ValueAccessor}
     * in order to make sure we do not assume position == 0 in the underlying {@link ByteBuffer}
     */
    public static <V> V maybePad(V value, int lPad, int rPad)
    {
        if (value instanceof ByteBuffer)
        {
            ByteBuffer buf = ByteBuffer.allocate(((ByteBuffer) value).remaining() + lPad + rPad);
            buf.position(lPad);
            buf.duplicate().put((ByteBuffer) value);
            buf.limit(buf.limit() - rPad);
            return (V) buf;
        }
        else
        {
            return value;
        }
    }

    private static byte[] randomBytes(int size)
    {
        assert size <= ValueAccessorTest.bytes.length;
        byte[] bytes = new byte[size];
        System.arraycopy(ValueAccessorTest.bytes, 0, bytes, 0, size);
        return bytes;
    }

    private static <V1, V2> void testHashCodeAndEquals(int size, int sliceOffset, int sliceSize,
                                                       ValueAccessor<V1> accessor1, Function<V1, V1> padding1,
                                                       ValueAccessor<V2> accessor2, Function<V2, V2> padding2)
    {
        byte[] rawBytes = randomBytes(size);
        V1 value1 = accessor1.slice(padding1.apply(accessor1.valueOf(rawBytes)), sliceOffset, sliceSize);
        V2 value2 = accessor2.slice(padding2.apply(accessor2.valueOf(rawBytes)), sliceOffset, sliceSize);

        Assert.assertTrue(ValueAccessor.equals(value1, accessor1, value2, accessor2));

        int hash1 = accessor1.hashCode(value1);
        int hash2 = accessor2.hashCode(value2);
        Assert.assertEquals(String.format("Inconsistency hash codes (%s != %s)", hash1, hash2), hash1, hash2);

        byte[] array1 = accessor1.toArray(value1);
        byte[] array2 = accessor2.toArray(value2);
        Assert.assertArrayEquals(String.format("Inconsistent byte arrays (%s != %s)", bytesToHex(array1), bytesToHex(array2)),
                                 array1, array2);

        ByteBuffer buffer1 = accessor1.toBuffer(value1);
        ByteBuffer buffer2 = accessor2.toBuffer(value2);
        Assert.assertEquals(String.format("Inconsistent byte buffers (%s != %s)", bytesToHex(buffer1), bytesToHex(buffer1)),
                            buffer1, buffer2);
    }

    /**
     * Identical data should yield identical hashcodes even if the underlying format is different
     */
    @Test
    public void testHashCodeAndEquals()
    {
        qt().forAll(accessors.get(), paddings.get(), accessors.get(), paddings.get())
            .checkAssert((accessor1, padding1, accessor2, padding2) -> {
                Gen<Integer> sizes = integers().between(2, 200);
                Gen<Integer> sliceOffsets = integers().between(2, 200);
                Gen<Integer> sliceSizes = integers().between(2, 200);
                qt().withExamples(1000).forAll(sizes, sliceOffsets, sliceSizes)
                    .as((size, sliceOffset, sliceSize) -> {
                        sliceOffset = sliceSize % size;
                        sliceSize = sliceSize % (size - sliceOffset);
                        return ImmutableTriple.of(size, sliceOffset, sliceSize);
                    })
                    .checkAssert(triple -> {
                        testHashCodeAndEquals(triple.left, triple.middle, triple.right, accessor1, padding1, accessor2, padding2);
                    });
            });
    }

    private static <V> void testTypeConversion(Number value, ValueAccessor<V> accessor, Function<V, V> pad)
    {
        String msg = String.format("%s value: %s (%s)", accessor.getClass().getSimpleName(), value, value.getClass().getSimpleName());
        byte[] b = value.toString().getBytes();
        Assert.assertEquals(msg, b[0], accessor.toByte(pad.apply(accessor.valueOf(b[0]))));
        Assert.assertArrayEquals(msg, b, accessor.toArray(pad.apply(accessor.valueOf(b))));

        short s = value.shortValue();
        Assert.assertEquals(msg, s, accessor.toShort(pad.apply(accessor.valueOf(s))));

        int i = value.intValue();
        Assert.assertEquals(msg, i, accessor.toInt(pad.apply(accessor.valueOf(i))));

        long l = value.longValue();
        Assert.assertEquals(msg, l, accessor.toLong(pad.apply(accessor.valueOf(l))));

        float f = value.floatValue();
        Assert.assertEquals(msg, f, accessor.toFloat(pad.apply(accessor.valueOf(f))), 0.000002);

        double d = value.doubleValue();
        Assert.assertEquals(msg, d, accessor.toDouble(pad.apply(accessor.valueOf(d))), 0.0000002);
    }

    @Test
    public void testTypeConversion()
    {
        qt().forAll(accessors.get(), leftPadding.get(), rightPadding.get()).checkAssert((accessor, lPad, rPad) -> {
            Gen<Double> numbers = doubles().between(-32000, 32000);
            qt().withExamples(10000).forAll(numbers).checkAssert(i -> testTypeConversion(i, accessor, v -> maybePad(v, lPad, rPad)));
        });
    }

    private static <V> void testReadWriteWithShortLength(ValueAccessor<V> accessor, int size, int lPad, int rPad) throws IOException
    {
        Random random = new Random(size);
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        ByteBuffer buffer = maybePad(ByteBuffer.wrap(bytes), lPad, rPad);

        try (DataOutputBuffer out = new DataOutputBuffer(size + 2))
        {
            ByteBufferUtil.writeWithShortLength(buffer, out);
            V flushed = maybePad(accessor.valueOf(out.toByteArray()), lPad, rPad);
            V value = accessor.sliceWithShortLength(flushed, 0);
            Assert.assertArrayEquals(bytes, accessor.toArray(value));
        }
    }

    @Test
    public void testReadWriteWithShortLength() throws IOException
    {

        qt().forAll(accessors.get(), leftPadding.get(), rightPadding.get()).checkAssert((accessor, lPad, rPad) -> {

            Gen<Integer> lengths = SourceDSL.arbitrary().pick(0, 1, 2, 256, 0x8001, 0xFFFF);
            qt().forAll(lengths).checkAssert(length -> {
                try
                {
                    testReadWriteWithShortLength(accessor, length, lPad, rPad);
                }
                catch (IOException e)
                {
                    Assert.fail("Unexpected exception: " + e);
                }
            });
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testUnsignedShort()
    {
        qt().forAll(accessors.get(), leftPadding.get(), rightPadding.get()).checkAssert((accessor, lPad, rPad) -> {
            Gen<Integer> sizes = integers().between(0, Short.MAX_VALUE * 2 + 1);
            Gen<Integer> offsets = arbitrary().pick(0, 3);
            Object value = maybePad(accessor.allocate(5), lPad, rPad);
            qt().forAll(sizes, offsets).checkAssert((size, offset) -> {
                accessor.putShort(value, offset, size.shortValue()); // testing signed
                Assertions.assertThat(accessor.getUnsignedShort(value, offset))
                          .as("getUnsignedShort(putShort(unsigned_short)) != unsigned_short for %s", accessor.getClass())
                          .isEqualTo(size);
            });
        });
    }
}
