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
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.assertj.core.api.Assertions;
import org.quicktheories.core.Gen;

import static org.apache.cassandra.utils.ByteArrayUtil.bytesToHex;
import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.Generate.constant;
import static org.quicktheories.generators.Generate.intArrays;
import static org.quicktheories.generators.SourceDSL.arbitrary;
import static org.quicktheories.generators.SourceDSL.doubles;
import static org.quicktheories.generators.SourceDSL.floats;
import static org.quicktheories.generators.SourceDSL.integers;
import static org.quicktheories.generators.SourceDSL.longs;

public class ValueAccessorTest extends ValueAccessorTester
{
    private static <V1, V2> void testHashCodeAndEquals(byte[] rawBytes,
                                                       ValueAccessor<V1> accessor1,
                                                       ValueAccessor<V2> accessor2,
                                                       int[] paddings)
    {
        V1 value1 = leftPad(accessor1.valueOf(rawBytes), paddings[0]);
        V2 value2 = leftPad(accessor2.valueOf(rawBytes), paddings[1]);

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
        qt().forAll(byteArrays(integers().between(2, 200)),
                    accessors(),
                    accessors(),
                    intArrays(constant(2), bbPadding()))
            .checkAssert(ValueAccessorTest::testHashCodeAndEquals);
    }

    private static <V> void testSlice(ValueAccessor<V> accessor, ByteArraySlice slice, int padding)
    {
        V value = leftPad(accessor.valueOf(slice.originalArray), padding);
        V s = accessor.slice(value, slice.offset, slice.length);

        byte[] array = accessor.toArray(s);
        byte[] expected = slice.toArray();
        Assert.assertArrayEquals(expected, array);
    }

    @Test
    public void testSlice()
    {
        qt().forAll(accessors(),
                    slices(byteArrays(integers().between(2, 200))),
                    bbPadding())
            .checkAssert(ValueAccessorTest::testSlice);
    }

    private static <V> void testByteArrayConversion(byte[] array, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf(array), padding);
        Assert.assertArrayEquals(array, accessor.toArray(value));
    }

    private static <V> void testByteConversion(int b, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf((byte) b), padding);
        Assert.assertEquals(b, accessor.toByte(value));
    }

    private static <V> void testShortConversion(int s, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf((short) s), padding);
        Assert.assertEquals(s, accessor.toShort(value));
    }

    private static <V> void testIntConversion(int i, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf(i), padding);
        Assert.assertEquals(i, accessor.toInt(value));
    }

    private static <V> void testLongConversion(long l, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf(l), padding);
        Assert.assertEquals(l, accessor.toLong(value));
    }

    private static <V> void testUnsignedVIntConversion(long l, ValueAccessor<V> accessor, int padding)
    {
        V value = accessor.allocate(VIntCoding.computeUnsignedVIntSize(l));
        accessor.putUnsignedVInt(value, 0, l);
        value = leftPad(value, padding);
        Assert.assertEquals(l, accessor.getUnsignedVInt(value, 0));
    }

    private static <V> void testVIntConversion(long l, ValueAccessor<V> accessor, int padding)
    {
        V value = accessor.allocate(VIntCoding.computeVIntSize(l));
        accessor.putVInt(value, 0, l);
        value = leftPad(value, padding);
        Assert.assertEquals(l, accessor.getVInt(value, 0));
    }

    private static <V> void testUnsignedVInt32Conversion(int l, ValueAccessor<V> accessor, int padding)
    {
        V value = accessor.allocate(VIntCoding.computeUnsignedVIntSize(l));
        accessor.putUnsignedVInt32(value, 0, l);
        value = leftPad(value, padding);
        Assert.assertEquals(l, accessor.getUnsignedVInt32(value, 0));
    }

    private static <V> void testVInt32Conversion(int l, ValueAccessor<V> accessor, int padding)
    {
        V value = accessor.allocate(VIntCoding.computeVIntSize(l));
        accessor.putVInt32(value, 0, l);
        value = leftPad(value, padding);
        Assert.assertEquals(l, accessor.getVInt32(value, 0));
    }

    private static <V> void testFloatConversion(float f, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf(f), padding);
        Assert.assertEquals(f, accessor.toFloat(value), 0.000002);
    }

    private static <V> void testDoubleConversion(double d, ValueAccessor<V> accessor, int padding)
    {
        V value = leftPad(accessor.valueOf(d), padding);
        Assert.assertEquals(d, accessor.toDouble(value), 0.000002);
    }

    @Test
    public void testTypeConversion()
    {
        qt().forAll(byteArrays(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testByteArrayConversion);

        qt().forAll(integers().between(Byte.MIN_VALUE, Byte.MAX_VALUE),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testByteConversion);

        qt().forAll(integers().between(Short.MIN_VALUE, Short.MAX_VALUE),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testShortConversion);

        qt().forAll(integers().all(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testIntConversion);

        qt().forAll(longs().all(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testLongConversion);

        qt().forAll(longs().between(0, Long.MAX_VALUE),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testUnsignedVIntConversion);

        qt().forAll(longs().all(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testVIntConversion);

        qt().forAll(integers().between(0, Integer.MAX_VALUE),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testUnsignedVInt32Conversion);

        qt().forAll(integers().all(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testVInt32Conversion);

        qt().forAll(floats().any(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testFloatConversion);

        qt().forAll(doubles().any(),
                    accessors(),
                    bbPadding()).checkAssert(ValueAccessorTest::testDoubleConversion);
    }

    private static <V> void testReadWriteWithShortLength(ValueAccessor<V> accessor, byte[] bytes, int padding)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        try (DataOutputBuffer out = new DataOutputBuffer(bytes.length + 2))
        {
            ByteBufferUtil.writeWithShortLength(buffer, out);
            V flushed = leftPad(accessor.valueOf(out.toByteArray()), padding);
            V value = accessor.sliceWithShortLength(flushed, 0);
            Assert.assertArrayEquals(bytes, accessor.toArray(value));
        }
        catch (IOException e)
        {
            Assert.fail("Unexpected exception: " + e);
        }
    }

    @Test
    public void testReadWriteWithShortLength()
    {
         Gen<Integer> lengths = arbitrary().pick(0, 1, 2, 256, 0x8001, 0xFFFF);
         qt().forAll(accessors(),
                     byteArrays(lengths),
                     bbPadding()).checkAssert(ValueAccessorTest::testReadWriteWithShortLength);
    }

    public static <V> void testUnsignedShort(int jint, ValueAccessor<V> accessor, int padding, int offset)
    {
        V value = leftPad(accessor.allocate(5), padding);
        accessor.putShort(value, offset, (short) jint); // testing signed
        Assertions.assertThat(accessor.getUnsignedShort(value, offset))
                  .as("getUnsignedShort(putShort(unsigned_short)) != unsigned_short for %s", accessor.getClass())
                  .isEqualTo(jint);
    }

    @Test
    public void testUnsignedShort()
    {
        qt().forAll(integers().between(0, Short.MAX_VALUE * 2 + 1),
                    accessors(),
                    bbPadding(),
                    integers().between(0, 3)).checkAssert(ValueAccessorTest::testUnsignedShort);
    }

    private static Gen<ByteArraySlice> slices(Gen<byte[]> arrayGen)
    {
        return td -> {
            byte[] array = arrayGen.generate(td);
            int arrayLength = array.length;
            int offset = integers().between(0, arrayLength - 1).generate(td);
            int length = integers().between(0, arrayLength - offset - 1).generate(td);
            return new ByteArraySlice(array, offset, length);
        };
    }

    private static final class ByteArraySlice
    {
        /**
         * The original array
         */
        final byte[] originalArray;

        /**
         * The slice offset;
         */
        final int offset;

        /**
         * The slice length
         */
        final int length;

        public ByteArraySlice(byte[] bytes, int offset, int length)
        {
            this.originalArray = bytes;
            this.offset = offset;
            this.length = length;
        }

        /**
         * Returns the silce as a byte array.
         */
        public byte[] toArray()
        {
            return Arrays.copyOfRange(originalArray, offset, offset + length);
        }

        @Override
        public String toString()
        {
            return "Byte Array Slice [array=" + Arrays.toString(originalArray) + ", offset=" + offset + ", length=" + length + "]";
        }
    }
}
