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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import com.google.common.primitives.Shorts;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.ValueAccessors.ACCESSORS;

public class ValueAccessorTest
{
    private static byte[] randomBytes(int minSize, int maxSize, Random random)
    {
        int size = random.nextInt(maxSize - minSize + 1) + minSize;
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static ValueAccessor<?> nextAccessor(Random random)
    {
        return ACCESSORS[random.nextInt(ACCESSORS.length)];
    }

    private static <V1, V2> void testHashCodeAndEquals(int seed, ValueAccessor<V1> accessor1, ValueAccessor<V2> accessor2, Random random)
    {
        byte[] rawBytes = randomBytes(2, 200, random);
        int sliceOffset = random.nextInt(rawBytes.length);
        int sliceSize = random.nextInt(rawBytes.length - sliceOffset);
        V1 value1 = accessor1.slice(accessor1.valueOf(rawBytes), sliceOffset, sliceSize);
        V2 value2 = accessor2.slice(accessor2.valueOf(rawBytes), sliceOffset, sliceSize);

        Assert.assertTrue(ValueAccessor.equals(value1, accessor1, value2, accessor2));

        int hash1 =  accessor1.hashCode(value1);
        int hash2 =  accessor2.hashCode(value2);
        if (hash1 != hash2)
            throw new AssertionError(String.format("%s and %s produced inconsistency hash codes for seed %s (%s != %s)",
                                                   accessor1.getClass().getSimpleName(), accessor2.getClass().getSimpleName(),
                                                   seed, hash1, hash2));

        byte[] array1 = accessor1.toArray(value1);
        byte[] array2 = accessor2.toArray(value2);
        if (!Arrays.equals(array1, array2))
            throw new AssertionError(String.format("%s and %s produced inconsistent byte arrays for seed %s (%s != %s)",
                                                   accessor1.getClass().getSimpleName(), accessor2.getClass().getSimpleName(),
                                                   seed, ByteArrayUtil.bytesToHex(array1), ByteArrayUtil.bytesToHex(array2)));

        ByteBuffer buffer1 = accessor1.toBuffer(value1);
        ByteBuffer buffer2 = accessor2.toBuffer(value2);
        if (!buffer1.equals(buffer2))
            throw new AssertionError(String.format("%s and %s produced inconsistent byte buffers for seed %s (%s != %s)",
                                                   accessor1.getClass().getSimpleName(), accessor2.getClass().getSimpleName(),
                                                   seed, ByteBufferUtil.bytesToHex(buffer1), ByteBufferUtil.bytesToHex(buffer1)));

    }

    private static void testHashCodeAndEquals(int seed)
    {
        Random random = new Random(seed);
        testHashCodeAndEquals(seed, nextAccessor(random), nextAccessor(random), random);
    }

    /**
     * Identical data should yield identical hashcodes even if the underlying format is different
     */
    @Test
    public void testHashCodeAndEquals()
    {
        for (int i=0; i<10000; i++)
            testHashCodeAndEquals(i);
    }

    private static <V> void testTypeConversion(int seed, ValueAccessor<V> accessor, Random random)
    {
        String msg = accessor.getClass().getSimpleName() + " seed: " + seed;
        byte[] b = new byte[2];
        random.nextBytes(b);
        Assert.assertEquals(msg, b[0], accessor.toByte(accessor.valueOf(b[0])));
        Assert.assertArrayEquals(msg, b, accessor.toArray(accessor.valueOf(b)));

        short s = Shorts.fromBytes(b[0], b[1]);
        Assert.assertEquals(msg, s, accessor.toShort(accessor.valueOf(s)));

        int i = random.nextInt();
        Assert.assertEquals(msg, i, accessor.toInt(accessor.valueOf(i)));

        long l = random.nextLong();
        Assert.assertEquals(msg, l, accessor.toLong(accessor.valueOf(l)));

        float f = random.nextFloat();
        Assert.assertEquals(msg, f, accessor.toFloat(accessor.valueOf(f)), 0.000002);

        double d = random.nextDouble();
        Assert.assertEquals(msg, d, accessor.toDouble(accessor.valueOf(d)), 0.0000002);
    }

    @Test
    public void testTypeConversion()
    {
        for (int i=0; i<10000; i++)
        {
            Random random = new Random(i);
            for (ValueAccessor<?> accessor: ACCESSORS)
                testTypeConversion(i, accessor, random);
        }
    }
}
