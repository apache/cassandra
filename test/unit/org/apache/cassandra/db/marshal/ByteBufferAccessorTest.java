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
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.marshal.ValueAccessorTest.accessors;
import static org.quicktheories.QuickTheory.qt;

public class ByteBufferAccessorTest
{
    private static final ValueAccessor<ByteBuffer> bbAccessor = ByteBufferAccessor.instance;
    private static final ValueAccessor<byte[]> baAccessor = ByteArrayAccessor.instance;

    private static byte[] array(int start, int size)
    {
        assert Byte.MIN_VALUE <= start && start <= Byte.MAX_VALUE;
        assert (start + size) <= Byte.MAX_VALUE;
        byte[] a = new byte[size];
        for (int i = 0; i < size; i++)
            a[i] = (byte) (start + i);
        return a;
    }

    private <V> void testCopyFromOffsets(ValueAccessor<V> dstAccessor, Function<ByteBuffer, ByteBuffer> padding1, Function<V, V> padding2)
    {
        ByteBuffer src = padding1.apply(ByteBuffer.wrap(array(0, 10)));
        src.position(src.position() + 5);
        Assert.assertEquals(5, src.remaining());

        V dst = padding2.apply(dstAccessor.allocate(5));
        bbAccessor.copyTo(src, 0, dst, dstAccessor, 0, 5);
        Assert.assertArrayEquals(dstAccessor.getClass().getSimpleName(),
                                 array(5, 5), dstAccessor.toArray(dst));
    }

    /**
     * Test byte buffers with position > 0 are copied correctly
     */
    @Test
    public void testCopyFromOffets()
    {
        qt().forAll(accessors.get(), ValueAccessorTest.paddings.get(), ValueAccessorTest.paddings.get())
            .checkAssert(this::testCopyFromOffsets);
    }

    private <V> void testCopyToOffsets(ValueAccessor<V> srcAccessor, Function<V, V> padding1, Function<ByteBuffer, ByteBuffer> padding2)
    {
        byte[] value = array(5, 5);
        V src = padding1.apply(srcAccessor.allocate(5));
        baAccessor.copyTo(value, 0, src, srcAccessor, 0, value.length);

        ByteBuffer bb = padding2.apply(ByteBuffer.wrap(new byte[10]));
        ByteBuffer actual = bb.duplicate();
        bb.position(bb.position() + 5);
        srcAccessor.copyTo(src, 0, bb, bbAccessor, 0, value.length);

        byte[] expected = new byte[10];
        System.arraycopy(value, 0, expected, 5, 5);
        Assert.assertArrayEquals(srcAccessor.getClass().getSimpleName(), expected, ByteBufferUtil.getArray(actual));
    }

    @Test
    public void testCopyToOffsets()
    {
        qt().forAll(accessors.get(), ValueAccessorTest.paddings.get(), ValueAccessorTest.paddings.get())
            .checkAssert(this::testCopyToOffsets);
    }
}
