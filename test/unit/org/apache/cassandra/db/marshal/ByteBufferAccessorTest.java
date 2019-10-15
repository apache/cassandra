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

import org.junit.Assert;
import org.junit.Test;

public class ByteBufferAccessorTest
{
    private static final ValueAccessor<ByteBuffer> bbAccessor = ByteBufferAccessor.instance;
    private static final ValueAccessor<byte[]> baAccessor = ByteArrayAccessor.instance;

    private static byte[] array(int start, int size)
    {
        assert Byte.MIN_VALUE <= start && start <= Byte.MAX_VALUE;
        assert (start + size) <= Byte.MAX_VALUE;
        byte[] a = new byte[size];
        for (int i=0; i<size; i++)
            a[i] = (byte) (start + i);
        return a;
    }

    private <V> void testCopyFromOffsets(ValueAccessor<V> dstAccessor)
    {
        ByteBuffer src = ByteBuffer.wrap(array(0, 10));
        src.position(5);
        Assert.assertEquals(5, src.remaining());

        V dst = dstAccessor.allocate(5);
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
        for (ValueAccessor<?> accessor : ValueAccessors.ACCESSORS)
            testCopyFromOffsets(accessor);
    }

    private <V> void testCopyToOffsets(ValueAccessor<V> srcAccessor)
    {
        byte[] value = array(5, 5);
        V src = srcAccessor.allocate(5);
        baAccessor.copyTo(value, 0, src, srcAccessor, 0, value.length);

        byte[] actual = new byte[10];
        ByteBuffer bb = ByteBuffer.wrap(actual);
        bb.position(5);
        srcAccessor.copyTo(src, 0, bb, bbAccessor, 0, value.length);

        byte[] expected = new byte[10];
        System.arraycopy(value, 0, expected, 5, 5);
        Assert.assertArrayEquals(srcAccessor.getClass().getSimpleName(), expected, actual);
    }

    @Test
    public void testCopyToOffsets()
    {
        for (ValueAccessor<?> accessor : ValueAccessors.ACCESSORS)
            testCopyToOffsets(accessor);
    }
}
