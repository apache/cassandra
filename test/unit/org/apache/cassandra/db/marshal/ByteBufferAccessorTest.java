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

import org.apache.cassandra.utils.ByteBufferUtil;

import static org.quicktheories.QuickTheory.qt;

public class ByteBufferAccessorTest extends ValueAccessorTester
{
    private static byte[] array(int start, int size)
    {
        byte[] a = new byte[size];
        for (int i = 0; i < size; i++)
            a[i] = (byte) (start + i);
        return a;
    }

    private <V> void testCopyFromOffsets(ValueAccessor<V> dstAccessor, int padding1, int padding2)
    {
        ByteBuffer src = leftPad(ByteBuffer.wrap(array(0, 10)), padding1);
        src.position(src.position() + 5);
        Assert.assertEquals(5, src.remaining());

        V dst = leftPad(dstAccessor.allocate(5), padding2);
        ByteBufferAccessor.instance.copyTo(src, 0, dst, dstAccessor, 0, 5);
        Assert.assertArrayEquals(array(5, 5), dstAccessor.toArray(dst));
    }

    /**
     * Test byte buffers with position > 0 are copied correctly
     */
    @Test
    public void testCopyFromOffets()
    {
        qt().forAll(accessors(), bbPadding(), bbPadding())
            .checkAssert(this::testCopyFromOffsets);
    }

    private <V> void testCopyToOffsets(ValueAccessor<V> srcAccessor, int padding1, int padding2)
    {
        byte[] value = array(5, 5);
        V src = leftPad(srcAccessor.allocate(5), padding1);
        ByteArrayAccessor.instance.copyTo(value, 0, src, srcAccessor, 0, value.length);

        ByteBuffer bb = leftPad(ByteBuffer.wrap(new byte[10]), padding2);
        ByteBuffer actual = bb.duplicate();
        bb.position(bb.position() + 5);
        srcAccessor.copyTo(src, 0, bb, ByteBufferAccessor.instance, 0, value.length);

        byte[] expected = new byte[10];
        System.arraycopy(value, 0, expected, 5, 5);
        Assert.assertArrayEquals(srcAccessor.getClass().getSimpleName(), expected, ByteBufferUtil.getArray(actual));
    }

    @Test
    public void testCopyToOffsets()
    {
        qt().forAll(accessors(), bbPadding(), bbPadding())
            .checkAssert(this::testCopyToOffsets);
    }
}
