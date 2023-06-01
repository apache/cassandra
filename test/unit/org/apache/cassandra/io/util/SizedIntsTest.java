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

package org.apache.cassandra.io.util;

import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.io.EndianUtils;
import org.junit.Test;

import org.apache.cassandra.utils.ByteArrayUtil;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Generators;
import org.apache.cassandra.utils.Throwables;

import static org.junit.Assert.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.longs;

public class SizedIntsTest
{
    @Test
    public void nonZeroSize()
    {
        assertEquals(1, SizedInts.nonZeroSize(0));
        assertEquals(1, SizedInts.nonZeroSize(1));
        assertEquals(1, SizedInts.nonZeroSize(127));
        assertEquals(1, SizedInts.nonZeroSize(-127));
        assertEquals(2, SizedInts.nonZeroSize(128));
        assertEquals(1, SizedInts.nonZeroSize(-128));
        assertEquals(2, SizedInts.nonZeroSize(-129));
        assertEquals(8, SizedInts.nonZeroSize(0x7fffffffffffffffL));
        assertEquals(8, SizedInts.nonZeroSize(0x7ffffffffffffffL));
        assertEquals(7, SizedInts.nonZeroSize(0x7fffffffffffffL));
        assertEquals(7, SizedInts.nonZeroSize(0x7ffffffffffffL));
        assertEquals(6, SizedInts.nonZeroSize(0x7fffffffffffL));
        assertEquals(6, SizedInts.nonZeroSize(0x7ffffffffffL));
        assertEquals(5, SizedInts.nonZeroSize(0x7fffffffffL));
        assertEquals(5, SizedInts.nonZeroSize(0x7ffffffffL));
        assertEquals(4, SizedInts.nonZeroSize(0x7fffffffL));
        assertEquals(4, SizedInts.nonZeroSize(0x7ffffffL));
        assertEquals(3, SizedInts.nonZeroSize(0x7fffffL));
        assertEquals(3, SizedInts.nonZeroSize(0x7ffffL));
        assertEquals(2, SizedInts.nonZeroSize(0x7fffL));
        assertEquals(2, SizedInts.nonZeroSize(0x7ffL));
        assertEquals(1, SizedInts.nonZeroSize(0x7fL));
        assertEquals(1, SizedInts.nonZeroSize(0x7L));
    }

    @Test
    public void sizeAllowingZero()
    {
        assertEquals(0, SizedInts.sizeAllowingZero(0));
        assertEquals(1, SizedInts.sizeAllowingZero(1));
        assertEquals(1, SizedInts.sizeAllowingZero(127));
        assertEquals(1, SizedInts.sizeAllowingZero(-127));
        assertEquals(2, SizedInts.sizeAllowingZero(128));
        assertEquals(1, SizedInts.sizeAllowingZero(-128));
        assertEquals(2, SizedInts.sizeAllowingZero(-129));
    }


    @Test
    public void readWrite()
    {
        try (DataOutputBuffer out = new DataOutputBuffer(8))
        {
            qt().forAll(Generators.bytes(1, 8).map(bb -> new BigInteger(ByteBufferUtil.getArray(bb)).longValue()).mix(longs().all())).check(v -> {
                out.clear();
                try
                {
                    SizedInts.write(out, v, SizedInts.sizeAllowingZero(v));
                    out.flush();
                    long r = SizedInts.read(out.asNewBuffer(), 0, SizedInts.sizeAllowingZero(v));
                    return v == r;
                }
                catch (IOException e)
                {
                    throw Throwables.cleaned(e);
                }
            });
        }
    }

    @Test
    public void readWriteUnsigned()
    {
        try (DataOutputBuffer out = new DataOutputBuffer(8))
        {
            byte[] buf1 = new byte[8];
            byte[] buf2 = new byte[8];
            qt().forAll(Generators.bytes(1, 8).map(bb -> new BigInteger(ByteBufferUtil.getArray(bb)).longValue()).mix(longs().all())).check(v -> {
                out.clear();
                try
                {
                    int size = SizedInts.sizeAllowingZero(v);
                    EndianUtils.writeSwappedLong(buf1, 0, v);
                    SizedInts.write(out, v, size);
                    out.flush();
                    long r = SizedInts.readUnsigned(out.asNewBuffer(), 0, size);
                    EndianUtils.writeSwappedLong(buf2, 0, r);
                    return ByteArrayUtil.compareUnsigned(buf1, 0, buf2, 0, size) == 0;
                }
                catch (IOException e)
                {
                    throw Throwables.cleaned(e);
                }
            });
        }
    }
}