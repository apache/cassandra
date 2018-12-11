/**
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

package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class ByteBufferUtilTest
{
    private static final String s = "cassandra";

    private ByteBuffer fromStringWithPosition(String s, int pos, boolean direct)
    {
        int l = s.length();
        ByteBuffer bb;
        if (direct)
        {
            bb = ByteBuffer.allocateDirect(l + pos);
        }
        else
        {
            ByteBuffer tmp = ByteBuffer.allocate(l + pos + 3);
            tmp.position(3);
            bb = tmp.slice(); // make bb have a non null arrayOffset
        }
        bb.position(pos);
        bb.mark();
        bb.put(s.getBytes());
        bb.reset();
        assert bb.position() == pos;
        return bb;
    }

    @Test
    public void testString() throws Exception
    {
        assert s.equals(ByteBufferUtil.string(ByteBufferUtil.bytes(s)));

        int pos = 10;
        ByteBuffer bb = fromStringWithPosition(s, 10, false);
        assert s.equals(ByteBufferUtil.string(bb, 10, s.length()));

        bb = fromStringWithPosition(s, 10, true);
        assert s.equals(ByteBufferUtil.string(bb, 10, s.length()));
    }

    @Test
    public void testGetArray()
    {
        byte[] t = s.getBytes();

        ByteBuffer bb = ByteBufferUtil.bytes(s);
        assertArrayEquals(t, ByteBufferUtil.getArray(bb));

        bb = fromStringWithPosition(s, 10, false);
        assertArrayEquals(t, ByteBufferUtil.getArray(bb));

        bb = fromStringWithPosition(s, 10, true);
        assertArrayEquals(t, ByteBufferUtil.getArray(bb));
    }

    @Test
    public void testLastIndexOf()
    {
        ByteBuffer bb = ByteBufferUtil.bytes(s);
        checkLastIndexOf(bb);

        bb = fromStringWithPosition(s, 10, false);
        checkLastIndexOf(bb);

        bb = fromStringWithPosition(s, 10, true);
        checkLastIndexOf(bb);
    }

    private void checkLastIndexOf(ByteBuffer bb)
    {
        assert bb.position() + 8 == ByteBufferUtil.lastIndexOf(bb, (byte) 'a', bb.position() + 8);
        assert bb.position() + 4 == ByteBufferUtil.lastIndexOf(bb, (byte) 'a', bb.position() + 7);
        assert bb.position() + 3 == ByteBufferUtil.lastIndexOf(bb, (byte) 's', bb.position() + 8);
        assert -1 == ByteBufferUtil.lastIndexOf(bb, (byte) 'o', bb.position() + 8);
        assert -1 == ByteBufferUtil.lastIndexOf(bb, (byte) 'd', bb.position() + 5);
    }

    @Test
    public void testClone()
    {
        ByteBuffer bb = ByteBufferUtil.bytes(s);
        ByteBuffer clone1 = ByteBufferUtil.clone(bb);
        assert bb != clone1;
        assert bb.equals(clone1);
        assert bb.array() != clone1.array();

        bb = fromStringWithPosition(s, 10, false);
        ByteBuffer clone2 = ByteBufferUtil.clone(bb);
        assert bb != clone2;
        assert bb.equals(clone2);
        assert clone1.equals(clone2);
        assert bb.array() != clone2.array();

        bb = fromStringWithPosition(s, 10, true);
        ByteBuffer clone3 = ByteBufferUtil.clone(bb);
        assert bb != clone3;
        assert bb.equals(clone3);
        assert clone1.equals(clone3);
    }

    @Test
    public void testArrayCopy()
    {
        ByteBuffer bb = ByteBufferUtil.bytes(s);
        checkArrayCopy(bb);

        bb = fromStringWithPosition(s, 10, false);
        checkArrayCopy(bb);

        bb = fromStringWithPosition(s, 10, true);
        checkArrayCopy(bb);
    }

    private void checkArrayCopy(ByteBuffer bb)
    {

        byte[] bytes = new byte[s.length()];
        ByteBufferUtil.arrayCopy(bb, bb.position(), bytes, 0, s.length());
        assertArrayEquals(s.getBytes(), bytes);

        bytes = new byte[5];
        ByteBufferUtil.arrayCopy(bb, bb.position() + 3, bytes, 1, 4);
        assertArrayEquals(Arrays.copyOfRange(s.getBytes(), 3, 7), Arrays.copyOfRange(bytes, 1, 5));
    }

    @Test
    public void testReadWrite() throws IOException
    {
        ByteBuffer bb = ByteBufferUtil.bytes(s);
        checkReadWrite(bb);

        bb = fromStringWithPosition(s, 10, false);
        checkReadWrite(bb);

        bb = fromStringWithPosition(s, 10, true);
        checkReadWrite(bb);
    }

    private void checkReadWrite(ByteBuffer bb) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ByteBufferUtil.writeWithLength(bb, out);
        ByteBufferUtil.writeWithShortLength(bb, out);

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        assert bb.equals(ByteBufferUtil.readWithLength(in));
        assert bb.equals(ByteBufferUtil.readWithShortLength(in));
    }

    @Test
    public void testInputStream() throws IOException
    {
        ByteBuffer bb = ByteBuffer.allocate(13);
        bb.putInt(255);
        bb.put((byte) -3);
        bb.putLong(42L);
        bb.clear();

        DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(bb));
        assert in.readInt() == 255;
        assert in.readByte() == (byte)-3;
        assert in.readLong() == 42L;
    }

    @Test
    public void testIntBytesConversions()
    {
        // positive, negative, 1 and 2 byte cases, including a few edges that would foul things up unless you're careful
        // about masking away sign extension.
        int[] ints = new int[]
        {
            -20, -127, -128, 0, 1, 127, 128, 65534, 65535, -65534, -65535
        };

        for (int i : ints) {
            ByteBuffer ba = ByteBufferUtil.bytes(i);
            int actual = ByteBufferUtil.toInt(ba);
            assertEquals(i, actual);
        }
    }

    @Test(expected=CharacterCodingException.class)
    public void testDecode() throws IOException
    {
        ByteBuffer bytes = ByteBuffer.wrap(new byte[]{(byte)0xff, (byte)0xfe});
        ByteBufferUtil.string(bytes);
    }

    @Test
    public void testHexBytesConversion()
    {
        for (int i = Byte.MIN_VALUE; i <= Byte.MAX_VALUE; i++)
        {
            ByteBuffer bb = ByteBuffer.allocate(1);
            bb.put((byte) i);
            bb.clear();
            String s = ByteBufferUtil.bytesToHex(bb);
            ByteBuffer bb2 = ByteBufferUtil.hexToBytes(s);
            assertEquals(bb, bb2);
        }
        // check that non-zero buffer positions work,
        // i.e. that conversion accounts for the buffer offset and limit correctly
        ByteBuffer bb = ByteBuffer.allocate(4);
        for (int i = 0; i < 4; i++)
        {
            bb.put((byte) i);
        }
        // use a chunk out of the middle of the buffer
        bb.position(1);
        bb.limit(3);
        assertEquals(2, bb.remaining());
        String s = ByteBufferUtil.bytesToHex(bb);
        ByteBuffer bb2 = ByteBufferUtil.hexToBytes(s);
        assertEquals(bb, bb2);
        assertEquals("0102", s);
    }

    @Test
    public void testStartsAndEndsWith()
    {
        byte[] bytes = new byte[512];
        ThreadLocalRandom random = ThreadLocalRandom.current();

        random.nextBytes(bytes);

        ByteBuffer a = ByteBuffer.wrap(bytes);
        ByteBuffer b = a.duplicate();

        // let's take random slices of a and match
        for (int i = 0; i < 512; i++)
        {
            // prefix from the original offset
            b.position(0).limit(a.remaining() - random.nextInt(0, a.remaining() - 1));
            Assert.assertTrue(ByteBufferUtil.startsWith(a, b));
            Assert.assertTrue(ByteBufferUtil.startsWith(a, b.slice()));

            // prefix from random position inside of array
            int pos = random.nextInt(1, a.remaining() - 5);
            a.position(pos);
            b.limit(bytes.length - 1).position(pos);

            Assert.assertTrue(ByteBufferUtil.startsWith(a, b));

            a.position(0);

            // endsWith at random position
            b.limit(a.remaining()).position(random.nextInt(0, a.remaining() - 1));
            Assert.assertTrue(ByteBufferUtil.endsWith(a, b));
            Assert.assertTrue(ByteBufferUtil.endsWith(a, b.slice()));

        }

        a.limit(bytes.length - 1).position(0);
        b.limit(bytes.length - 1).position(1);

        assertFalse(ByteBufferUtil.startsWith(a, b));
        assertFalse(ByteBufferUtil.startsWith(a, b.slice()));

        Assert.assertTrue(ByteBufferUtil.endsWith(a, b));
        Assert.assertTrue(ByteBufferUtil.endsWith(a, b.slice()));


        a.position(5);

        assertFalse(ByteBufferUtil.startsWith(a, b));
        assertFalse(ByteBufferUtil.endsWith(a, b));
    }

    @Test
    public void testWriteZeroes()
    {
        byte[] initial = new byte[1024];
        Arrays.fill(initial, (byte) 1);
        for (ByteBuffer b : new ByteBuffer[] { ByteBuffer.allocate(1024), ByteBuffer.allocateDirect(1024) })
        {
            for (int i = 0; i <= 32; ++i)
                for (int j = 1024; j >= 1024 - 32; --j)
                {
                    b.clear();
                    b.put(initial);
                    b.flip();
                    b.position(i);
                    ByteBufferUtil.writeZeroes(b, j-i);
                    assertEquals(j, b.position());
                    int ii = 0;
                    for (; ii < i; ++ii)
                        assertEquals(initial[ii], b.get(ii));
                    for (; ii < j; ++ii)
                        assertEquals(0, b.get(ii));
                    for (; ii < 1024; ++ii)
                        assertEquals(initial[ii], b.get(ii));

                    b.clear();
                    b.put(initial);
                    b.limit(j).position(i);
                    ByteBuffer slice = b.slice();
                    ByteBufferUtil.writeZeroes(slice, slice.capacity());
                    assertFalse(slice.hasRemaining());
                    b.clear();  // reset position and limit for check
                    ii = 0;
                    for (; ii < i; ++ii)
                        assertEquals(initial[ii], b.get(ii));
                    for (; ii < j; ++ii)
                        assertEquals(0, b.get(ii));
                    for (; ii < 1024; ++ii)
                        assertEquals(initial[ii], b.get(ii));

                    slice.clear();
                    try
                    {
                        ByteBufferUtil.writeZeroes(slice, slice.capacity() + 1);
                        fail("Line above should throw.");
                    }
                    catch (BufferOverflowException | IndexOutOfBoundsException e)
                    {
                        // correct path
                    }
                }
        }
    }
}
