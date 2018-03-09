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
package org.apache.cassandra.index.sasi.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileUtils;

import org.junit.Assert;
import org.junit.Test;

public class MappedBufferTest
{
    @Test
    public void testBasicWriteThenRead() throws Exception
    {
        long numLongs = 10000;
        final MappedBuffer buffer = createTestFile(numLongs);

        Assert.assertEquals(0, buffer.position());
        for (long i = 0; i < numLongs; i++)
        {
            Assert.assertEquals(i * 8, buffer.position());
            Assert.assertEquals(i, buffer.getLong());
        }

        buffer.position(0);
        for (long i = 0; i < numLongs; i++)
        {
            Assert.assertEquals(i, buffer.getLong(i * 8));
            Assert.assertEquals(0, buffer.position());
        }

        // read all the numbers as shorts (all numbers fit into four bytes)
        for (long i = 0; i < Math.min(Integer.MAX_VALUE, numLongs); i++)
            Assert.assertEquals(i, buffer.getInt((i * 8) + 4));

        // read all the numbers as shorts (all numbers fit into two bytes)
        for (long i = 0; i < Math.min(Short.MAX_VALUE, numLongs); i++) {
            Assert.assertEquals(i, buffer.getShort((i * 8) + 6));
        }

        // read all the numbers that can be represented as a single byte
        for (long i = 0; i < 128; i++)
            Assert.assertEquals(i, buffer.get((i * 8) + 7));

        buffer.close();
    }

    @Test
    public void testDuplicate() throws Exception
    {
        long numLongs = 10;
        final MappedBuffer buffer1 = createTestFile(numLongs);

        Assert.assertEquals(0, buffer1.getLong());
        Assert.assertEquals(1, buffer1.getLong());

        final MappedBuffer buffer2 = buffer1.duplicate();

        Assert.assertEquals(2, buffer1.getLong());
        Assert.assertEquals(2, buffer2.getLong());

        buffer2.position(0);
        Assert.assertEquals(3, buffer1.getLong());
        Assert.assertEquals(0, buffer2.getLong());
    }

    @Test
    public void testLimit() throws Exception
    {
        long numLongs =  10;
        final MappedBuffer buffer1 = createTestFile(numLongs);

        MappedBuffer buffer2 = buffer1.duplicate().position(16).limit(32);
        buffer1.position(0).limit(16);
        List<Long> longs = new ArrayList<>(4);

        while (buffer1.hasRemaining())
            longs.add(buffer1.getLong());

        while (buffer2.hasRemaining())
            longs.add(buffer2.getLong());

        Assert.assertArrayEquals(new Long[]{0L, 1L, 2L, 3L}, longs.toArray());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testPositionGreaterThanLimit() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.limit(4);

        try
        {
            buffer.position(buffer.limit() + 1);
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativePosition() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.position(-1);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitGreaterThanCapacity() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.limit(buffer.capacity() + 1);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLimitLessThanPosition() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.position(1);

        try
        {
            buffer.limit(0);
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetRelativeUnderflow() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.position(buffer.limit());
        try
        {
            buffer.get();
        }
        finally
        {
            buffer.close();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetAbsoluteGreaterThanCapacity() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.get(buffer.limit());
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetAbsoluteNegativePosition() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.get(-1);
        }
    }


    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortRelativeUnderflow() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.position(buffer.capacity() - 1);
        try
        {
            buffer.getShort();
        }
        finally
        {
            buffer.close();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortAbsoluteGreaterThanCapacity() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getShort(buffer.capacity() - 1);
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetShortAbsoluteNegativePosition() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.getShort(-1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntRelativeUnderflow() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.position(buffer.capacity() - 3);
        try
        {
            buffer.getInt();
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntAbsoluteGreaterThanCapacity() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getInt(buffer.capacity() - 3);
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetIntAbsoluteNegativePosition() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.getInt(-1);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongRelativeUnderflow() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        buffer.position(buffer.capacity() - 7);
        try
        {
            buffer.getLong();
        }
        finally
        {
            buffer.close();
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongAbsoluteGreaterThanCapacity() throws Exception
    {
        final MappedBuffer buffer = createTestFile(1);

        Assert.assertEquals(8, buffer.capacity());
        try
        {
            buffer.getLong(buffer.capacity() - 7);
        }
        finally
        {
            buffer.close();
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetLongAbsoluteNegativePosition() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(1))
        {
            buffer.getLong(-1);
        }
    }

    @Test
    public void testGetPageRegion() throws Exception
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        int numLongs = 1000;
        int byteSize = 8;
        int capacity = numLongs * byteSize;
        try (MappedBuffer buffer = createTestFile(numLongs))
        {
            for (int i = 0; i < 1000; i++)
            {
                // offset, length are always aligned on sizeof(long)
                int offset = random.nextInt(0, 1000 * byteSize - byteSize) & ~(byteSize - 1);
                int length = Math.min(capacity, random.nextInt(byteSize, capacity - offset) & ~(byteSize - 1));

                ByteBuffer region = buffer.getPageRegion(offset, length);
                for (int j = offset; j < (offset + length); j += 8)
                    Assert.assertEquals(j / 8, region.getLong(j));
            }
        }
    }

    @Test (expected = IllegalArgumentException.class)
    public void testMisalignedRegionAccess() throws Exception
    {
        try (MappedBuffer buffer = createTestFile(100, 8, 4, 0))
        {
            buffer.getPageRegion(13, 27);
        }
    }

    @Test
    public void testSequentialIterationWithPadding() throws Exception
    {
        long numValues = 1000;
        int maxPageBits = 6; // 64 bytes page
        int[] paddings = new int[] { 0, 3, 5, 7, 9, 11, 13 };

        // test different page sizes, with different padding and types
        for (int numPageBits = 3; numPageBits <= maxPageBits; numPageBits++)
        {
            for (int typeSize = 2; typeSize <= 8; typeSize *= 2)
            {
                for (int padding : paddings)
                {
                    try (MappedBuffer buffer = createTestFile(numValues, typeSize, numPageBits, padding))
                    {
                        long offset = 0;
                        for (long j = 0; j < numValues; j++)
                        {
                            switch (typeSize)
                            {
                                case 2:
                                    Assert.assertEquals(j, buffer.getShort(offset));
                                    break;

                                case 4:
                                    Assert.assertEquals(j, buffer.getInt(offset));
                                    break;

                                case 8:
                                    Assert.assertEquals(j, buffer.getLong(offset));
                                    break;

                                default:
                                    throw new AssertionError();
                            }

                            offset += typeSize + padding;
                        }
                    }
                }
            }
        }
    }

    @Test
    public void testSequentialIteration() throws IOException
    {
        long numValues = 1000;
        for (int typeSize = 2; typeSize <= 8; typeSize *= 2)
        {
            try (MappedBuffer buffer = createTestFile(numValues, typeSize, 16, 0))
            {
                for (int j = 0; j < numValues; j++)
                {
                    Assert.assertEquals(j * typeSize, buffer.position());

                    switch (typeSize)
                    {
                        case 2:
                            Assert.assertEquals(j, buffer.getShort());
                            break;

                        case 4:
                            Assert.assertEquals(j, buffer.getInt());
                            break;

                        case 8:
                            Assert.assertEquals(j, buffer.getLong());
                            break;

                        default:
                            throw new AssertionError();
                    }
                }
            }
        }
    }

    @Test
    public void testCompareToPage() throws IOException
    {
        long numValues = 100;
        int typeSize = 8;

        try (MappedBuffer buffer = createTestFile(numValues))
        {
            for (long i = 0; i < numValues * typeSize; i += typeSize)
            {
                long value = i / typeSize;
                Assert.assertEquals(0, buffer.comparePageTo(i, typeSize, LongType.instance, LongType.instance.decompose(value)));
            }
        }
    }

    @Test
    public void testOpenWithoutPageBits() throws IOException
    {
        File tmp = File.createTempFile("mapped-buffer", "tmp");
        tmp.deleteOnExit();

        RandomAccessFile file = new RandomAccessFile(tmp, "rw");

        long numValues = 1000;
        for (long i = 0; i < numValues; i++)
            file.writeLong(i);

        file.getFD().sync();

        try (MappedBuffer buffer = new MappedBuffer(new ChannelProxy(tmp.getAbsolutePath(), file.getChannel())))
        {
            Assert.assertEquals(numValues * 8, buffer.limit());
            Assert.assertEquals(numValues * 8, buffer.capacity());

            for (long i = 0; i < numValues; i++)
            {
                Assert.assertEquals(i * 8, buffer.position());
                Assert.assertEquals(i, buffer.getLong());
            }
        }
        finally
        {
            FileUtils.closeQuietly(file);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testIncorrectPageSize() throws Exception
    {
        new MappedBuffer(null, 33);
    }

    private MappedBuffer createTestFile(long numCount) throws IOException
    {
        return createTestFile(numCount, 8, 16, 0);
    }

    private MappedBuffer createTestFile(long numCount, int typeSize, int numPageBits, int padding) throws IOException
    {
        final File testFile = File.createTempFile("mapped-buffer-test", "db");
        testFile.deleteOnExit();

        RandomAccessFile file = new RandomAccessFile(testFile, "rw");

        for (long i = 0; i < numCount; i++)
        {

            switch (typeSize)
            {
                case 1:
                    file.write((byte) i);
                    break;

                case 2:
                    file.writeShort((short) i);
                    break;

                case 4:
                    file.writeInt((int) i);
                    break;

                case 8:
                    // bunch of longs
                    file.writeLong(i);
                    break;

                default:
                    throw new IllegalArgumentException("unknown byte size: " + typeSize);
            }

            for (int j = 0; j < padding; j++)
                file.write(0);
        }

        file.getFD().sync();

        try
        {
            return new MappedBuffer(new ChannelProxy(testFile.getAbsolutePath(), file.getChannel()), numPageBits);
        }
        finally
        {
            FileUtils.closeQuietly(file);
        }
    }

}
