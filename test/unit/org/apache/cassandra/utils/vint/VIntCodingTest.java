/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils.vint;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;

import org.junit.Test;

import org.junit.Assert;

import static org.junit.Assert.fail;

public class VIntCodingTest
{
    private static final long[] LONGS = new long[] {53L, 10201L, 1097151L,
                                                    168435455L, 33251130335L, 3281283447775L,
                                                    417672546086779L, 52057592037927932L, 72057594037927937L};

    @Test
    public void testComputeSize() throws Exception
    {
        assertEncodedAtExpectedSize(0L, 1);

        for (int size = 1 ; size < 8 ; size++)
        {
            assertEncodedAtExpectedSize((1L << 7 * size) - 1, size);
            assertEncodedAtExpectedSize(1L << 7 * size, size + 1);
        }
        Assert.assertEquals(9, VIntCoding.computeUnsignedVIntSize(Long.MAX_VALUE));
    }

    private void assertEncodedAtExpectedSize(long value, int expectedSize) throws Exception
    {
        Assert.assertEquals(expectedSize, VIntCoding.computeUnsignedVIntSize(value));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos);
        VIntCoding.writeUnsignedVInt(value, out);
        out.flush();
        Assert.assertEquals( expectedSize, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(value);
        Assert.assertEquals( expectedSize, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testReadExtraBytesCount()
    {
        for (int i = 1 ; i < 8 ; i++)
            Assert.assertEquals(i, VIntCoding.numberOfExtraBytesToRead((byte) ((0xFF << (8 - i)) & 0xFF)));
    }

    /*
     * Quick sanity check that 1 byte encodes up to 127 as expected
     */
    @Test
    public void testOneByteCapacity() throws Exception {
        int biggestOneByte = 127;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos);
        VIntCoding.writeUnsignedVInt(biggestOneByte, out);
        out.flush();
        Assert.assertEquals( 1, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(biggestOneByte);
        Assert.assertEquals( 1, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testByteBufWithNegativeNumber() throws IOException
    {
        int i = -1231238694;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            VIntCoding.writeUnsignedVInt(i, out);
            long result = VIntCoding.getUnsignedVInt(out.buffer(), 0);
            Assert.assertEquals(i, result);
        }
    }

    @Test
    public void testWriteUnsignedVIntBufferedDOP() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            Assert.assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                VIntCoding.writeUnsignedVInt(val, out);
                // read as ByteBuffer
                Assert.assertEquals(val, VIntCoding.getUnsignedVInt(out.buffer(), 0));
                // read as DataInput
                InputStream is = new ByteArrayInputStream(out.toByteArray());
                Assert.assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputPlus.DataInputStreamPlus(is)));
            }
        }
    }

    @Test
    public void testWriteUnsignedVIntUnbufferedDOP() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            Assert.assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos))
            {
                VIntCoding.writeUnsignedVInt(val, out);
                out.flush();
                Assert.assertEquals( i + 1, baos.toByteArray().length);
                // read as ByteBuffer
                Assert.assertEquals(val, VIntCoding.getUnsignedVInt(ByteBuffer.wrap(baos.toByteArray()), 0));
                // read as DataInput
                InputStream is = new ByteArrayInputStream(baos.toByteArray());
                Assert.assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputPlus.DataInputStreamPlus(is)));
            }
        }
    }

    @Test
    public void testWriteUnsignedVIntBB() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            Assert.assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            ByteBuffer bb = ByteBuffer.allocate(VIntCoding.MAX_SIZE);
            VIntCoding.writeUnsignedVInt(val, bb);
            // read as ByteBuffer
            Assert.assertEquals(val, VIntCoding.getUnsignedVInt(bb, 0));
            // read as DataInput
            InputStream is = new ByteArrayInputStream(bb.array());
            Assert.assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputPlus.DataInputStreamPlus(is)));
        }
    }

    @Test
    public void testWriteUnsignedVIntBBLessThan8Bytes() throws IOException
    {
        long val = 10201L;
        Assert.assertEquals(2, VIntCoding.computeUnsignedVIntSize(val));
        ByteBuffer bb = ByteBuffer.allocate(2);
        VIntCoding.writeUnsignedVInt(val, bb);
        // read as ByteBuffer
        Assert.assertEquals(val, VIntCoding.getUnsignedVInt(bb, 0));
        // read as DataInput
        InputStream is = new ByteArrayInputStream(bb.array());
        Assert.assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputPlus.DataInputStreamPlus(is)));
    }

    @Test
    public void testWriteUnsignedVIntBBHasLessThan8BytesLeft()
    {
        long val = 10201L;
        Assert.assertEquals(2, VIntCoding.computeUnsignedVIntSize(val));
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.position(1);
        VIntCoding.writeUnsignedVInt(val, bb);
        // read as ByteBuffer
        Assert.assertEquals(val, VIntCoding.getUnsignedVInt(bb, 1));
    }

    @Test
    public void testWriteUnsignedVIntBBDoesNotHaveEnoughSpaceOverflows()
    {
        ByteBuffer bb = ByteBuffer.allocate(3);
        try
        {
            VIntCoding.writeUnsignedVInt(52057592037927932L, bb);
            fail();
        } catch (BufferOverflowException e) {}
    }
}
