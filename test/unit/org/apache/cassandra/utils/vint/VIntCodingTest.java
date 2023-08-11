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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import org.apache.cassandra.db.marshal.ByteArrayAccessor;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.WrappedDataOutputStreamPlus;
import org.apache.cassandra.utils.CassandraUInt;
import org.assertj.core.api.Assertions;
import org.quicktheories.generators.SourceDSL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.quicktheories.QuickTheory.qt;

public class VIntCodingTest
{
    private static final long[] LONGS = new long[] {53L, 10201L, 1097151L,
                                                    168435455L, 33251130335L, 3281283447775L,
                                                    417672546086779L, 52057592037927932L, 72057594037927937L};

    @Test
    public void serdeSigned()
    {
        qt().forAll(SourceDSL.longs().all()).checkAssert(l -> {
            for (ValueAccessor<?> accessor : Arrays.asList(ByteBufferAccessor.instance, ByteArrayAccessor.instance))
                serdeSigned(l, accessor);
        });
    }

    private static <T> void serdeSigned(long value, ValueAccessor<T> accessor)
    {
        T buffer = accessor.allocate(Long.BYTES + 1);
        VIntCoding.writeVInt(value, buffer, 0, accessor);
        Assertions.assertThat(VIntCoding.getVInt(buffer, accessor, 0)).isEqualTo(value);
    }

    @Test
    public void serdeUnsigned()
    {
        qt().forAll(SourceDSL.longs().between(0, Long.MAX_VALUE)).checkAssert(l -> {
            for (ValueAccessor<?> accessor : Arrays.asList(ByteBufferAccessor.instance, ByteArrayAccessor.instance))
                serdeUnsigned(l, accessor);
        });
    }

    private static <T> void serdeUnsigned(long value, ValueAccessor<T> accessor)
    {
        T buffer = accessor.allocate(Long.BYTES + 1);
        VIntCoding.writeUnsignedVInt(value, buffer, 0, accessor);
        Assertions.assertThat(VIntCoding.getUnsignedVInt(buffer, accessor, 0)).isEqualTo(value);
    }

    @Test
    public void serdeSigned32()
    {
        qt().forAll(SourceDSL.integers().all()).checkAssert(l -> {
            for (ValueAccessor<?> accessor : Arrays.asList(ByteBufferAccessor.instance, ByteArrayAccessor.instance))
                serdeSigned32(l, accessor);
        });
    }

    private static <T> void serdeSigned32(int value, ValueAccessor<T> accessor)
    {
        T buffer = accessor.allocate(Integer.BYTES + 1);
        VIntCoding.writeVInt32(value, buffer, 0, accessor);
        Assertions.assertThat(VIntCoding.getVInt32(buffer, accessor, 0)).isEqualTo(value);
    }

    @Test
    public void serdeUnsignedInt32()
    {
        qt().forAll(SourceDSL.integers().between(0, Integer.MAX_VALUE)).checkAssert(l -> {
            for (ValueAccessor<?> accessor : Arrays.asList(ByteBufferAccessor.instance, ByteArrayAccessor.instance))
                serdeUnsignedInt32(l, accessor);
        });
    }

    private static <T> void serdeUnsignedInt32(int value, ValueAccessor<T> accessor)
    {
        T buffer = accessor.allocate(Integer.BYTES + 1);
        VIntCoding.writeUnsignedVInt32(value, buffer, 0, accessor);
        Assertions.assertThat(VIntCoding.getUnsignedVInt32(buffer, accessor, 0)).isEqualTo(value);
    }

    @Test
    public void testComputeSize() throws Exception
    {
        assertEncodedAtExpectedSize(0L, 1);

        for (int size = 1 ; size < 8 ; size++)
        {
            assertEncodedAtExpectedSize((1L << 7 * size) - 1, size);
            assertEncodedAtExpectedSize(1L << 7 * size, size + 1);
        }
        assertEquals(9, VIntCoding.computeUnsignedVIntSize(Long.MAX_VALUE));
    }

    private void assertEncodedAtExpectedSize(long value, int expectedSize) throws Exception
    {
        assertEquals(expectedSize, VIntCoding.computeUnsignedVIntSize(value));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos);
        VIntCoding.writeUnsignedVInt(value, out);
        out.flush();
        assertEquals( expectedSize, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt(value);
        assertEquals( expectedSize, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testReadExtraBytesCount()
    {
        for (int i = 1 ; i < 8 ; i++)
            assertEquals(i, VIntCoding.numberOfExtraBytesToRead((byte) ((0xFF << (8 - i)) & 0xFF)));
    }

    /*
     * Quick sanity check that 1 byte encodes up to 127 as expected
     */
    @Test
    public void testOneByteCapacity() throws Exception {
        int biggestOneByte = 127;

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos);
        VIntCoding.writeUnsignedVInt32(biggestOneByte, out);
        out.flush();
        assertEquals( 1, baos.toByteArray().length);

        DataOutputBuffer dob = new DataOutputBuffer();
        dob.writeUnsignedVInt32(biggestOneByte);
        assertEquals( 1, dob.buffer().remaining());
        dob.close();
    }

    @Test
    public void testByteBufWithNegativeNumber() throws IOException
    {
        int i = -1231238694;
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            VIntCoding.writeUnsignedVInt32(i, out);
            long result = VIntCoding.getUnsignedVInt(out.buffer(), 0);
            assertEquals(i, result);
        }
    }

    @Test
    public void testWriteUnsignedVIntBufferedDOP() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            try (DataOutputBuffer out = new DataOutputBuffer())
            {
                VIntCoding.writeUnsignedVInt(val, out);
                // read as ByteBuffer
                assertEquals(val, VIntCoding.getUnsignedVInt(out.buffer(), 0));
                // read as DataInput
                assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputBuffer(out.toByteArray())));
            }
        }
    }

    @Test
    public void testWriteUnsignedVIntUnbufferedDOP() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (WrappedDataOutputStreamPlus out = new WrappedDataOutputStreamPlus(baos))
            {
                VIntCoding.writeUnsignedVInt(val, out);
                out.flush();
                assertEquals( i + 1, baos.toByteArray().length);
                // read as ByteBuffer
                assertEquals(val, VIntCoding.getUnsignedVInt(ByteBuffer.wrap(baos.toByteArray()), 0));
                // read as DataInput
                assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputBuffer(baos.toByteArray())));
            }
        }
    }

    @Test
    public void testWriteUnsignedVIntBB() throws IOException
    {
        for (int i = 0; i < VIntCoding.MAX_SIZE - 1; i++)
        {
            long val = LONGS[i];
            assertEquals(i + 1, VIntCoding.computeUnsignedVIntSize(val));
            ByteBuffer bb = ByteBuffer.allocate(VIntCoding.MAX_SIZE);
            VIntCoding.writeUnsignedVInt(val, bb);
            // read as ByteBuffer
            assertEquals(val, VIntCoding.getUnsignedVInt(bb, 0));
            // read as DataInput
            assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputBuffer(bb.array())));
        }
    }

    @Test
    public void testWriteUnsignedVIntBBLessThan8Bytes() throws IOException
    {
        long val = 10201L;
        assertEquals(2, VIntCoding.computeUnsignedVIntSize(val));
        ByteBuffer bb = ByteBuffer.allocate(2);
        VIntCoding.writeUnsignedVInt(val, bb);
        // read as ByteBuffer
        assertEquals(val, VIntCoding.getUnsignedVInt(bb, 0));
        // read as DataInput
        assertEquals(val, VIntCoding.readUnsignedVInt(new DataInputBuffer(bb.array())));
    }

    @Test
    public void testWriteUnsignedVIntBBHasLessThan8BytesLeft()
    {
        long val = 10201L;
        assertEquals(2, VIntCoding.computeUnsignedVIntSize(val));
        ByteBuffer bb = ByteBuffer.allocate(3);
        bb.position(1);
        VIntCoding.writeUnsignedVInt(val, bb);
        // read as ByteBuffer
        assertEquals(val, VIntCoding.getUnsignedVInt(bb, 1));
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

    static int[] roundtripTestValues =  new int[] {
            CassandraUInt.MAX_VALUE_UINT,
            Integer.MAX_VALUE + 1,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE - 1,
            Integer.MIN_VALUE,
            Integer.MIN_VALUE + 1,
            Integer.MIN_VALUE - 1,
            0,
            -1,
            1
    };

    @Test
    public void testRoundtripUnsignedVInt32() throws Throwable
    {
        for (int value : roundtripTestValues)
            testRoundtripUnsignedVInt32(value);
    }

    private static void testRoundtripUnsignedVInt32(int value) throws Throwable
    {
        ByteBuffer bb = ByteBuffer.allocate(9);
        VIntCoding.writeUnsignedVInt32(value, bb);
        bb.flip();
        assertEquals(value, VIntCoding.getUnsignedVInt32(bb, 0));

        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            dob.writeUnsignedVInt32(value);
            try (DataInputBuffer dib = new DataInputBuffer(dob.buffer(), false))
            {
                assertEquals(value, dib.readUnsignedVInt32());
            }
        }
    }

    @Test
    public void testRoundtripVInt32() throws Throwable
    {
        for (int value : roundtripTestValues)
            testRoundtripVInt32(value);
    }

    private static void testRoundtripVInt32(int value) throws Throwable
    {
        ByteBuffer bb = ByteBuffer.allocate(9);

        VIntCoding.writeVInt32(value, bb);
        bb.flip();
        assertEquals(value, VIntCoding.getVInt32(bb, 0));

        try (DataOutputBuffer dob = new DataOutputBuffer())
        {
            dob.writeVInt32(value);
            try (DataInputBuffer dib = new DataInputBuffer(dob.buffer(), false))
            {
                assertEquals(value, dib.readVInt32());
            }
        }
    }
}
