/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.cassandra.io.util;

import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLong;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraUInt;
import org.apache.cassandra.utils.vint.VIntCoding;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Field;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.Random;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.FBUtilities.preventIllegalAccessWarnings;
import static org.junit.Assert.*;

public class BufferedDataOutputStreamTest
{
    static
    {
        preventIllegalAccessWarnings();
    }

    @Test(expected = BufferOverflowException.class)
    public void testDataOutputBufferFixedByes() throws Exception
    {
        try (DataOutputBufferFixed dob = new DataOutputBufferFixed())
        {
            try
            {
                for (int ii = 0; ii < 128; ii++)
                    dob.write(0);
            }
            catch (BufferOverflowException e)
            {
                fail("Should not throw BufferOverflowException yet");
            }
            dob.write(0);
        }
    }

    @Test(expected = BufferOverflowException.class)
    public void testDataOutputBufferFixedByteBuffer() throws Exception
    {
        try (DataOutputBufferFixed dob = new DataOutputBufferFixed())
        {
            try
            {
                dob.write(ByteBuffer.allocateDirect(128));
            }
            catch (BufferOverflowException e)
            {
                fail("Should not throw BufferOverflowException yet");
            }
            dob.write(ByteBuffer.allocateDirect(1));
        }
    }

    WritableByteChannel adapter = new WritableByteChannel()
    {

        @Override
        public boolean isOpen()  {return true;}

        @Override
        public void close() throws IOException {}

        @Override
        public int write(ByteBuffer src) throws IOException
        {
            int retval = src.remaining();
            while (src.hasRemaining())
                generated.write(src.get());
            return retval;
        }

    };

    BufferedDataOutputStreamPlus fakeStream = new BufferedDataOutputStreamPlus(adapter, 8);

    @Test(expected = NullPointerException.class)
    public void testNullChannel()
    {
        new BufferedDataOutputStreamPlus((WritableByteChannel)null, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTooSmallBuffer()
    {
        new BufferedDataOutputStreamPlus(adapter, 7);
    }

    @Test(expected = NullPointerException.class)
    public void testNullBuffer() throws Exception
    {
        byte type[] = null;
        fakeStream.write(type, 0, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, -1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeLength() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigLength() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 0, 11);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigLengthWithOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 8, 3);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testTooBigOffset() throws Exception
    {
        byte type[] = new byte[10];
        fakeStream.write(type, 11, 1);
    }

    static final Random r;

    static Field baos_bytes;
    static {
        long seed = nanoTime();
        //seed = 210187780999648L;
        System.out.println("Seed " + seed);
        r = new Random(seed);
        try
        {
            baos_bytes = ByteArrayOutputStream.class.getDeclaredField("buf");
            baos_bytes.setAccessible(true);
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    private ByteArrayOutputStream generated;
    private BufferedDataOutputStreamPlus ndosp;

    private ByteArrayOutputStream canonical;
    private DataOutputStreamPlus dosp;

    void setUp() throws Exception
    {

        generated = new ByteArrayOutputStream();
        canonical = new ByteArrayOutputStream();
        dosp = new WrappedDataOutputStreamPlus(canonical);
        if (ndosp != null)
            ndosp.close();
        ndosp = new BufferedDataOutputStreamPlus(adapter, 4096);
    }

    @Test
    public void testFuzz() throws Exception
    {
        for (int ii = 0; ii < 30; ii++)
            fuzzOnce();
    }

    String simple = "foobar42";
    public static final String twoByte = "\u0180";
    public static final String threeByte = "\u34A8";
    public static final String fourByte = "\uD841\uDF79";

    @SuppressWarnings("unused")
    private void fuzzOnce() throws Exception
    {
        setUp();
        int iteration = 0;
        int bytesChecked = 0;
        int action = 0;
        while (generated.size() < 1024 * 1024 * 8)
        {
            action = r.nextInt(21);

            //System.out.println("Action " + action + " iteration " + iteration);
            iteration++;

            switch (action)
            {
            case 0:
            {
                generated.flush();
                dosp.flush();
                break;
            }
            case 1:
            {
                int val = r.nextInt();
                dosp.write(val);
                ndosp.write(val);
                break;
            }
            case 2:
            {
                byte randomBytes[] = new byte[r.nextInt(4096 * 2 + 1)];
                r.nextBytes(randomBytes);
                dosp.write(randomBytes);
                ndosp.write(randomBytes);
                break;
            }
            case 3:
            {
                byte randomBytes[] = new byte[r.nextInt(4096 * 2 + 1)];
                r.nextBytes(randomBytes);
                int offset = randomBytes.length == 0 ? 0 : r.nextInt(randomBytes.length);
                int length = randomBytes.length == 0 ? 0 : r.nextInt(randomBytes.length - offset);
                dosp.write(randomBytes, offset, length);
                ndosp.write(randomBytes, offset, length);
                break;
            }
            case 4:
            {
                boolean val = r.nextInt(2) == 0;
                dosp.writeBoolean(val);
                ndosp.writeBoolean(val);
                break;
            }
            case 5:
            {
                int val = r.nextInt();
                dosp.writeByte(val);
                ndosp.writeByte(val);
                break;
            }
            case 6:
            {
                int val = r.nextInt();
                dosp.writeShort(val);
                ndosp.writeShort(val);
                break;
            }
            case 7:
            {
                int val = r.nextInt();
                dosp.writeChar(val);
                ndosp.writeChar(val);
                break;
            }
            case 8:
            {
                int val = r.nextInt();
                dosp.writeInt(val);
                ndosp.writeInt(val);
                break;
            }
            case 9:
            {
                int val = r.nextInt();
                dosp.writeLong(val);
                ndosp.writeLong(val);
                break;
            }
            case 10:
            {
                float val = r.nextFloat();
                dosp.writeFloat(val);
                ndosp.writeFloat(val);
                break;
            }
            case 11:
            {
                double val = r.nextDouble();
                dosp.writeDouble(val);
                ndosp.writeDouble(val);
                break;
            }
            case 12:
            {
                dosp.writeBytes(simple);
                ndosp.writeBytes(simple);
                break;
            }
            case 13:
            {
                dosp.writeChars(twoByte);
                ndosp.writeChars(twoByte);
                break;
            }
            case 14:
            {
                StringBuilder sb = new StringBuilder();
                int length = r.nextInt(500);
                //Some times do big strings
                if (r.nextDouble() > .95)
                    length += 4000;
                sb.append(simple + twoByte + threeByte + fourByte);
                for (int ii = 0; ii < length; ii++)
                {
                    sb.append((char)(r.nextInt() & 0xffff));
                }
                String str = sb.toString();
                writeUTFLegacy(str, dosp);
                ndosp.writeUTF(str);
                break;
            }
            case 15:
            {
                StringBuilder sb = new StringBuilder();
                int length = r.nextInt(500);
                sb.append("the very model of a modern major general familiar with all things animal vegetable and mineral");
                for (int ii = 0; ii < length; ii++)
                {
                    sb.append(' ');
                }
                String str = sb.toString();
                writeUTFLegacy(str, dosp);
                ndosp.writeUTF(str);
                break;
            }
            case 16:
            {
                ByteBuffer buf = ByteBuffer.allocate(r.nextInt(1024 * 8 + 1));
                r.nextBytes(buf.array());
                buf.position(buf.capacity() == 0 ? 0 : r.nextInt(buf.capacity()));
                buf.limit(buf.position() + (buf.capacity() - buf.position() == 0 ? 0 : r.nextInt(buf.capacity() - buf.position())));
                ByteBuffer dup = buf.duplicate();
                ndosp.write(buf.duplicate());
                assertEquals(dup.position(), buf.position());
                assertEquals(dup.limit(), buf.limit());
                dosp.write(buf.duplicate());
                break;
            }
            case 17:
            {
                ByteBuffer buf = ByteBuffer.allocateDirect(r.nextInt(1024 * 8 + 1));
                while (buf.hasRemaining())
                    buf.put((byte)r.nextInt());
                buf.position(buf.capacity() == 0 ? 0 : r.nextInt(buf.capacity()));
                buf.limit(buf.position() + (buf.capacity() - buf.position() == 0 ? 0 : r.nextInt(buf.capacity() - buf.position())));
                ByteBuffer dup = buf.duplicate();
                ndosp.write(buf.duplicate());
                assertEquals(dup.position(), buf.position());
                assertEquals(dup.limit(), buf.limit());
                dosp.write(buf.duplicate());
                break;
            }
            case 18:
            {
                try (Memory buf = Memory.allocate(r.nextInt(1024 * 8 - 1) + 1);)
                {
                    for (int ii = 0; ii < buf.size(); ii++)
                        buf.setByte(ii, (byte)r.nextInt());
                    long offset = buf.size() == 0 ? 0 : r.nextInt((int)buf.size());
                    long length = (buf.size() - offset == 0 ? 0 : r.nextInt((int)(buf.size() - offset)));
                    ndosp.write(buf, offset, length);
                    dosp.write(buf, offset, length);
                }
                break;
            }
            case 19:
            {
                long val = r.nextLong();
                VIntCoding.writeVInt(val, dosp);
                ndosp.writeVInt(val);
                break;
            }
            case 20:
            {
                long val = r.nextLong();
                VIntCoding.writeUnsignedVInt(val, dosp);
                ndosp.writeUnsignedVInt(val);
                break;
            }
            default:
                fail("Shouldn't reach here");
            }
            //bytesChecked = assertSameOutput(bytesChecked, action, iteration);
        }

        assertSameOutput(0, -1, iteration);
    }

    public static void writeUTFLegacy(String str, OutputStream out) throws IOException
    {
        int utfCount = 0, length = str.length();
        for (int i = 0; i < length; i++)
        {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127)
            {
                utfCount++;
            }
            else if (charValue <= 2047)
            {
                utfCount += 2;
            }
            else
            {
                utfCount += 3;
            }
        }
        if (utfCount > 65535)
        {
            throw new UTFDataFormatException(); //$NON-NLS-1$
        }
        byte utfBytes[] = new byte[utfCount + 2];
        int utfIndex = 2;
        for (int i = 0; i < length; i++)
        {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127)
            {
                utfBytes[utfIndex++] = (byte) charValue;
            }
            else if (charValue <= 2047)
            {
                utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
            else
            {
                utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        utfBytes[0] = (byte) (utfCount >> 8);
        utfBytes[1] = (byte) utfCount;
        out.write(utfBytes);
    }

    private int assertSameOutput(int bytesChecked, int lastAction, int iteration) throws Exception
    {
        ndosp.flush();
        dosp.flush();

        byte generatedBytes[] = (byte[])baos_bytes.get(generated);
        byte canonicalBytes[] = (byte[])baos_bytes.get(canonical);

        int count = generated.size();
        if (count != canonical.size())
            System.out.println("Failed at " + bytesChecked + " last action " + lastAction + " iteration " + iteration);
        assertEquals(count, canonical.size());
        for (;bytesChecked < count; bytesChecked++)
        {
            byte generatedByte = generatedBytes[bytesChecked];
            byte canonicalByte = canonicalBytes[bytesChecked];
            if (generatedByte != canonicalByte)
                System.out.println("Failed at " + bytesChecked + " last action " + lastAction + " iteration " + iteration);
            assertEquals(generatedByte, canonicalByte);
        }
        return count;
    }

    @Test
    public void testWriteUTF() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(baos);

        StringBuilder sb = new StringBuilder(65535);
        for (int ii = 0; ii < 1 << 16; ii++)
        {
            String s = sb.toString();
            UnbufferedDataOutputStreamPlus.writeUTF(s, dataOut);
            DataInput dataIn = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            assertEquals(s, dataIn.readUTF());
            baos.reset();
            sb.append("a");
        }
    }

    @Test
    public void testWriteUTFBigChar() throws Exception
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput dataOut = new DataOutputStream(baos);

        StringBuilder sb = new StringBuilder(65535);
        for (int ii = 0; ii < 1 << 15; ii++)
        {
            String s = sb.toString();
            UnbufferedDataOutputStreamPlus.writeUTF(s, dataOut);
            DataInput dataIn = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
            assertEquals(s, dataIn.readUTF());
            baos.reset();
            if (ii == (1 << 15) - 1)
                sb.append("a");
            else
                sb.append(twoByte);
        }
    }

    /*
     * Add values to the array with a bit set in every position
     */
    public static long[] enrich(long vals[])
    {
        long retval[] = Arrays.copyOf(vals, vals.length + 64);
        for (int ii = 0; ii < 64; ii++)
            retval[vals.length + ii] = 1L << ii;
        return retval;
   }

    @Test
    public void testVInt() throws Exception
    {
        setUp();
        long testValues[] = new long[] {
                0, 1, -1
                ,Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MAX_VALUE - 1
                ,Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE, Integer.MAX_VALUE - 1
                ,Short.MIN_VALUE, Short.MIN_VALUE + 1, Short.MAX_VALUE, Short.MAX_VALUE - 1
                ,Byte.MIN_VALUE, Byte.MIN_VALUE + 1, Byte.MAX_VALUE, Byte.MAX_VALUE - 1 };
        testValues = enrich(testValues);

        int expectedSize = 0;
        for (long v : testValues)
        {
            expectedSize += VIntCoding.computeVIntSize(v);
            ndosp.writeVInt(v);
        }

        ndosp.flush();

        DataInputBuffer in = new DataInputBuffer(generated.toByteArray());
        assertEquals(expectedSize, generated.toByteArray().length);

        for (long v : testValues)
        {
            assertEquals(v, in.readVInt());
        }
    }

    @Test
    public void testUnsignedVInt() throws Exception
    {
        setUp();
        long testValues[] = new long[] { //-1 };
                0, 1
                , UnsignedLong.MAX_VALUE.longValue(), UnsignedLong.MAX_VALUE.longValue() - 1, UnsignedLong.MAX_VALUE.longValue() + 1
                , CassandraUInt.MAX_VALUE_LONG, CassandraUInt.MAX_VALUE_LONG - 1, CassandraUInt.MAX_VALUE_LONG + 1
                , UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE - 1, UnsignedBytes.MAX_VALUE + 1
                , 65536, 65536 - 1, 65536 + 1 };
        testValues = enrich(testValues);

        int expectedSize = 0;
        for (long v : testValues)
        {
            expectedSize += VIntCoding.computeUnsignedVIntSize(v);
            ndosp.writeUnsignedVInt(v);
        }

        ndosp.flush();

        DataInputBuffer in = new DataInputBuffer(generated.toByteArray());
        assertEquals(expectedSize, generated.toByteArray().length);

        for (long v : testValues)
            assertEquals(v, in.readUnsignedVInt());
    }

    @Test
    public void testWriteSlowByteOrder() throws Exception
    {
        try (DataOutputBuffer dob = new DataOutputBuffer(4))
        {
            dob.order(ByteOrder.LITTLE_ENDIAN);
            dob.writeLong(42);
            assertEquals(42, ByteBuffer.wrap(dob.toByteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong());
        }
    }

    @Test
    public void testWriteBytes() throws Exception
    {
        setUp();
        DataOutputStreamPlus dosp = new BufferedDataOutputStreamPlus(adapter, 8);
        for (int i = 0; i < 1000; i++)
        {
            long val = r.nextLong();
            int size = r.nextInt(9);
            byte[] bytes = ByteBufferUtil.bytes(val).array();
            canonical.write(bytes, 0, size);
            dosp.writeMostSignificantBytes(val, size);
        }
        dosp.flush();
        assertArrayEquals(canonical.toByteArray(), generated.toByteArray());
    }
}
