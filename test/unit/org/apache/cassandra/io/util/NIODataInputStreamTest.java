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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;

import static org.junit.Assert.*;

public class NIODataInputStreamTest
{

    Random r;
    ByteBuffer corpus = ByteBuffer.allocate(1024 * 1024 * 8);

    void init()
    {
        long seed = System.nanoTime();
        //seed = 365238103404423L;
        System.out.println("Seed " + seed);
        r = new Random(seed);
        r.nextBytes(corpus.array());
    }

    class FakeChannel implements ReadableByteChannel
    {

        @Override
        public boolean isOpen() { return true; }

        @Override
        public void close() throws IOException {}

        @Override
        public int read(ByteBuffer dst) throws IOException { return 0; }

    }

    class DummyChannel implements ReadableByteChannel
    {

        boolean isOpen = true;
        Queue<ByteBuffer> slices = new ArrayDeque<ByteBuffer>();

        DummyChannel()
        {
            slices.clear();
            corpus.clear();

            while (corpus.hasRemaining())
            {
                int sliceSize = Math.min(corpus.remaining(), r.nextInt(8193));
                corpus.limit(corpus.position() + sliceSize);
                slices.offer(corpus.slice());
                corpus.position(corpus.limit());
                corpus.limit(corpus.capacity());
            }
            corpus.clear();
        }

        @Override
        public boolean isOpen()
        {
            return isOpen();
        }

        @Override
        public void close() throws IOException
        {
            isOpen = false;
        }

        @Override
        public int read(ByteBuffer dst) throws IOException
        {
            if (!isOpen) throw new IOException("closed");
            if (slices.isEmpty()) return -1;

            if (!slices.peek().hasRemaining())
            {
                if (r.nextInt(2) == 1)
                {
                    return 0;
                }
                else
                {
                    slices.poll();
                    if (slices.isEmpty()) return -1;
                }
            }

            ByteBuffer slice = slices.peek();
            int oldLimit = slice.limit();

            int copied = 0;
            if (slice.remaining() > dst.remaining())
            {
                slice.limit(slice.position() + dst.remaining());
                copied = dst.remaining();
            }
            else
            {
                copied = slice.remaining();
            }

            dst.put(slice);
            slice.limit(oldLimit);


            return copied;
        }

    }

    NIODataInputStream fakeStream = new NIODataInputStream(new FakeChannel(), 9);

    @Test(expected = IOException.class)
    public void testResetThrows() throws Exception
    {
        fakeStream.reset();
    }

    @Test(expected = NullPointerException.class)
    public void testNullReadBuffer() throws Exception
    {
        fakeStream.read(null, 0, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeOffsetReadBuffer() throws Exception
    {
        fakeStream.read(new byte[1], -1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNegativeLengthReadBuffer() throws Exception
    {
        fakeStream.read(new byte[1], 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testLengthToBigReadBuffer() throws Exception
    {
        fakeStream.read(new byte[1], 0, 2);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testLengthToWithOffsetBigReadBuffer() throws Exception
    {
        fakeStream.read(new byte[1], 1, 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testReadLine() throws Exception
    {
        fakeStream.readLine();
    }

    @Test
    public void testMarkSupported() throws Exception
    {
        assertFalse(fakeStream.markSupported());
    }

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void testNullRBC() throws Exception
    {
        new NIODataInputStream(null, 9);
    }

    @SuppressWarnings("resource")
    @Test
    public void testAvailable() throws Exception
    {
        init();
        DummyChannel dc = new DummyChannel();
        dc.slices.clear();
        dc.slices.offer(ByteBuffer.allocate(8190));
        NIODataInputStream is = new NIODataInputStream(dc, 4096);
        assertEquals(0, is.available());
        is.read();
        assertEquals(4095, is.available());
        is.read(new byte[4095]);
        assertEquals(0, is.available());
        is.read(new byte[10]);
        assertEquals(8190 - 10 - 4096, is.available());

        File f = File.createTempFile("foo", "bar");
        RandomAccessFile fos = new RandomAccessFile(f, "rw");
        fos.write(new byte[10]);
        fos.seek(0);

        is = new NIODataInputStream(fos.getChannel(), 9);

        int remaining = 10;
        assertEquals(10, is.available());

        while (remaining > 0)
        {
            is.read();
            remaining--;
            assertEquals(remaining, is.available());
        }
        assertEquals(0, is.available());
    }

    private static ReadableByteChannel wrap(final byte bytes[])
    {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        return new ReadableByteChannel()
        {

            @Override
            public boolean isOpen() {return false;}

            @Override
            public void close() throws IOException {}

            @Override
            public int read(ByteBuffer dst) throws IOException
            {
                int read = Math.min(dst.remaining(), buf.remaining());
                buf.limit(buf.position() + read);
                dst.put(buf);
                buf.limit(buf.capacity());
                return read == 0 ? -1 : read;
            }

        };
    }

    @SuppressWarnings("resource")
    @Test
    public void testReadUTF() throws Exception
    {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);

        String simple = "foobar42";

        assertEquals(2, BufferedDataOutputStreamTest.twoByte.getBytes(Charsets.UTF_8).length);
        assertEquals(3, BufferedDataOutputStreamTest.threeByte.getBytes(Charsets.UTF_8).length);
        assertEquals(4, BufferedDataOutputStreamTest.fourByte.getBytes(Charsets.UTF_8).length);

        daos.writeUTF(simple);
        daos.writeUTF(BufferedDataOutputStreamTest.twoByte);
        daos.writeUTF(BufferedDataOutputStreamTest.threeByte);
        daos.writeUTF(BufferedDataOutputStreamTest.fourByte);

        NIODataInputStream is = new NIODataInputStream(wrap(baos.toByteArray()), 4096);

        assertEquals(simple, is.readUTF());
        assertEquals(BufferedDataOutputStreamTest.twoByte, is.readUTF());
        assertEquals(BufferedDataOutputStreamTest.threeByte, is.readUTF());
        assertEquals(BufferedDataOutputStreamTest.fourByte, is.readUTF());
    }

    @SuppressWarnings("resource")
    @Test
    public void testReadVInt() throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStreamPlus daos = new WrappedDataOutputStreamPlus(baos);

        long values[] = new long[] {
                0, 1, -1,
                Long.MIN_VALUE, Long.MIN_VALUE + 1, Long.MAX_VALUE, Long.MAX_VALUE - 1,
                Integer.MIN_VALUE, Integer.MIN_VALUE + 1, Integer.MAX_VALUE, Integer.MAX_VALUE - 1,
                Short.MIN_VALUE, Short.MIN_VALUE + 1, Short.MAX_VALUE, Short.MAX_VALUE - 1,
                Byte.MIN_VALUE, Byte.MIN_VALUE + 1, Byte.MAX_VALUE, Byte.MAX_VALUE - 1 };
        values = BufferedDataOutputStreamTest.enrich(values);

        for (long v : values)
            daos.writeVInt(v);

        daos.flush();

        NIODataInputStream is = new NIODataInputStream(wrap(baos.toByteArray()), 9);

        for (long v : values)
            assertEquals(v, is.readVInt());

        boolean threw = false;
        try
        {
            is.readVInt();
        }
        catch (EOFException e)
        {
            threw = true;
        }
        assertTrue(threw);
    }

    @SuppressWarnings("resource")
    @Test
    public void testReadUnsignedVInt() throws Exception {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStreamPlus daos = new WrappedDataOutputStreamPlus(baos);

        long values[] = new long[] {
                0, 1
                , UnsignedLong.MAX_VALUE.longValue(), UnsignedLong.MAX_VALUE.longValue() - 1, UnsignedLong.MAX_VALUE.longValue() + 1
                , UnsignedInteger.MAX_VALUE.longValue(), UnsignedInteger.MAX_VALUE.longValue() - 1, UnsignedInteger.MAX_VALUE.longValue() + 1
                , UnsignedBytes.MAX_VALUE, UnsignedBytes.MAX_VALUE - 1, UnsignedBytes.MAX_VALUE + 1
                , 65536, 65536 - 1, 65536 + 1 };
        values = BufferedDataOutputStreamTest.enrich(values);

        for (long v : values)
            daos.writeUnsignedVInt(v);

        daos.flush();

        NIODataInputStream is = new NIODataInputStream(wrap(baos.toByteArray()), 9);

        for (long v : values)
            assertEquals(v, is.readUnsignedVInt());

        boolean threw = false;
        try
        {
            is.readUnsignedVInt();
        }
        catch (EOFException e)
        {
            threw = true;
        }
        assertTrue(threw);
    }

    @Test
    public void testFuzz() throws Exception
    {
        for (int ii = 0; ii < 80; ii++)
            fuzzOnce();
    }

    void validateAgainstCorpus(byte bytes[], int offset, int length, int position) throws Exception
    {
        assertEquals(corpus.position(), position);
        int startPosition = corpus.position();
        for (int ii = 0; ii < length; ii++)
        {
            byte expected = corpus.get();
            byte actual = bytes[ii + offset];
            if (expected != actual)
                fail("Mismatch compared to ByteBuffer");
            byte canonical = dis.readByte();
            if (canonical != actual)
                fail("Mismatch compared to DataInputStream");
        }
        assertEquals(length, corpus.position() - startPosition);
    }

    DataInputStream dis;

    @SuppressWarnings({ "resource", "unused" })
    void fuzzOnce() throws Exception
    {
        init();
        int read = 0;
        int totalRead = 0;

        DummyChannel dc = new DummyChannel();
        NIODataInputStream is = new NIODataInputStream( dc, 1024 * 4);
        dis = new DataInputStream(new ByteArrayInputStream(corpus.array()));

        int iteration = 0;
        while (totalRead < corpus.capacity())
        {
            assertEquals(corpus.position(), totalRead);
            int action = r.nextInt(16);

//            System.out.println("Action " + action + " iteration " + iteration + " remaining " + corpus.remaining());
//            if (iteration == 434756) {
//                System.out.println("Here we go");
//            }
            iteration++;

            switch (action) {
            case 0:
            {
                byte bytes[] = new byte[111];

                int expectedBytes = corpus.capacity() - totalRead;
                boolean expectEOF = expectedBytes < 111;
                boolean threwEOF = false;
                try
                {
                    is.readFully(bytes);
                }
                catch (EOFException e)
                {
                    threwEOF = true;
                }

                assertEquals(expectEOF, threwEOF);

                if (expectEOF)
                    return;

                validateAgainstCorpus(bytes, 0, 111, totalRead);

                totalRead += 111;
                break;
            }
            case 1:
            {
                byte bytes[] = new byte[r.nextInt(1024 * 8 + 1)];

                int offset = bytes.length == 0 ? 0 : r.nextInt(bytes.length);
                int length = bytes.length == 0 ? 0 : r.nextInt(bytes.length - offset);
                int expectedBytes = corpus.capacity() - totalRead;
                boolean expectEOF = expectedBytes < length;
                boolean threwEOF = false;
                try {
                    is.readFully(bytes, offset, length);
                }
                catch (EOFException e)
                {
                    threwEOF = true;
                }

                assertEquals(expectEOF, threwEOF);

                if (expectEOF)
                    return;

                validateAgainstCorpus(bytes, offset, length, totalRead);

                totalRead += length;
                break;
            }
            case 2:
            {
                byte bytes[] = new byte[r.nextInt(1024 * 8 + 1)];

                int offset = bytes.length == 0 ? 0 : r.nextInt(bytes.length);
                int length = bytes.length == 0 ? 0 : r.nextInt(bytes.length - offset);
                int expectedBytes = corpus.capacity() - totalRead;
                boolean expectEOF = expectedBytes == 0;
                read = is.read(bytes, offset, length);

                assertTrue((expectEOF && read <= 0) || (!expectEOF && read >= 0));

                if (expectEOF)
                    return;

                validateAgainstCorpus(bytes, offset, read, totalRead);

                totalRead += read;
                break;
            }
            case 3:
            {
                byte bytes[] = new byte[111];

                int expectedBytes = corpus.capacity() - totalRead;
                boolean expectEOF = expectedBytes == 0;
                read = is.read(bytes);

                assertTrue((expectEOF && read <= 0) || (!expectEOF && read >= 0));

                if (expectEOF)
                    return;

                validateAgainstCorpus(bytes, 0, read, totalRead);

                totalRead += read;
                break;
            }
            case 4:
            {
                boolean expected = corpus.get() != 0;
                boolean canonical = dis.readBoolean();
                boolean actual = is.readBoolean();
                assertTrue(expected == canonical && canonical == actual);
                totalRead++;
                break;
            }
            case 5:
            {
                byte expected = corpus.get();
                byte canonical = dis.readByte();
                byte actual = is.readByte();
                assertTrue(expected == canonical && canonical == actual);
                totalRead++;
                break;
            }
            case 6:
            {
                int expected = corpus.get() & 0xFF;
                int canonical = dis.read();
                int actual = is.read();
                assertTrue(expected == canonical && canonical == actual);
                totalRead++;
                break;
            }
            case 7:
            {
                int expected = corpus.get() & 0xFF;
                int canonical = dis.readUnsignedByte();
                int actual = is.readUnsignedByte();
                assertTrue(expected == canonical && canonical == actual);
                totalRead++;
                break;
            }
            case 8:
            {
                if (corpus.remaining() < 2)
                {
                    boolean threw = false;
                    try
                    {
                        is.readShort();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readShort(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 2);
                    totalRead = corpus.capacity();
                    break;
                }
                short expected = corpus.getShort();
                short canonical = dis.readShort();
                short actual = is.readShort();
                assertTrue(expected == canonical && canonical == actual);
                totalRead += 2;
                break;
            }
            case 9:
            {
                if (corpus.remaining() < 2)
                {
                    boolean threw = false;
                    try
                    {
                        is.readUnsignedShort();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readUnsignedShort(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 2);
                    totalRead = corpus.capacity();
                    break;
                }
                int ch1 = corpus.get() & 0xFF;
                int ch2 = corpus.get() & 0xFF;
                int expected = (ch1 << 8) + (ch2 << 0);
                int canonical = dis.readUnsignedShort();
                int actual = is.readUnsignedShort();
                assertTrue(expected == canonical && canonical == actual);
                totalRead += 2;
                break;
            }
            case 10:
            {
                if (corpus.remaining() < 2)
                {
                    boolean threw = false;
                    try
                    {
                        is.readChar();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readChar(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 2);
                    totalRead = corpus.capacity();
                    break;
                }
                char expected = corpus.getChar();
                char canonical = dis.readChar();
                char actual = is.readChar();
                assertTrue(expected == canonical && canonical == actual);
                totalRead += 2;
                break;
            }
            case 11:
            {
                if (corpus.remaining() < 4)
                {
                    boolean threw = false;
                    try
                    {
                        is.readInt();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readInt(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 4);
                    totalRead = corpus.capacity();
                    break;
                }
                int expected = corpus.getInt();
                int canonical = dis.readInt();
                int actual = is.readInt();
                assertTrue(expected == canonical && canonical == actual);
                totalRead += 4;
                break;
            }
            case 12:
            {
                if (corpus.remaining() < 4)
                {
                    boolean threw = false;
                    try
                    {
                        is.readFloat();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readFloat(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 4);
                    totalRead = corpus.capacity();
                    break;
                }
                float expected = corpus.getFloat();
                float canonical = dis.readFloat();
                float actual = is.readFloat();
                totalRead += 4;

                if (Float.isNaN(expected)) {
                    assertTrue(Float.isNaN(canonical) && Float.isNaN(actual));
                } else {
                    assertTrue(expected == canonical && canonical == actual);
                }
                break;
            }
            case 13:
            {
                if (corpus.remaining() < 8)
                {
                    boolean threw = false;
                    try
                    {
                        is.readLong();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readLong(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 8);
                    totalRead = corpus.capacity();
                    break;
                }
                long expected = corpus.getLong();
                long canonical = dis.readLong();
                long actual = is.readLong();

                assertTrue(expected == canonical && canonical == actual);
                totalRead += 8;
                break;
            }
            case 14:
            {
                if (corpus.remaining() < 8)
                {
                    boolean threw = false;
                    try
                    {
                        is.readDouble();
                    }
                    catch (EOFException e)
                    {
                        try { dis.readDouble(); } catch (EOFException e2) {}
                        threw = true;
                    }
                    assertTrue(threw);
                    assertTrue(corpus.remaining() - totalRead < 8);
                    totalRead = corpus.capacity();
                    break;
                }
                double expected = corpus.getDouble();
                double canonical = dis.readDouble();
                double actual = is.readDouble();
                totalRead += 8;

                if (Double.isNaN(expected)) {
                    assertTrue(Double.isNaN(canonical) && Double.isNaN(actual));
                } else {
                    assertTrue(expected == canonical && canonical == actual);
                }
                break;
            }
            case 15:
            {
                int skipBytes = r.nextInt(1024);
                int actuallySkipped =  Math.min(skipBytes, corpus.remaining());

                totalRead += actuallySkipped;
                corpus.position(corpus.position() + actuallySkipped);
                int canonical = dis.skipBytes(actuallySkipped);
                int actual = is.skipBytes(actuallySkipped);
                assertEquals(actuallySkipped, canonical);
                assertEquals(canonical, actual);
                break;
            }
            default:
                fail("Should never reach here");
            }
        }

        assertEquals(totalRead, corpus.capacity());
        assertEquals(-1, dis.read());
    }


    @Test
    @SuppressWarnings({ "resource"})
    public void testVIntRemainingBytes() throws Exception
    {
        for(int ii = 0; ii < 10; ii++)
        {
            for (int zz = 0; zz < 10; zz++)
            {
                if (zz + ii > 10)
                    continue;

                ByteBuffer buf = ByteBuffer.allocate(10);
                buf.position(ii);

                long value = 0;
                if (ii > 0)
                    value = (1L << 7 * zz) - 1;

                BufferedDataOutputStreamPlus out = new DataOutputBufferFixed(buf);
                out.writeUnsignedVInt(value);

                buf.position(ii);
                RebufferingInputStream in = new DataInputBuffer(buf, false);

                assertEquals(value, in.readUnsignedVInt());
            }
        }
    }

    @Test
    @SuppressWarnings({ "resource"})
    public void testVIntSmallBuffer() throws Exception
    {
        for(int ii = 0; ii < 10; ii++)
        {
            ByteBuffer buf = ByteBuffer.allocate(Math.max(1,  ii));

            long value = 0;
            if (ii > 0)
                value = (1L << 7 * ii) - 1;

            BufferedDataOutputStreamPlus out = new DataOutputBufferFixed(buf);
            out.writeUnsignedVInt(value);

            buf.position(0);
            RebufferingInputStream in = new DataInputBuffer(buf, false);

            assertEquals(value, in.readUnsignedVInt());

            boolean threw = false;
            try
            {
                in.readUnsignedVInt();
            }
            catch (EOFException e)
            {
                threw = true;
            }
            assertTrue(threw);
        }
    }

    @Test
    @SuppressWarnings({ "resource"})
    public void testVIntTruncationEOF() throws Exception
    {
        for(int ii = 0; ii < 10; ii++)
        {
            ByteBuffer buf = ByteBuffer.allocate(Math.max(1,  ii));

            long value = 0;
            if (ii > 0)
                value = (1L << 7 * ii) - 1;

            BufferedDataOutputStreamPlus out = new DataOutputBufferFixed(buf);
            out.writeUnsignedVInt(value);

            buf.position(0);

            ByteBuffer truncated = ByteBuffer.allocate(buf.capacity() - 1);
            buf.limit(buf.limit() - 1);
            truncated.put(buf);
            truncated.flip();

            RebufferingInputStream in = new DataInputBuffer(truncated, false);

            boolean threw = false;
            try
            {
                in.readUnsignedVInt();
            }
            catch (EOFException e)
            {
                threw = true;
            }
            assertTrue(threw);
        }
    }
}
