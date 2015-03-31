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

import org.apache.cassandra.io.util.NIODataInputStream;
import org.junit.Test;

import com.google.common.base.Charsets;

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

    NIODataInputStream fakeStream = new NIODataInputStream(new FakeChannel(), 8);

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
    @Test(expected = IllegalArgumentException.class)
    public void testTooSmallBufferSize() throws Exception
    {
        new NIODataInputStream(new FakeChannel(), 4);
    }

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void testNullRBC() throws Exception
    {
        new NIODataInputStream(null, 8);
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

        is = new NIODataInputStream(fos.getChannel(), 8);

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

    @SuppressWarnings("resource")
    @Test
    public void testReadUTF() throws Exception
    {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);

        String simple = "foobar42";
        String twoByte = "ƀ";
        String threeByte = "㒨";
        String fourByte = "𠝹";

        assertEquals(2, twoByte.getBytes(Charsets.UTF_8).length);
        assertEquals(3, threeByte.getBytes(Charsets.UTF_8).length);
        assertEquals(4, fourByte.getBytes(Charsets.UTF_8).length);

        daos.writeUTF(simple);
        daos.writeUTF(twoByte);
        daos.writeUTF(threeByte);
        daos.writeUTF(fourByte);

        NIODataInputStream is = new NIODataInputStream(new ReadableByteChannel()
        {

            @Override
            public boolean isOpen() {return false;}

            @Override
            public void close() throws IOException {}

            @Override
            public int read(ByteBuffer dst) throws IOException
            {
                dst.put(baos.toByteArray());
                return baos.toByteArray().length;
            }

        }, 4096);

        assertEquals(simple, is.readUTF());
        assertEquals(twoByte, is.readUTF());
        assertEquals(threeByte, is.readUTF());
        assertEquals(fourByte, is.readUTF());
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
}
