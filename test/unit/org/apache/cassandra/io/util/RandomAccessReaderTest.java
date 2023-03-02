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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RandomAccessReaderTest
{
    private static final Logger logger = LoggerFactory.getLogger(RandomAccessReaderTest.class);

    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    private static final class Parameters
    {
        final long fileLength;
        final int bufferSize;

        BufferType bufferType;
        int maxSegmentSize;
        boolean mmappedRegions;
        public byte[] expected;

        Parameters(long fileLength, int bufferSize)
        {
            this.fileLength = fileLength;
            this.bufferSize = bufferSize;
            this.bufferType = BufferType.OFF_HEAP;
            this.maxSegmentSize = MmappedRegions.MAX_SEGMENT_SIZE;
            this.mmappedRegions = false;
            this.expected = "The quick brown fox jumps over the lazy dog".getBytes(FileUtils.CHARSET);
        }

        Parameters mmappedRegions(boolean mmappedRegions)
        {
            this.mmappedRegions = mmappedRegions;
            return this;
        }

        Parameters bufferType(BufferType bufferType)
        {
            this.bufferType = bufferType;
            return this;
        }

        Parameters maxSegmentSize(int maxSegmentSize)
        {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }
    }

    @Test
    public void testBufferedOffHeap() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).bufferType(BufferType.OFF_HEAP));
    }

    @Test
    public void testBufferedOnHeap() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testBigBufferSize() throws IOException
    {
        testReadFully(new Parameters(8192, 65536).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testTinyBufferSize() throws IOException
    {
        testReadFully(new Parameters(8192, 16).bufferType(BufferType.ON_HEAP));
    }

    @Test
    public void testOneSegment() throws IOException
    {
        testReadFully(new Parameters(8192, 4096).mmappedRegions(true));
    }

    @Test
    public void testMultipleSegments() throws IOException
    {
        // FIXME: This is the same as above.
        testReadFully(new Parameters(8192, 4096).mmappedRegions(true).maxSegmentSize(1024));
    }

    @Test
    public void testVeryLarge() throws IOException
    {
        final long SIZE = 1L << 32; // 2GiB
        Parameters params = new Parameters(SIZE, 1 << 20); // 1MiB


        try (FileHandle fh = new FileHandle.Builder(new File("abc")).bufferType(params.bufferType)
                                                                    .bufferSize(params.bufferSize)
                                                                    .complete(f -> new ChannelProxy(f, new FakeFileChannel(SIZE)));
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(fh.channel.size(), reader.length());
            assertEquals(fh.channel.size(), reader.bytesRemaining());
            assertEquals(Integer.MAX_VALUE, reader.available());

            assertEquals(fh.channel.size(), reader.skip(fh.channel.size()));

            assertTrue(reader.isEOF());
            assertEquals(0, reader.bytesRemaining());
        }
    }

    /** A fake file channel that simply increments the position and doesn't
     * actually read anything. We use it to simulate very large files, > 2G.
     */
    private static final class FakeFileChannel extends FileChannel
    {
        private final long size;
        private long position;

        FakeFileChannel(long size)
        {
            this.size = size;
        }

        public int read(ByteBuffer dst)
        {
            int ret = dst.remaining();
            position += ret;
            dst.position(dst.limit());
            return ret;
        }

        public long read(ByteBuffer[] dsts, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        public int write(ByteBuffer src)
        {
            throw new UnsupportedOperationException();
        }

        public long write(ByteBuffer[] srcs, int offset, int length)
        {
            throw new UnsupportedOperationException();
        }

        public long position()
        {
            return position;
        }

        public FileChannel position(long newPosition)
        {
            position = newPosition;
            return this;
        }

        public long size()
        {
            return size;
        }

        public FileChannel truncate(long size)
        {
            throw new UnsupportedOperationException();
        }

        public void force(boolean metaData)
        {
            throw new UnsupportedOperationException();
        }

        public long transferTo(long position, long count, WritableByteChannel target)
        {
            throw new UnsupportedOperationException();
        }

        public long transferFrom(ReadableByteChannel src, long position, long count)
        {
            throw new UnsupportedOperationException();
        }

        public int read(ByteBuffer dst, long position)
        {
            int ret = dst.remaining();
            this.position = position + ret;
            dst.position(dst.limit());
            return ret;
        }

        public int write(ByteBuffer src, long position)
        {
            throw new UnsupportedOperationException();
        }

        public MappedByteBuffer map(MapMode mode, long position, long size)
        {
            throw new UnsupportedOperationException();
        }

        public FileLock lock(long position, long size, boolean shared)
        {
            throw new UnsupportedOperationException();
        }

        public FileLock tryLock(long position, long size, boolean shared)
        {
            throw new UnsupportedOperationException();
        }

        protected void implCloseChannel()
        {

        }
    }

    private static File writeFile(Parameters params) throws IOException
    {
        final File f = FileUtils.createTempFile("testReadFully", "1");
        f.deleteOnExit();

        try(SequentialWriter writer = new SequentialWriter(f))
        {
            long numWritten = 0;
            while (numWritten < params.fileLength)
            {
                writer.write(params.expected);
                numWritten += params.expected.length;
            }

            writer.finish();
        }

        assert f.exists();
        assert f.length() >= params.fileLength;
        return f;
    }

    private static void testReadFully(Parameters params) throws IOException
    {
        final File f = writeFile(params);
        FileHandle.Builder builder = new FileHandle.Builder(f).bufferType(params.bufferType)
                                                              .bufferSize(params.bufferSize);
        builder.mmapped(params.mmappedRegions);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(f.absolutePath(), reader.getPath());
            assertEquals(f.length(), reader.length());
            assertEquals(f.length(), reader.bytesRemaining());
            assertEquals(Math.min(Integer.MAX_VALUE, f.length()), reader.available());

            byte[] b = new byte[params.expected.length];
            long numRead = 0;
            while (numRead < params.fileLength)
            {
                reader.readFully(b);
                assertTrue(Arrays.equals(params.expected, b));
                numRead += b.length;
            }

            assertTrue(reader.isEOF());
            assertEquals(0, reader.bytesRemaining());
        }
    }

    @Test
    public void testReadBytes() throws IOException
    {
        File f = FileUtils.createTempFile("testReadBytes", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        try(SequentialWriter writer = new SequentialWriter(f))
        {
            writer.write(expected.getBytes());
            writer.finish();
        }

        assert f.exists();

        try (FileHandle fh = new FileHandle.Builder(f).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(f.absolutePath(), reader.getPath());
            assertEquals(expected.length(), reader.length());

            ByteBuffer b = ByteBufferUtil.read(reader, expected.length());
            assertEquals(expected, new String(b.array(), StandardCharsets.UTF_8));

            assertTrue(reader.isEOF());
            assertEquals(0, reader.bytesRemaining());
        }
    }

    @Test
    public void testReset() throws IOException
    {
        File f = FileUtils.createTempFile("testMark", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";
        final int numIterations = 10;

        try(SequentialWriter writer = new SequentialWriter(f))
        {
            for (int i = 0; i < numIterations; i++)
                writer.write(expected.getBytes());
            writer.finish();
        }

        assert f.exists();

        try (FileHandle fh = new FileHandle.Builder(f).complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(expected.length() * numIterations, reader.length());

            ByteBuffer b = ByteBufferUtil.read(reader, expected.length());
            assertEquals(expected, new String(b.array(), StandardCharsets.UTF_8));

            assertFalse(reader.isEOF());
            assertEquals((numIterations - 1) * expected.length(), reader.bytesRemaining());

            DataPosition mark = reader.mark();
            assertEquals(0, reader.bytesPastMark());
            assertEquals(0, reader.bytesPastMark(mark));

            for (int i = 0; i < (numIterations - 1); i++)
            {
                b = ByteBufferUtil.read(reader, expected.length());
                assertEquals(expected, new String(b.array(), StandardCharsets.UTF_8));
            }
            assertTrue(reader.isEOF());
            assertEquals(expected.length() * (numIterations - 1), reader.bytesPastMark());
            assertEquals(expected.length() * (numIterations - 1), reader.bytesPastMark(mark));

            reader.reset(mark);
            assertEquals(0, reader.bytesPastMark());
            assertEquals(0, reader.bytesPastMark(mark));
            assertFalse(reader.isEOF());
            for (int i = 0; i < (numIterations - 1); i++)
            {
                b = ByteBufferUtil.read(reader, expected.length());
                assertEquals(expected, new String(b.array(), StandardCharsets.UTF_8));
            }

            reader.reset();
            assertEquals(0, reader.bytesPastMark());
            assertEquals(0, reader.bytesPastMark(mark));
            assertFalse(reader.isEOF());
            for (int i = 0; i < (numIterations - 1); i++)
            {
                b = ByteBufferUtil.read(reader, expected.length());
                assertEquals(expected, new String(b.array(), StandardCharsets.UTF_8));
            }

            assertTrue(reader.isEOF());
        }
    }

    @Test
    public void testSeekSingleThread() throws IOException, InterruptedException
    {
        testSeek(1);
    }

    @Test
    public void testSeekMultipleThreads() throws IOException, InterruptedException
    {
        testSeek(10);
    }

    private static void testSeek(int numThreads) throws IOException, InterruptedException
    {
        final File f = FileUtils.createTempFile("testMark", "1");
        final byte[] expected = new byte[1 << 16];

        long seed = nanoTime();
        //seed = 365238103404423L;
        logger.info("Seed {}", seed);
        Random r = new Random(seed);
        r.nextBytes(expected);

        try(SequentialWriter writer = new SequentialWriter(f))
        {
            writer.write(expected);
            writer.finish();
        }

        assert f.exists();

        FileHandle.Builder builder = new FileHandle.Builder(f);
        final Runnable worker = () ->
        {
            try (FileHandle fh = builder.complete();
                 RandomAccessReader reader = fh.createReader())
            {
                assertEquals(expected.length, reader.length());

                ByteBuffer b = ByteBufferUtil.read(reader, expected.length);
                assertTrue(Arrays.equals(expected, b.array()));
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());

                reader.seek(0);
                b = ByteBufferUtil.read(reader, expected.length);
                assertTrue(Arrays.equals(expected, b.array()));
                assertTrue(reader.isEOF());
                assertEquals(0, reader.bytesRemaining());

                for (int i = 0; i < 10; i++)
                {
                    int pos = r.nextInt(expected.length);
                    reader.seek(pos);
                    assertEquals(pos, reader.getPosition());

                    ByteBuffer buf = ByteBuffer.wrap(expected, pos, expected.length - pos)
                                               .order(ByteOrder.BIG_ENDIAN);

                    while (reader.bytesRemaining() > 4)
                        assertEquals(buf.getInt(), reader.readInt());
                }

                reader.close();
            }
            catch (Exception ex)
            {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        };

        if (numThreads == 1)
        {
            worker.run();
        }
        else
        {
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            for (int i = 0; i < numThreads; i++)
                executor.submit(worker);

            executor.shutdown();
            Assert.assertTrue(executor.awaitTermination(1, TimeUnit.MINUTES));
        }
    }

    @Test
    public void testSkipBytesLessThanBufferSize() throws IOException
    {
        testSkipBytes(new Parameters(8192, 1024), 1);
    }

    @Test
    public void testSkipBytesGreaterThanBufferSize() throws IOException
    {
        int bufferSize = 16;
        Parameters params = new Parameters(8192, bufferSize);
        int numberOfExpectationsInBufferSize = bufferSize / params.expected.length;
        testSkipBytes(params, numberOfExpectationsInBufferSize + 1);
    }

    public void testSkipBytesNonPositive() throws IOException
    {
        Parameters params = new Parameters(8192, 4096);
        final File f = writeFile(params);
        FileHandle.Builder builder = new FileHandle.Builder(f).bufferType(params.bufferType)
                                                                     .bufferSize(params.bufferSize)
                                                                     .mmapped(params.mmappedRegions);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            assertEquals(0, reader.skipBytes(0));
            assertEquals(0, reader.skipBytes(-1));
        }
    }

    @Test(expected = IOException.class)
    public void testSkipBytesClosed() throws IOException
    {
        Parameters params = new Parameters(8192, 4096);
        final File f = writeFile(params);
        FileHandle.Builder builder = new FileHandle.Builder(f).bufferType(params.bufferType)
                                                              .bufferSize(params.bufferSize);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            reader.close();
            reader.skipBytes(31415);
        }
    }

    private static void testSkipBytes(Parameters params, int expectationMultiples) throws IOException
    {
        final File f = writeFile(params);
        FileHandle.Builder builder = new FileHandle.Builder(f).bufferType(params.bufferType)
                                                              .bufferSize(params.bufferSize)
                                                              .mmapped(params.mmappedRegions);
        try (FileHandle fh = builder.complete();
             RandomAccessReader reader = fh.createReader())
        {
            int toSkip = expectationMultiples * params.expected.length;
            byte[] b = new byte[params.expected.length];
            long numRead = 0;

            while (numRead < params.fileLength)
            {
                reader.readFully(b);
                assertTrue(Arrays.equals(params.expected, b));
                numRead += b.length;
                int skipped = reader.skipBytes(toSkip);
                long expectedSkipped = Math.max(Math.min(toSkip, params.fileLength - numRead), 0);
                assertEquals(expectedSkipped, skipped);
                numRead += skipped;
            }
        }
    }
}
