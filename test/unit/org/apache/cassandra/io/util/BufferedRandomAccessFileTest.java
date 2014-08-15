/**
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

import org.apache.cassandra.service.FileCacheService;
import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.cassandra.Util.expectEOF;
import static org.apache.cassandra.Util.expectException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BufferedRandomAccessFileTest
{
    @Test
    public void testReadAndWrite() throws Exception
    {
        SequentialWriter w = createTempFile("braf");

        // writting string of data to the file
        byte[] data = "Hello".getBytes();
        w.write(data);
        assertEquals(data.length, w.length());
        assertEquals(data.length, w.getFilePointer());

        w.sync();

        // reading small amount of data from file, this is handled by initial buffer
        RandomAccessReader r = RandomAccessReader.open(w);

        byte[] buffer = new byte[data.length];
        assertEquals(data.length, r.read(buffer));
        assertTrue(Arrays.equals(buffer, data)); // we read exactly what we wrote
        assertEquals(r.read(), -1); // nothing more to read EOF
        assert r.bytesRemaining() == 0 && r.isEOF();

        r.close();

        // writing buffer bigger than page size, which will trigger reBuffer()
        byte[] bigData = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE + 10];

        for (int i = 0; i < bigData.length; i++)
            bigData[i] = 'd';

        long initialPosition = w.getFilePointer();
        w.write(bigData); // writing data
        assertEquals(w.getFilePointer(), initialPosition + bigData.length);
        assertEquals(w.length(), initialPosition + bigData.length); // file size should equals to last position

        w.sync();

        r = RandomAccessReader.open(w); // re-opening file in read-only mode

        // reading written buffer
        r.seek(initialPosition); // back to initial (before write) position
        data = new byte[bigData.length];
        long sizeRead = 0;
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) r.read();
            sizeRead++;
        }

        assertEquals(sizeRead, data.length); // read exactly data.length bytes
        assertEquals(r.getFilePointer(), initialPosition + data.length);
        assertEquals(r.length(), initialPosition + bigData.length);
        assertTrue(Arrays.equals(bigData, data));
        assertTrue(r.bytesRemaining() == 0 && r.isEOF()); // we are at the of the file

        // test readBytes(int) method
        r.seek(0);
        ByteBuffer fileContent = r.readBytes((int) w.length());
        assertEquals(fileContent.limit(), w.length());
        assert ByteBufferUtil.string(fileContent).equals("Hello" + new String(bigData));

        // read the same buffer but using readFully(int)
        data = new byte[bigData.length];
        r.seek(initialPosition);
        r.readFully(data);
        assert r.bytesRemaining() == 0 && r.isEOF(); // we should be at EOF
        assertTrue(Arrays.equals(bigData, data));

        // try to read past mark (all methods should return -1)
        data = new byte[10];
        assertEquals(r.read(), -1);
        assertEquals(r.read(data), -1);
        assertEquals(r.read(data, 0, data.length), -1);

        // test read(byte[], int, int)
        r.seek(0);
        data = new byte[20];
        assertEquals(15, r.read(data, 0, 15));
        assertTrue(new String(data).contains("Hellodddddddddd"));
        for (int i = 16; i < data.length; i++)
        {
            assert data[i] == 0;
        }

        w.close();
        r.close();
    }

    @Test
    public void testReadAndWriteOnCapacity() throws IOException
    {
        File tmpFile = File.createTempFile("readtest", "bin");
        SequentialWriter w = SequentialWriter.open(tmpFile);

        // Fully write the file and sync..
        byte[] in = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE);
        w.write(in);

        RandomAccessReader r = RandomAccessReader.open(w);

        // Read it into a same size array.
        byte[] out = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE];
        r.read(out);

        // Cannot read any more.
        int negone = r.read();
        assert negone == -1 : "We read past the end of the file, should have gotten EOF -1. Instead, " + negone;

        r.close();
        w.close();
    }

    @Test
    public void testLength() throws IOException
    {
        File tmpFile = File.createTempFile("lengthtest", "bin");
        SequentialWriter w = SequentialWriter.open(tmpFile);
        assertEquals(0, w.length());

        // write a chunk smaller then our buffer, so will not be flushed
        // to disk
        byte[] lessThenBuffer = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE / 2);
        w.write(lessThenBuffer);
        assertEquals(lessThenBuffer.length, w.length());

        // sync the data and check length
        w.sync();
        assertEquals(lessThenBuffer.length, w.length());

        // write more then the buffer can hold and check length
        byte[] biggerThenBuffer = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE * 2);
        w.write(biggerThenBuffer);
        assertEquals(biggerThenBuffer.length + lessThenBuffer.length, w.length());

        w.close();

        // will use cachedlength
        RandomAccessReader r = RandomAccessReader.open(tmpFile);
        assertEquals(lessThenBuffer.length + biggerThenBuffer.length, r.length());
        r.close();
    }

    @Test
    public void testReadBytes() throws IOException
    {
        final SequentialWriter w = createTempFile("brafReadBytes");

        byte[] data = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE + 10];

        for (int i = 0; i < data.length; i++)
        {
            data[i] = 'c';
        }

        w.write(data);
        w.sync();

        final RandomAccessReader r = RandomAccessReader.open(w);

        ByteBuffer content = r.readBytes((int) r.length());

        // after reading whole file we should be at EOF
        assertEquals(0, ByteBufferUtil.compare(content, data));
        assert r.bytesRemaining() == 0 && r.isEOF();

        r.seek(0);
        content = r.readBytes(10); // reading first 10 bytes
        assertEquals(ByteBufferUtil.compare(content, "cccccccccc".getBytes()), 0);
        assertEquals(r.bytesRemaining(), r.length() - content.limit());

        // trying to read more than file has right now
        expectEOF(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                return r.readBytes((int) r.length() + 10);
            }
        });

        w.close();
        r.close();
    }

    @Test
    public void testSeek() throws Exception
    {
        SequentialWriter w = createTempFile("brafSeek");
        byte[] data = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE + 20);
        w.write(data);
        w.close();

        final RandomAccessReader file = RandomAccessReader.open(w);

        file.seek(0);
        assertEquals(file.getFilePointer(), 0);
        assertEquals(file.bytesRemaining(), file.length());

        file.seek(20);
        assertEquals(file.getFilePointer(), 20);
        assertEquals(file.bytesRemaining(), file.length() - 20);

        // trying to seek past the end of the file should produce EOFException
        expectException(new Callable<Object>()
        {
            public Object call()
            {
                file.seek(file.length() + 30);
                return null;
            }
        }, IllegalArgumentException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                file.seek(-1);
                return null;
            }
        }, IllegalArgumentException.class); // throws IllegalArgumentException

        file.close();
    }

    @Test
    public void testSkipBytes() throws IOException
    {
        SequentialWriter w = createTempFile("brafSkipBytes");
        w.write(generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE * 2));
        w.close();

        RandomAccessReader file = RandomAccessReader.open(w);

        file.seek(0); // back to the beginning of the file
        assertEquals(file.skipBytes(10), 10);
        assertEquals(file.bytesRemaining(), file.length() - 10);

        int initialPosition = (int) file.getFilePointer();
        // can't skip more than file size
        assertEquals(file.skipBytes((int) file.length() + 10), file.length() - initialPosition);
        assertEquals(file.getFilePointer(), file.length());
        assert file.bytesRemaining() == 0 && file.isEOF();

        file.seek(0);

        // skipping negative amount should return 0
        assertEquals(file.skipBytes(-1000), 0);
        assertEquals(file.getFilePointer(), 0);
        assertEquals(file.bytesRemaining(), file.length());

        file.close();
    }

    @Test
    public void testGetFilePointer() throws IOException
    {
        final SequentialWriter w = createTempFile("brafGetFilePointer");

        assertEquals(w.getFilePointer(), 0); // initial position should be 0

        w.write(generateByteArray(20));
        assertEquals(w.getFilePointer(), 20); // position 20 after writing 20 bytes

        w.sync();

        RandomAccessReader r = RandomAccessReader.open(w);

        // position should change after skip bytes
        r.seek(0);
        r.skipBytes(15);
        assertEquals(r.getFilePointer(), 15);

        r.read();
        assertEquals(r.getFilePointer(), 16);
        r.read(new byte[4]);
        assertEquals(r.getFilePointer(), 20);

        w.close();
        r.close();
    }

    @Test
    public void testGetPath() throws IOException
    {
        SequentialWriter file = createTempFile("brafGetPath");
        assert file.getPath().contains("brafGetPath");
        file.close();
    }

    @Test
    public void testIsEOF() throws IOException
    {
        for (int bufferSize : Arrays.asList(1, 2, 3, 5, 8, 64))  // smaller, equal, bigger buffer sizes
        {
            final byte[] target = new byte[32];

            // single too-large read
            for (final int offset : Arrays.asList(0, 8))
            {
                File file1 = writeTemporaryFile(new byte[16]);
                try (final RandomAccessReader file = RandomAccessReader.open(file1, bufferSize, null))
                {
                    expectEOF(new Callable<Object>()
                    {
                        public Object call() throws IOException
                        {
                            file.readFully(target, offset, 17);
                            return null;
                        }
                    });
                }
            }

            // first read is ok but eventually EOFs
            for (final int n : Arrays.asList(1, 2, 4, 8))
            {
                File file1 = writeTemporaryFile(new byte[16]);
                try (final RandomAccessReader file = RandomAccessReader.open(file1, bufferSize, null))
                {
                    expectEOF(new Callable<Object>()
                    {
                        public Object call() throws IOException
                        {
                            while (true)
                                file.readFully(target, 0, n);
                        }
                    });
                }
            }
        }
    }

    @Test
    public void testNotEOF() throws IOException
    {
        assertEquals(1, RandomAccessReader.open(writeTemporaryFile(new byte[1])).read(new byte[2]));
    }

    @Test
    public void testBytesRemaining() throws IOException
    {
        SequentialWriter w = createTempFile("brafBytesRemaining");

        int toWrite = RandomAccessReader.DEFAULT_BUFFER_SIZE + 10;

        w.write(generateByteArray(toWrite));

        w.sync();

        RandomAccessReader r = RandomAccessReader.open(w);

        assertEquals(r.bytesRemaining(), toWrite);

        for (int i = 1; i <= r.length(); i++)
        {
            r.read();
            assertEquals(r.bytesRemaining(), r.length() - i);
        }

        r.seek(0);
        r.skipBytes(10);
        assertEquals(r.bytesRemaining(), r.length() - 10);

        w.close();
        r.close();
    }

    @Test
    public void testBytesPastMark() throws IOException
    {
        File tmpFile = File.createTempFile("overflowtest", "bin");
        tmpFile.deleteOnExit();

        // Create the BRAF by filename instead of by file.
        try (final RandomAccessReader r = RandomAccessReader.open(new File(tmpFile.getPath())))
        {
            assert tmpFile.getPath().equals(r.getPath());
    
            // Create a mark and move the rw there.
            final FileMark mark = r.mark();
            r.reset(mark);
    
            // Expect this call to succeed.
            r.bytesPastMark(mark);
        }
    }

    @Test
    public void testClose() throws IOException
    {
        final SequentialWriter w = createTempFile("brafClose");

        byte[] data = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE + 20);

        w.write(data);
        w.close(); // will flush

        final RandomAccessReader r = RandomAccessReader.open(new File(w.getPath()));

        r.close(); // closing to test read after close

        expectException(new Callable<Object>()
        {
            public Object call()
            {
                return r.read();
            }
        }, AssertionError.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                w.write(generateByteArray(1));
                return null;
            }
        }, ClosedChannelException.class);

        try (RandomAccessReader copy = RandomAccessReader.open(new File(r.getPath())))
        {
            ByteBuffer contents = copy.readBytes((int) copy.length());
    
            assertEquals(contents.limit(), data.length);
            assertEquals(ByteBufferUtil.compare(contents, data), 0);
        }
    }

    @Test
    public void testMarkAndReset() throws IOException
    {
        SequentialWriter w = createTempFile("brafTestMark");
        w.write(new byte[30]);

        w.close();

        RandomAccessReader file = RandomAccessReader.open(w);

        file.seek(10);
        FileMark mark = file.mark();

        file.seek(file.length());
        assertTrue(file.isEOF());

        file.reset();
        assertEquals(file.bytesRemaining(), 20);

        file.seek(file.length());
        assertTrue(file.isEOF());

        file.reset(mark);
        assertEquals(file.bytesRemaining(), 20);

        file.seek(file.length());
        assertEquals(file.bytesPastMark(), 20);
        assertEquals(file.bytesPastMark(mark), 20);

        file.reset(mark);
        assertEquals(file.bytesPastMark(), 0);

        file.close();
    }

    @Test (expected = AssertionError.class)
    public void testAssertionErrorWhenBytesPastMarkIsNegative() throws IOException
    {
        try (SequentialWriter w = createTempFile("brafAssertionErrorWhenBytesPastMarkIsNegative"))
        {
            w.write(new byte[30]);
            w.flush();
    
            try (RandomAccessReader r = RandomAccessReader.open(w))
            {
                r.seek(10);
                r.mark();
        
                r.seek(0);
                r.bytesPastMark();
            }
        }
    }

    @Test
    public void testFileCacheService() throws IOException, InterruptedException
    {
        //see https://issues.apache.org/jira/browse/CASSANDRA-7756

        final FileCacheService.CacheKey cacheKey = new FileCacheService.CacheKey();

        final int THREAD_COUNT = 40;
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        SequentialWriter w1 = createTempFile("fscache1");
        SequentialWriter w2 = createTempFile("fscache2");

        w1.write(new byte[30]);
        w1.close();

        w2.write(new byte[30]);
        w2.close();

        for (int i = 0; i < 20; i++)
        {


            RandomAccessReader r1 = RandomAccessReader.open(w1);
            RandomAccessReader r2 = RandomAccessReader.open(w2);


            FileCacheService.instance.put(cacheKey, r1);
            FileCacheService.instance.put(cacheKey, r2);

            final CountDownLatch finished = new CountDownLatch(THREAD_COUNT);
            final AtomicBoolean hadError = new AtomicBoolean(false);

            for (int k = 0; k < THREAD_COUNT; k++)
            {
                executorService.execute( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            long size = FileCacheService.instance.sizeInBytes();

                            while (size > 0)
                                size = FileCacheService.instance.sizeInBytes();
                        }
                        catch (Throwable t)
                        {
                            t.printStackTrace();
                            hadError.set(true);
                        }
                        finally
                        {
                            finished.countDown();
                        }
                    }
                });

            }

            finished.await();
            assert !hadError.get();
        }
    }

    @Test
    public void testReadOnly() throws IOException
    {
        SequentialWriter file = createTempFile("brafReadOnlyTest");

        byte[] data = new byte[20];
        for (int i = 0; i < data.length; i++)
            data[i] = 'c';

        file.write(data);
        file.sync(); // flushing file to the disk

        // read-only copy of the file, with fixed file length
        final RandomAccessReader copy = RandomAccessReader.open(new File(file.getPath()));

        copy.seek(copy.length());
        assertTrue(copy.bytesRemaining() == 0 && copy.isEOF());

        // can't seek past the end of the file for read-only files
        expectException(new Callable<Object>()
        {
            public Object call()
            {
                copy.seek(copy.length() + 1);
                return null;
            }
        }, IllegalArgumentException.class);

        // Any write() call should fail
        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(1);
                return null;
            }
        }, UnsupportedOperationException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(new byte[1]);
                return null;
            }
        }, UnsupportedOperationException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(new byte[3], 0, 2);
                return null;
            }
        }, UnsupportedOperationException.class);

        copy.seek(0);
        copy.skipBytes(5);

        assertEquals(copy.bytesRemaining(), 15);
        assertEquals(copy.getFilePointer(), 5);
        assertTrue(!copy.isEOF());

        copy.seek(0);
        ByteBuffer contents = copy.readBytes((int) copy.length());

        assertEquals(contents.limit(), copy.length());
        assertTrue(ByteBufferUtil.compare(contents, data) == 0);

        copy.seek(0);

        int count = 0;
        while (!copy.isEOF())
        {
            assertEquals((byte) copy.read(), 'c');
            count++;
        }

        assertEquals(count, copy.length());

        copy.seek(0);
        byte[] content = new byte[10];
        copy.read(content);

        assertEquals(new String(content), "cccccccccc");

        file.close();
        copy.close();
    }

    @Test (expected=IllegalArgumentException.class)
    public void testSetNegativeLength() throws IOException, IllegalArgumentException
    {
        File tmpFile = File.createTempFile("set_negative_length", "bin");
        try (SequentialWriter file = SequentialWriter.open(tmpFile))
        {
            file.truncate(-8L);
        }
    }

    @Test (expected=IOException.class)
    public void testSetLengthDuringReadMode() throws IOException
    {
        File tmpFile = File.createTempFile("set_length_during_read_mode", "bin");
        try (RandomAccessReader file = RandomAccessReader.open(tmpFile))
        {
            file.setLength(4L);
        }
    }

    private SequentialWriter createTempFile(String name) throws IOException
    {
        File tempFile = File.createTempFile(name, null);
        tempFile.deleteOnExit();

        return SequentialWriter.open(tempFile);
    }

    private File writeTemporaryFile(byte[] data) throws IOException
    {
        File f = File.createTempFile("BRAFTestFile", null);
        f.deleteOnExit();
        FileOutputStream fout = new FileOutputStream(f);
        fout.write(data);
        fout.getFD().sync();
        fout.close();
        return f;
    }

    private byte[] generateByteArray(int length)
    {
        byte[] arr = new byte[length];

        for (int i = 0; i < length; i++)
            arr[i] = 'a';

        return arr;
    }
}
