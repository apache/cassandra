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

import org.apache.cassandra.utils.ByteBufferUtil;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class BufferedRandomAccessFileTest
{
    @Test
    public void testReadAndWrite() throws Exception
    {
        BufferedRandomAccessFile file = createTempFile("braf");

        // writting string of data to the file
        byte[] data = "Hello".getBytes();
        file.write(data);
        assertEquals(file.length(), data.length);
        assertEquals(file.getFilePointer(), data.length);

        // reading small amount of data from file, this is handled by initial buffer
        file.seek(0);
        byte[] buffer = new byte[data.length];
        assertEquals(file.read(buffer), data.length);
        assertTrue(Arrays.equals(buffer, data)); // we read exactly what we wrote
        assertEquals(file.read(), -1); // nothing more to read EOF
        assert file.bytesRemaining() == 0 && file.isEOF();

        // writing buffer bigger than page size, which will trigger reBuffer()
        byte[] bigData = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE + 10];

        for (int i = 0; i < bigData.length; i++)
            bigData[i] = 'd';

        long initialPosition = file.getFilePointer();
        file.write(bigData); // writing data
        assertEquals(file.getFilePointer(), initialPosition + bigData.length);
        assertEquals(file.length(), initialPosition + bigData.length); // file size should equals to last position

        // reading written buffer
        file.seek(initialPosition); // back to initial (before write) position
        data = new byte[bigData.length];
        long sizeRead = 0;
        for (int i = 0; i < data.length; i++)
        {
            data[i] = (byte) file.read(); // this will trigger reBuffer()
            sizeRead++;
        }

        assertEquals(sizeRead, data.length); // read exactly data.length bytes
        assertEquals(file.getFilePointer(), initialPosition + data.length);
        assertEquals(file.length(), initialPosition + bigData.length);
        assertTrue(Arrays.equals(bigData, data));
        assert file.bytesRemaining() == 0 && file.isEOF(); // we are at the of the file

        // test readBytes(int) method
        file.seek(0);
        ByteBuffer fileContent = file.readBytes((int) file.length());
        assertEquals(fileContent.limit(), file.length());
        assert ByteBufferUtil.string(fileContent).equals("Hello" + new String(bigData));

        // read the same buffer but using readFully(int)
        data = new byte[bigData.length];
        file.seek(initialPosition);
        file.readFully(data);
        assert file.bytesRemaining() == 0 && file.isEOF(); // we should be at EOF
        assertTrue(Arrays.equals(bigData, data));

        // try to read past mark (all methods should return -1)
        data = new byte[10];
        assertEquals(file.read(), -1);
        assertEquals(file.read(data), -1);
        assertEquals(file.read(data, 0, data.length), -1);

        // test read(byte[], int, int)
        file.seek(0);
        data = new byte[20];
        assertEquals(file.read(data, 0, 15), 15);
        assertTrue(new String(data).contains("Hellodddddddddd"));
        for (int i = 16; i < data.length; i++)
        {
            assert data[i] == 0;
        }

        // try to seek past EOF
        file.seek(file.length() + 10); // should not throw an exception
        assert file.bytesRemaining() == 0 && file.isEOF();

        file.close();
    }

     @Test
    public void testReadsAndWriteOnCapacity() throws IOException
    {
        File tmpFile = File.createTempFile("readtest", "bin");
        BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile, "rw");

        // Fully write the file and sync..
        byte[] in = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE];
        rw.write(in);

        // Read it into a same size array.
        byte[] out = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE];
        rw.read(out);

        // We're really at the end.
        long rem = rw.bytesRemaining();
        assert rw.isEOF();
        assert rem == 0 : "BytesRemaining should be 0 but it's " + rem;

        // Cannot read any more.
        int negone = rw.read();
        assert negone == -1 : "We read past the end of the file, should have gotten EOF -1. Instead, " + negone;

        // Writing will succeed
        rw.write(new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE]);
        // Forcing a rebuffer here
        rw.write(42);
    }

    @Test
    public void testLength() throws IOException
    {
        File tmpFile = File.createTempFile("lengthtest", "bin");
        BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile, "rw");
        assertEquals(0, rw.length());

        // write a chunk smaller then our buffer, so will not be flushed
        // to disk
        byte[] lessThenBuffer = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE / 2];
        rw.write(lessThenBuffer);
        assertEquals(lessThenBuffer.length, rw.length());

        // sync the data and check length
        rw.sync();
        assertEquals(lessThenBuffer.length, rw.length());

        // write more then the buffer can hold and check length
        byte[] biggerThenBuffer = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE * 2];
        rw.write(biggerThenBuffer);
        assertEquals(biggerThenBuffer.length + lessThenBuffer.length, rw.length());

        // checking that reading doesn't interfere
        rw.seek(0);
        rw.read();
        assertEquals(biggerThenBuffer.length + lessThenBuffer.length, rw.length());

        rw.close();

        // will use cachedlength
        BufferedRandomAccessFile r = new BufferedRandomAccessFile(tmpFile, "r");
        assertEquals(lessThenBuffer.length + biggerThenBuffer.length, r.length());
        r.close();
    }

    @Test
    public void testReadBytes() throws IOException
    {
        final BufferedRandomAccessFile file = createTempFile("brafReadBytes");

        byte[] data = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE + 10];

        for (int i = 0; i < data.length; i++)
        {
            data[i] = 'c';
        }

        file.write(data);

        file.seek(0);
        ByteBuffer content = file.readBytes((int) file.length());

        // after reading whole file we should be at EOF
        assertEquals(ByteBufferUtil.compare(content, data), 0);
        assert file.bytesRemaining() == 0 && file.isEOF();

        file.seek(0);
        content = file.readBytes(10); // reading first 10 bytes
        assertEquals(ByteBufferUtil.compare(content, "cccccccccc".getBytes()), 0);
        assertEquals(file.bytesRemaining(), file.length() - content.limit());

        // trying to read more than file has right now
        expectEOF(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                return file.readBytes((int) file.length() + 10);
            }
        });

        file.close();
    }

    @Test
    public void testSeek() throws Exception
    {
        final BufferedRandomAccessFile file = createTempFile("brafSeek");
        byte[] data = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE + 20];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = 'c';
        }

        file.write(data);
        assert file.bytesRemaining() == 0 && file.isEOF();

        file.seek(0);
        assertEquals(file.getFilePointer(), 0);
        assertEquals(file.bytesRemaining(), file.length());

        file.seek(20);
        assertEquals(file.getFilePointer(), 20);
        assertEquals(file.bytesRemaining(), file.length() - 20);

        // trying to seek past the end of the file
        file.seek(file.length() + 30);
        assertEquals(file.getFilePointer(), data.length + 30);
        assertEquals(file.getFilePointer(), file.length()); // length should be at seek position
        assert file.bytesRemaining() == 0 && file.isEOF();

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
        BufferedRandomAccessFile file = createTempFile("brafSkipBytes");
        byte[] data = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE * 2];

        file.write(data);
        assert file.bytesRemaining() == 0 && file.isEOF();

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
        final BufferedRandomAccessFile file = createTempFile("brafGetFilePointer");

        assertEquals(file.getFilePointer(), 0); // initial position should be 0

        file.write(new byte[20]);
        assertEquals(file.getFilePointer(), 20); // position 20 after writing 20 bytes

        file.seek(10);
        assertEquals(file.getFilePointer(), 10); // after seek to 10 should be 10

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                file.seek(-1);
                return null;
            }
        }, IllegalArgumentException.class);

        assertEquals(file.getFilePointer(), 10);

        file.seek(30); // past previous end file
        assertEquals(file.getFilePointer(), 30);

        // position should change after skip bytes
        file.seek(0);
        file.skipBytes(15);
        assertEquals(file.getFilePointer(), 15);

        file.read();
        assertEquals(file.getFilePointer(), 16);
        file.read(new byte[4]);
        assertEquals(file.getFilePointer(), 20);

        file.close();
    }

    @Test
    public void testGetPath() throws IOException
    {
        BufferedRandomAccessFile file = createTempFile("brafGetPath");
        assert file.getPath().contains("brafGetPath");
    }

     @Test
    public void testIsEOF() throws IOException
    {
        for (String mode : Arrays.asList("r", "rw")) // read, read+write
        {
            for (int bufferSize : Arrays.asList(1, 2, 3, 5, 8, 64))  // smaller, equal, bigger buffer sizes
            {
                final byte[] target = new byte[32];

                // single too-large read
                for (final int offset : Arrays.asList(0, 8))
                {
                    final BufferedRandomAccessFile file = new BufferedRandomAccessFile(writeTemporaryFile(new byte[16]), mode, bufferSize);
                    expectEOF(new Callable<Object>()
                    {
                        public Object call() throws IOException
                        {
                            file.readFully(target, offset, 17);
                            return null;
                        }
                    });
                }

                // first read is ok but eventually EOFs
                for (final int n : Arrays.asList(1, 2, 4, 8))
                {
                    final BufferedRandomAccessFile file = new BufferedRandomAccessFile(writeTemporaryFile(new byte[16]), mode, bufferSize);
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
        assertEquals(1, new BufferedRandomAccessFile(writeTemporaryFile(new byte[1]), "rw").read(new byte[2]));
    }

    @Test
    public void testBytesRemaining() throws IOException
    {
        BufferedRandomAccessFile file = createTempFile("brafBytesRemaining");
        assertEquals(file.bytesRemaining(), 0);

        int toWrite = BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE + 10;

        file.write(new byte[toWrite]);
        assertEquals(file.bytesRemaining(), 0);

        file.seek(0);
        assertEquals(file.bytesRemaining(), toWrite);

        for (int i = 1; i <= file.length(); i++)
        {
            file.read();
            assertEquals(file.bytesRemaining(), file.length() - i);
        }

        file.seek(0);
        file.skipBytes(10);
        assertEquals(file.bytesRemaining(), file.length() - 10);

        file.close();
    }

    @Test
    public void testBytesPastMark() throws IOException
    {
        File tmpFile = File.createTempFile("overflowtest", "bin");
        tmpFile.deleteOnExit();

        // Create the BRAF by filename instead of by file.
        final BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile.getPath(), "rw");
        assert tmpFile.getPath().equals(rw.getPath());

        // Create a mark and move the rw there.
        final FileMark mark = rw.mark();
        rw.reset(mark);

        // Expect this call to succeed.
        rw.bytesPastMark(mark);
    }

    @Test
    public void testClose() throws IOException
    {
        final BufferedRandomAccessFile file = createTempFile("brafClose");

        byte[] data = new byte[BufferedRandomAccessFile.DEFAULT_BUFFER_SIZE + 20];
        for (int i = 0; i < data.length; i++)
        {
            data[i] = 'c';
        }

        file.write(data);
        file.close();

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                return file.read();
            }
        }, ClosedChannelException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                file.write(new byte[1]);
                return null;
            }
        }, ClosedChannelException.class);

        BufferedRandomAccessFile copy = new BufferedRandomAccessFile(file.getPath(), "r");
        ByteBuffer contents = copy.readBytes((int) copy.length());

        assertEquals(contents.limit(), data.length);
        assertEquals(ByteBufferUtil.compare(contents, data), 0);
    }

    @Test
    public void testMarkAndReset() throws IOException
    {
        BufferedRandomAccessFile file = createTempFile("brafTestMark");
        file.write(new byte[30]);

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
        BufferedRandomAccessFile file = createTempFile("brafAssertionErrorWhenBytesPastMarkIsNegative");
        file.write(new byte[30]);

        file.seek(10);
        file.mark();

        file.seek(0);
        file.bytesPastMark();
    }

    @Test
    public void testReadOnly() throws IOException
    {
        BufferedRandomAccessFile file = createTempFile("brafReadOnlyTest");

        byte[] data = new byte[20];
        for (int i = 0; i < data.length; i++)
            data[i] = 'c';

        file.write(data);
        file.sync(); // flushing file to the disk

        // read-only copy of the file, with fixed file length
        final BufferedRandomAccessFile copy = new BufferedRandomAccessFile(file.getPath(), "r");

        copy.seek(copy.length());
        assertTrue(copy.bytesRemaining() == 0 && copy.isEOF());

        // can't seek past the end of the file for read-only files
        expectEOF(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.seek(copy.length() + 1);
                return null;
            }
        });

        /* Any write() call should fail */
        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(1);
                return null;
            }
        }, IOException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(new byte[1]);
                return null;
            }
        }, IOException.class);

        expectException(new Callable<Object>()
        {
            public Object call() throws IOException
            {
                copy.write(new byte[3], 0, 2);
                return null;
            }
        }, IOException.class);

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

    @Test
    public void testSeekPastEOF() throws IOException
    {
        BufferedRandomAccessFile file = createTempFile("brafTestSeekPastEOF");
        file.seek(1);
        file.write(1);
        file.seek(0);
        assertEquals(0, file.read());
        assertEquals(1, file.read());
    }

    private void expectException(Callable<?> callable, Class<?> exception)
    {
        boolean thrown = false;

        try
        {
            callable.call();
        }
        catch (Exception e)
        {
            assert e.getClass().equals(exception) : e.getClass().getName() + " is not " + exception.getName();
            thrown = true;
        }

        assert thrown : exception.getName() + " not received";
    }

    private void expectEOF(Callable<?> callable)
    {
        expectException(callable, EOFException.class);
    }

    private BufferedRandomAccessFile createTempFile(String name) throws IOException
    {
        File tempFile = File.createTempFile(name, null);
        tempFile.deleteOnExit();

        return new BufferedRandomAccessFile(tempFile, "rw");
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

    public void assertSetLength(BufferedRandomAccessFile file, long length) throws IOException
    {
        assert file.getFilePointer() == length;
        assert file.length() == file.getFilePointer();
        assert file.getChannel().size() == file.length();
    }

    @Test
    public void testSetLength() throws IOException
    {
        File tmpFile = File.createTempFile("set_length", "bin");
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(tmpFile, "rw", 8*1024*1024);

        // test that data in buffer is truncated
        file.writeLong(1L);
        file.writeLong(2L);
        file.writeLong(3L);
        file.writeLong(4L);
        file.setLength(16L);
        assertSetLength(file, 16L);

        // seek back and truncate within file
        file.writeLong(3L);
        file.seek(8L);
        file.setLength(24L);
        assertSetLength(file, 24L);

        // seek back and truncate past end of file
        file.setLength(64L);
        assertSetLength(file, 64L);

        // make sure file is consistent after sync
        file.sync();
        assertSetLength(file, 64L);
    }

    @Test (expected=IllegalArgumentException.class)
    public void testSetNegativeLength() throws IOException, IllegalArgumentException
    {
        File tmpFile = File.createTempFile("set_negative_length", "bin");
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(tmpFile, "rw", 8*1024*1024);
        file.setLength(-8L);
    }

    @Test (expected=IOException.class)
    public void testSetLengthDuringReadMode() throws IOException
    {
        File tmpFile = File.createTempFile("set_length_during_read_mode", "bin");
        BufferedRandomAccessFile file = new BufferedRandomAccessFile(tmpFile, "r", 8*1024*1024);
        file.setLength(4L);
    }
}
