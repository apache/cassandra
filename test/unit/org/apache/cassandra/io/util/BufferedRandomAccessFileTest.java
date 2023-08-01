/*
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
import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.Util.expectEOF;
import static org.apache.cassandra.Util.expectException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BufferedRandomAccessFileTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testReadAndWrite() throws Exception
    {
        SequentialWriter w = createTempFile("braf");

        // writing string of data to the file
        byte[] data = "Hello".getBytes();
        w.write(data);
        assertEquals(data.length, w.length());
        assertEquals(data.length, w.position());

        w.sync();

        // reading small amount of data from file, this is handled by initial buffer
        FileHandle.Builder builder = new FileHandle.Builder(w.getFile());
        try (FileHandle fh = builder.complete();
             RandomAccessReader r = fh.createReader())
        {

            byte[] buffer = new byte[data.length];
            assertEquals(data.length, r.read(buffer));
            assertTrue(Arrays.equals(buffer, data)); // we read exactly what we wrote
            assertEquals(r.read(), -1); // nothing more to read EOF
            assert r.bytesRemaining() == 0 && r.isEOF();
        }

        // writing buffer bigger than page size, which will trigger reBuffer()
        byte[] bigData = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE + 10];

        for (int i = 0; i < bigData.length; i++)
            bigData[i] = 'd';

        long initialPosition = w.position();
        w.write(bigData); // writing data
        assertEquals(w.position(), initialPosition + bigData.length);
        assertEquals(w.length(), initialPosition + bigData.length); // file size should equals to last position

        w.sync();

        // re-opening file in read-only mode
        try (FileHandle fh = builder.complete();
             RandomAccessReader r = fh.createReader())
        {

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
            ByteBuffer fileContent = ByteBufferUtil.read(r, (int) w.length());
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

            w.finish();
        }
    }

    @Test
    public void testReadAndWriteOnCapacity() throws IOException
    {
        File tmpFile = FileUtils.createTempFile("readtest", "bin");
        try (SequentialWriter w = new SequentialWriter(tmpFile))
        {
            // Fully write the file and sync..
            byte[] in = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE);
            w.write(in);
    
            try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
                 RandomAccessReader r = fh.createReader())
            {
                // Read it into a same size array.
                byte[] out = new byte[RandomAccessReader.DEFAULT_BUFFER_SIZE];
                r.read(out);
    
                // Cannot read any more.
                int negone = r.read();
                assert negone == -1 : "We read past the end of the file, should have gotten EOF -1. Instead, " + negone;
    
                w.finish();
            }
        }
    }

    @Test
    public void testLength() throws IOException
    {
        File tmpFile = FileUtils.createTempFile("lengthtest", "bin");
        try (SequentialWriter w = new SequentialWriter(tmpFile))
        {
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
    
            w.finish();
    
            // will use cachedlength
            try (FileHandle fh = new FileHandle.Builder(tmpFile).complete();
                 RandomAccessReader r = fh.createReader())
            {
                assertEquals(lessThenBuffer.length + biggerThenBuffer.length, r.length());
            }
        }
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

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader r = fh.createReader())
        {

            ByteBuffer content = ByteBufferUtil.read(r, (int) r.length());

            // after reading whole file we should be at EOF
            assertEquals(0, ByteBufferUtil.compare(content, data));
            assert r.bytesRemaining() == 0 && r.isEOF();

            r.seek(0);
            content = ByteBufferUtil.read(r, 10); // reading first 10 bytes
            assertEquals(ByteBufferUtil.compare(content, "cccccccccc".getBytes()), 0);
            assertEquals(r.bytesRemaining(), r.length() - content.limit());

            // trying to read more than file has right now
            expectEOF(() -> ByteBufferUtil.read(r, (int) r.length() + 10));

            w.finish();
        }
    }

    @Test
    public void testSeek() throws Exception
    {
        SequentialWriter w = createTempFile("brafSeek");
        byte[] data = generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE + 20);
        w.write(data);
        w.finish();

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader file = fh.createReader())
        {
            file.seek(0);
            assertEquals(file.getFilePointer(), 0);
            assertEquals(file.bytesRemaining(), file.length());

            file.seek(20);
            assertEquals(file.getFilePointer(), 20);
            assertEquals(file.bytesRemaining(), file.length() - 20);

            // trying to seek past the end of the file should produce EOFException
            expectException(() -> {
                file.seek(file.length() + 30);
                return null;
            }, IllegalArgumentException.class);

            expectException(() -> {
                file.seek(-1);
                return null;
            }, IllegalArgumentException.class); // throws IllegalArgumentException
        }
    }

    @Test
    public void testSkipBytes() throws IOException
    {
        SequentialWriter w = createTempFile("brafSkipBytes");
        w.write(generateByteArray(RandomAccessReader.DEFAULT_BUFFER_SIZE * 2));
        w.finish();

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader file = fh.createReader())
        {

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
        }
    }

    @Test
    public void testGetFilePointer() throws IOException
    {
        final SequentialWriter w = createTempFile("brafGetFilePointer");

        assertEquals(w.position(), 0); // initial position should be 0

        w.write(generateByteArray(20));
        assertEquals(w.position(), 20); // position 20 after writing 20 bytes

        w.sync();

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader r = fh.createReader())
        {

            // position should change after skip bytes
            r.seek(0);
            r.skipBytes(15);
            assertEquals(r.getFilePointer(), 15);

            r.read();
            assertEquals(r.getFilePointer(), 16);
            r.read(new byte[4]);
            assertEquals(r.getFilePointer(), 20);

            w.finish();
        }
    }

    @Test
    public void testGetPath() throws IOException
    {
        SequentialWriter file = createTempFile("brafGetPath");
        assert file.getPath().contains("brafGetPath");
        file.finish();
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
                try (FileHandle fh = new FileHandle.Builder(file1).bufferSize(bufferSize).complete();
                     RandomAccessReader file = fh.createReader())
                {
                    expectEOF(() -> { file.readFully(target, offset, 17); return null; });
                }
            }

            // first read is ok but eventually EOFs
            for (final int n : Arrays.asList(1, 2, 4, 8))
            {
                File file1 = writeTemporaryFile(new byte[16]);
                try (FileHandle fh = new FileHandle.Builder(file1).bufferSize(bufferSize).complete();
                     RandomAccessReader file = fh.createReader())
                {
                    expectEOF(() -> {
                        while (true)
                            file.readFully(target, 0, n);
                    });
                }
            }
        }
    }

    @Test
    public void testNotEOF() throws IOException
    {
        try (final RandomAccessReader reader = RandomAccessReader.open(writeTemporaryFile(new byte[1])))
        {
            assertEquals(1, reader.read(new byte[2]));
        }
    }

    @Test
    public void testBytesRemaining() throws IOException
    {
        SequentialWriter w = createTempFile("brafBytesRemaining");

        int toWrite = RandomAccessReader.DEFAULT_BUFFER_SIZE + 10;

        w.write(generateByteArray(toWrite));

        w.sync();

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader r = fh.createReader())
        {

            assertEquals(r.bytesRemaining(), toWrite);

            for (int i = 1; i <= r.length(); i++)
            {
                r.read();
                assertEquals(r.bytesRemaining(), r.length() - i);
            }

            r.seek(0);
            r.skipBytes(10);
            assertEquals(r.bytesRemaining(), r.length() - 10);

            w.finish();
        }
    }

    @Test
    public void testBytesPastMark() throws IOException
    {
        File tmpFile = FileUtils.createTempFile("overflowtest", "bin");
        tmpFile.deleteOnExit();

        // Create the BRAF by filename instead of by file.
        try (FileHandle fh = new FileHandle.Builder(tmpFile).complete();
             RandomAccessReader r = fh.createReader())
        {
            assert tmpFile.path().equals(r.getPath());

            // Create a mark and move the rw there.
            final DataPosition mark = r.mark();
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
        w.finish();

        final RandomAccessReader r = RandomAccessReader.open(new File(w.getPath()));

        r.close(); // closing to test read after close

        expectException(() -> r.read(), NullPointerException.class);

        //Used to throw ClosedChannelException, but now that it extends BDOSP it just NPEs on the buffer
        //Writing to a BufferedOutputStream that is closed generates no error
        //Going to allow the NPE to throw to catch as a bug any use after close. Notably it won't throw NPE for a
        //write of a 0 length, but that is kind of a corner case
        expectException(() -> { w.write(generateByteArray(1)); return null; }, AssertionError.class);

        try (RandomAccessReader copy = RandomAccessReader.open(new File(r.getPath())))
        {
            ByteBuffer contents = ByteBufferUtil.read(copy, (int) copy.length());

            assertEquals(contents.limit(), data.length);
            assertEquals(ByteBufferUtil.compare(contents, data), 0);
        }
    }

    @Test
    public void testMarkAndReset() throws IOException
    {
        SequentialWriter w = createTempFile("brafTestMark");
        w.write(new byte[30]);

        w.finish();

        try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
             RandomAccessReader file = fh.createReader())
        {
            file.seek(10);
            DataPosition mark = file.mark();

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
        }
    }

    @Test(expected = AssertionError.class)
    public void testAssertionErrorWhenBytesPastMarkIsNegative() throws IOException
    {
        try (SequentialWriter w = createTempFile("brafAssertionErrorWhenBytesPastMarkIsNegative"))
        {
            w.write(new byte[30]);
            w.flush();

            try (FileHandle fh = new FileHandle.Builder(w.getFile()).complete();
                 RandomAccessReader r = fh.createReader())
            {
                r.seek(10);
                r.mark();

                r.seek(0);
                r.bytesPastMark();
            }
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
        expectException(() -> { copy.seek(copy.length() + 1); return null; }, IllegalArgumentException.class);

        copy.seek(0);
        copy.skipBytes(5);

        assertEquals(copy.bytesRemaining(), 15);
        assertEquals(copy.getFilePointer(), 5);
        assertTrue(!copy.isEOF());

        copy.seek(0);
        ByteBuffer contents = ByteBufferUtil.read(copy, (int) copy.length());

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

        file.finish();
        copy.close();
    }

    @Test (expected=IllegalArgumentException.class)
    public void testSetNegativeLength() throws IOException, IllegalArgumentException
    {
        File tmpFile = FileUtils.createTempFile("set_negative_length", "bin");
        try (SequentialWriter file = new SequentialWriter(tmpFile))
        {
            file.truncate(-8L);
        }
    }

    private SequentialWriter createTempFile(String name) throws IOException
    {
        File tempFile = FileUtils.createTempFile(name, null);
        tempFile.deleteOnExit();

        return new SequentialWriter(tempFile);
    }

    private File writeTemporaryFile(byte[] data) throws IOException
    {
        File f = FileUtils.createTempFile("BRAFTestFile", null);
        f.deleteOnExit();
        FileOutputStreamPlus fout = new FileOutputStreamPlus(f);
        fout.write(data);
        fout.sync();
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