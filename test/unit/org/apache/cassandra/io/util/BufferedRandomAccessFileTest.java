package org.apache.cassandra.io.util;
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


import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.Callable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BufferedRandomAccessFileTest
{

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
    public void testNotEOF() throws IOException
    {
        assertEquals(1, new BufferedRandomAccessFile(writeTemporaryFile(new byte[1]), "rw").read(new byte[2]));
    }


    protected void expectEOF(Callable<?> callable)
    {
        boolean threw = false;
        try
        {
            callable.call();
        }
        catch (Exception e)
        {
            assert e.getClass().equals(EOFException.class) : e.getClass().getName() + " is not " + EOFException.class.getName();
            threw = true;
        }
        assert threw : EOFException.class.getName() + " not received";
    }

    @Test
    public void testEOF() throws IOException
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

    protected File writeTemporaryFile(byte[] data) throws IOException
    {
        File f = File.createTempFile("BRAFTestFile", null);
        f.deleteOnExit();
        FileOutputStream fout = new FileOutputStream(f);
        fout.write(data);
        fout.getFD().sync();
        fout.close();
        return f;
    }


    @Test (expected=UnsupportedOperationException.class)
    public void testOverflowMark() throws IOException
    {
        File tmpFile = File.createTempFile("overflowtest", "bin");
        tmpFile.deleteOnExit();

        // Create the BRAF by filename instead of by file.
        BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile.getPath(), "rw");
        assert tmpFile.getPath().equals(rw.getPath());

        // Create a mark and move the rw there.
        FileMark mark = rw.mark();
        rw.reset(mark);

        // Expect this call to succeed.
        int bpm = rw.bytesPastMark(mark);

        // Seek 4gb
        rw.seek(4L*1024L*1024L*1024L*1024L);
        
        // Expect this call to fail -- the distance from mark to current file pointer > 2gb.
        bpm = rw.bytesPastMark(mark);
    }

    @Test
    public void testRead() throws IOException
    {
        File tmpFile = File.createTempFile("readtest", "bin");
        tmpFile.deleteOnExit();

        BufferedRandomAccessFile rw = new BufferedRandomAccessFile(tmpFile.getPath(), "rw");
        rw.write(new byte[]{ 1 });
        rw.seek(0);

        // test read of buffered-but-not-yet-written data
        byte[] buffer = new byte[1];
        assertEquals(1, rw.read(buffer));
        assertEquals(1, buffer[0]);
        rw.close();

        // test read of not-yet-buffered data
        rw = new BufferedRandomAccessFile(tmpFile.getPath(), "rw");
        assert rw.read(buffer) == 1;
        assert buffer[0] == 1;
    }

}
