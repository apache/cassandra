package org.apache.cassandra.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.cassandra.io.compress.*;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.FileMark;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.io.util.SequentialWriter;

public class RandomAccessReaderTest
{
    @Test
    public void testReadFully() throws IOException
    {
        final File f = File.createTempFile("testReadFully", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        SequentialWriter writer = new SequentialWriter(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);
        writer.write(expected.getBytes());
        writer.finish();

        assert f.exists();

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader reader = RandomAccessReader.open(channel);
        assertEquals(f.getAbsolutePath(), reader.getPath());
        assertEquals(expected.length(), reader.length());

        byte[] b = new byte[expected.length()];
        reader.readFully(b);
        assertEquals(expected, new String(b));

        assertTrue(reader.isEOF());
        assertEquals(0, reader.bytesRemaining());

        reader.close();
        channel.close();
    }

    @Test
    public void testReadBytes() throws IOException
    {
        File f = File.createTempFile("testReadBytes", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";

        SequentialWriter writer = new SequentialWriter(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);
        writer.write(expected.getBytes());
        writer.finish();

        assert f.exists();

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader reader = RandomAccessReader.open(channel);
        assertEquals(f.getAbsolutePath(), reader.getPath());
        assertEquals(expected.length(), reader.length());

        ByteBuffer b = reader.readBytes(expected.length());
        assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));

        assertTrue(reader.isEOF());
        assertEquals(0, reader.bytesRemaining());

        reader.close();
        channel.close();
    }

    @Test
    public void testReset() throws IOException
    {
        File f = File.createTempFile("testMark", "1");
        final String expected = "The quick brown fox jumps over the lazy dog";
        final int numIterations = 10;

        SequentialWriter writer = new SequentialWriter(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);
        for (int i = 0; i < numIterations; i++)
            writer.write(expected.getBytes());
        writer.finish();

        assert f.exists();

        ChannelProxy channel = new ChannelProxy(f);
        RandomAccessReader reader = RandomAccessReader.open(channel);
        assertEquals(expected.length() * numIterations, reader.length());

        ByteBuffer b = reader.readBytes(expected.length());
        assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));

        assertFalse(reader.isEOF());
        assertEquals((numIterations - 1) * expected.length(), reader.bytesRemaining());

        FileMark mark = reader.mark();
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));

        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }
        assertTrue(reader.isEOF());
        assertEquals(expected.length() * (numIterations -1), reader.bytesPastMark());
        assertEquals(expected.length() * (numIterations - 1), reader.bytesPastMark(mark));

        reader.reset(mark);
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));
        assertFalse(reader.isEOF());
        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }

        reader.reset();
        assertEquals(0, reader.bytesPastMark());
        assertEquals(0, reader.bytesPastMark(mark));
        assertFalse(reader.isEOF());
        for (int i = 0; i < (numIterations - 1); i++)
        {
            b = reader.readBytes(expected.length());
            assertEquals(expected, new String(b.array(), Charset.forName("UTF-8")));
        }

        assertTrue(reader.isEOF());
        reader.close();
        channel.close();
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

    private void testSeek(int numThreads) throws IOException, InterruptedException
    {
        final File f = File.createTempFile("testMark", "1");
        final String[] expected = new String[10];
        int len = 0;
        for (int i = 0; i < expected.length; i++)
        {
            expected[i] = UUID.randomUUID().toString();
            len += expected[i].length();
        }
        final int totalLength = len;

        SequentialWriter writer = new SequentialWriter(f, CompressionParameters.DEFAULT_CHUNK_LENGTH, false);
        for (int i = 0; i < expected.length; i++)
            writer.write(expected[i].getBytes());
        writer.finish();

        assert f.exists();

        final ChannelProxy channel = new ChannelProxy(f);

        final Runnable worker = new Runnable() {

            @Override
            public void run()
            {
                try
                {
                    RandomAccessReader reader = RandomAccessReader.open(channel);
                    assertEquals(totalLength, reader.length());

                    ByteBuffer b = reader.readBytes(expected[0].length());
                    assertEquals(expected[0], new String(b.array(), Charset.forName("UTF-8")));

                    assertFalse(reader.isEOF());
                    assertEquals(totalLength - expected[0].length(), reader.bytesRemaining());

                    long filePointer = reader.getFilePointer();

                    for (int i = 1; i < expected.length; i++)
                    {
                        b = reader.readBytes(expected[i].length());
                        assertEquals(expected[i], new String(b.array(), Charset.forName("UTF-8")));
                    }
                    assertTrue(reader.isEOF());

                    reader.seek(filePointer);
                    assertFalse(reader.isEOF());
                    for (int i = 1; i < expected.length; i++)
                    {
                        b = reader.readBytes(expected[i].length());
                        assertEquals(expected[i], new String(b.array(), Charset.forName("UTF-8")));
                    }

                    assertTrue(reader.isEOF());
                    reader.close();
                }
                catch (Exception ex)
                {
                    ex.printStackTrace();
                    fail(ex.getMessage());
                }
            }
        };

        if(numThreads == 1)
        {
            worker.run();
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (int i = 0; i < numThreads; i++)
            executor.submit(worker);

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.MINUTES);

        channel.close();
    }
}
